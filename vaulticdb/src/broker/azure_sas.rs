use std::{collections::BTreeMap, time::Duration};

use anyhow::Context;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use quick_xml::{de::from_str, se::to_string};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use time::{format_description::well_known::Rfc3339, Duration as TimeDuration, OffsetDateTime};
use url::form_urlencoded::Serializer;

use crate::topology::{
    AzureUserDelegationPolicy, Credential, CredentialKind, StorageCredentialTier,
};

const CLOCK_SKEW: TimeDuration = TimeDuration::minutes(5);
const MAX_PROVIDER_RESPONSE_BYTES: usize = 64 * 1024;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug)]
pub(super) enum AzureSasIssueError {
    Unavailable(anyhow::Error),
    Terminal(anyhow::Error),
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Serialize)]
#[serde(rename = "KeyInfo")]
struct DelegationKeyRequest<'a> {
    #[serde(rename = "Start")]
    start: &'a str,
    #[serde(rename = "Expiry")]
    expiry: &'a str,
}

#[derive(Deserialize)]
#[serde(rename = "UserDelegationKey")]
struct UserDelegationKey {
    #[serde(rename = "SignedOid")]
    signed_oid: String,
    #[serde(rename = "SignedTid")]
    signed_tid: String,
    #[serde(rename = "SignedStart")]
    signed_start: String,
    #[serde(rename = "SignedExpiry")]
    signed_expiry: String,
    #[serde(rename = "SignedService")]
    signed_service: String,
    #[serde(rename = "SignedVersion")]
    signed_version: String,
    #[serde(rename = "Value")]
    value: String,
}

pub(super) async fn issue(
    issuer: &Credential,
    policy: &AzureUserDelegationPolicy,
    endpoint: &BTreeMap<String, Value>,
    tier: StorageCredentialTier,
    ttl: Duration,
) -> std::result::Result<Credential, AzureSasIssueError> {
    if issuer.kind != CredentialKind::AzureEntraClientSecret {
        return Err(terminal(
            "Azure user delegation issuer must be an Entra client secret",
        ));
    }
    if !policy.tiers.contains(&tier) {
        return Err(terminal("Azure user delegation tier is not enabled"));
    }
    if tier == StorageCredentialTier::Lock && !policy.hierarchical_namespace {
        return Err(terminal(
            "Azure storage-lock user delegation requires hierarchical namespace",
        ));
    }
    let tenant_id = required(issuer.tenant_id.as_deref(), "tenant ID")?;
    let client_id = required(issuer.client_id.as_deref(), "client ID")?;
    let client_secret = required(issuer.client_secret.as_deref(), "client secret")?;
    let token_uri = required(issuer.token_uri.as_deref(), "token URI")?;
    validate_token_uri(token_uri, tenant_id)?;
    let scope = issuer
        .scopes
        .first()
        .filter(|scope| !scope.is_empty())
        .context("Azure Entra issuer scope is missing")
        .map_err(AzureSasIssueError::Terminal)?;
    if issuer.scopes.len() != 1 {
        return Err(terminal(
            "Azure Entra issuer requires exactly one OAuth scope",
        ));
    }

    let (account, container, mut delegation_url) = azure_endpoint(endpoint)?;
    delegation_url.set_path("/");
    delegation_url.set_query(Some("restype=service&comp=userdelegationkey"));

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build Azure issuer HTTP client")
        .map_err(AzureSasIssueError::Terminal)?;
    let token_body = Serializer::new(String::new())
        .append_pair("client_id", client_id)
        .append_pair("client_secret", client_secret)
        .append_pair("scope", scope)
        .append_pair("grant_type", "client_credentials")
        .finish();
    let token_response = client
        .post(token_uri)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(token_body)
        .send()
        .await
        .map_err(classify_transport)?;
    let token_response = require_success(token_response, "Microsoft Entra token request").await?;
    let token_body =
        read_provider_response(token_response, "Microsoft Entra token response").await?;
    let token: TokenResponse = serde_json::from_slice(&token_body)
        .context("decode Microsoft Entra token response")
        .map_err(AzureSasIssueError::Terminal)?;
    if token.access_token.is_empty() {
        return Err(terminal(
            "Microsoft Entra token response has no access token",
        ));
    }

    let now = OffsetDateTime::now_utc();
    let signed_start = (now - CLOCK_SKEW)
        .format(&Rfc3339)
        .context("format Azure delegation start")
        .map_err(AzureSasIssueError::Terminal)?;
    let requested_expiry = now
        .checked_add(TimeDuration::seconds(
            i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX),
        ))
        .context("Azure delegation expiry overflow")
        .map_err(AzureSasIssueError::Terminal)?;
    let signed_expiry = requested_expiry
        .format(&Rfc3339)
        .context("format Azure delegation expiry")
        .map_err(AzureSasIssueError::Terminal)?;
    let request_xml = to_string(&DelegationKeyRequest {
        start: &signed_start,
        expiry: &signed_expiry,
    })
    .context("encode Azure delegation-key request")
    .map_err(AzureSasIssueError::Terminal)?;
    let key_response = client
        .post(delegation_url)
        .bearer_auth(&token.access_token)
        .header("x-ms-version", &policy.service_version)
        .header("Content-Type", "application/xml")
        .body(request_xml)
        .send()
        .await
        .map_err(classify_transport)?;
    let key_response = require_success(key_response, "Azure user delegation key request").await?;
    let key_xml =
        read_provider_response(key_response, "Azure user delegation key response").await?;
    let key_xml = std::str::from_utf8(&key_xml)
        .context("Azure user delegation key response is not UTF-8")
        .map_err(AzureSasIssueError::Terminal)?;
    let key: UserDelegationKey = from_str(key_xml)
        .context("decode Azure user delegation key response")
        .map_err(AzureSasIssueError::Terminal)?;
    validate_delegation_key(&key, &signed_start, &signed_expiry)?;

    let sas_token = sign_container_sas(
        &account,
        &container,
        policy,
        tier,
        &signed_start,
        &signed_expiry,
        &key,
    )?;
    Ok(Credential {
        kind: CredentialKind::AzureSas,
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        expires_at: Some(signed_expiry),
        account_name: Some(account),
        account_key: None,
        sas_token: Some(sas_token),
        service_account_json: None,
        access_token: None,
        subject: None,
        tenant_id: None,
        client_id: None,
        client_secret: None,
        refresh_token: None,
        scopes: Vec::new(),
        token_uri: None,
        issued_at: None,
        rotation_due: None,
    })
}

fn required<'a>(
    value: Option<&'a str>,
    name: &str,
) -> std::result::Result<&'a str, AzureSasIssueError> {
    value
        .filter(|value| !value.is_empty())
        .with_context(|| format!("Azure Entra issuer {name} is missing"))
        .map_err(AzureSasIssueError::Terminal)
}

fn terminal(message: &str) -> AzureSasIssueError {
    AzureSasIssueError::Terminal(anyhow::anyhow!(message.to_owned()))
}

fn validate_token_uri(
    token_uri: &str,
    tenant_id: &str,
) -> std::result::Result<(), AzureSasIssueError> {
    let parsed = Url::parse(token_uri)
        .context("Azure Entra token URI is invalid")
        .map_err(AzureSasIssueError::Terminal)?;
    let expected_path = format!("/{tenant_id}/oauth2/v2.0/token");
    let loopback_http = parsed.scheme() == "http"
        && parsed
            .host_str()
            .is_some_and(|host| matches!(host, "127.0.0.1" | "::1" | "localhost"));
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != expected_path
        || parsed.scheme() != "https" && !loopback_http
    {
        return Err(terminal(
            "Azure Entra token URI must be HTTPS and match the configured tenant",
        ));
    }
    Ok(())
}

fn classify_transport(error: reqwest::Error) -> AzureSasIssueError {
    if error.is_connect() || error.is_timeout() || error.is_request() {
        AzureSasIssueError::Unavailable(anyhow::Error::new(error))
    } else {
        AzureSasIssueError::Terminal(anyhow::Error::new(error))
    }
}

async fn require_success(
    response: reqwest::Response,
    operation: &str,
) -> std::result::Result<reqwest::Response, AzureSasIssueError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let error = anyhow::anyhow!("{operation} failed with HTTP status {status}");
    if status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
    {
        Err(AzureSasIssueError::Unavailable(error))
    } else {
        Err(AzureSasIssueError::Terminal(error))
    }
}

async fn read_provider_response(
    response: reqwest::Response,
    operation: &str,
) -> std::result::Result<Vec<u8>, AzureSasIssueError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROVIDER_RESPONSE_BYTES as u64)
    {
        return Err(terminal(&format!("{operation} exceeds size limit")));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(classify_transport)?;
        if body.len().saturating_add(chunk.len()) > MAX_PROVIDER_RESPONSE_BYTES {
            return Err(terminal(&format!("{operation} exceeds size limit")));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn azure_endpoint(
    endpoint: &BTreeMap<String, Value>,
) -> std::result::Result<(String, String, Url), AzureSasIssueError> {
    let raw_url = endpoint
        .get("url")
        .and_then(Value::as_str)
        .context("Azure endpoint is missing url")
        .map_err(AzureSasIssueError::Terminal)?;
    let url = Url::parse(raw_url)
        .context("Azure endpoint has invalid URL")
        .map_err(AzureSasIssueError::Terminal)?;
    let account = endpoint
        .get("account")
        .and_then(Value::as_str)
        .filter(|account| !account.is_empty())
        .context("Azure endpoint is missing account")
        .map_err(AzureSasIssueError::Terminal)?;
    let container = endpoint
        .get("container")
        .and_then(Value::as_str)
        .filter(|container| !container.is_empty())
        .context("Azure endpoint is missing container")
        .map_err(AzureSasIssueError::Terminal)?;
    Ok((account.to_owned(), container.to_owned(), url))
}

fn validate_delegation_key(
    key: &UserDelegationKey,
    sas_start: &str,
    sas_expiry: &str,
) -> std::result::Result<(), AzureSasIssueError> {
    if key.signed_oid.is_empty()
        || key.signed_tid.is_empty()
        || key.signed_start.is_empty()
        || key.signed_expiry.is_empty()
        || key.signed_service != "b"
        || key.signed_version.is_empty()
        || key.value.is_empty()
    {
        return Err(terminal("Azure user delegation key response is incomplete"));
    }
    let parse = |value: &str| {
        OffsetDateTime::parse(value, &Rfc3339)
            .context("Azure user delegation key timestamp is invalid")
            .map_err(AzureSasIssueError::Terminal)
    };
    if parse(&key.signed_start)? > parse(sas_start)?
        || parse(&key.signed_expiry)? < parse(sas_expiry)?
    {
        return Err(terminal(
            "Azure user delegation key does not cover the requested SAS lifetime",
        ));
    }
    Ok(())
}

fn tier_permissions(tier: StorageCredentialTier) -> &'static str {
    match tier {
        StorageCredentialTier::Read => "rl",
        StorageCredentialTier::Append => "rcwl",
        StorageCredentialTier::Maintain | StorageCredentialTier::Lock => "rcwdl",
    }
}

fn sign_container_sas(
    account: &str,
    container: &str,
    policy: &AzureUserDelegationPolicy,
    tier: StorageCredentialTier,
    signed_start: &str,
    signed_expiry: &str,
    key: &UserDelegationKey,
) -> std::result::Result<String, AzureSasIssueError> {
    let permissions = tier_permissions(tier);
    let directory_scoped = tier == StorageCredentialTier::Lock;
    let signed_ip = policy.signed_ip.as_deref().unwrap_or_default();
    let mut fields = vec![permissions, signed_start, signed_expiry];
    let canonical_resource = if directory_scoped {
        format!("/blob/{account}/{container}/locks")
    } else {
        format!("/blob/{account}/{container}")
    };
    fields.extend([
        canonical_resource.as_str(),
        key.signed_oid.as_str(),
        key.signed_tid.as_str(),
        key.signed_start.as_str(),
        key.signed_expiry.as_str(),
        key.signed_service.as_str(),
        key.signed_version.as_str(),
        "",
        "",
        "",
    ]);
    if policy.service_version.as_str() >= "2025-07-05" {
        fields.extend(["", ""]);
    }
    fields.extend([
        signed_ip,
        "https",
        policy.service_version.as_str(),
        if directory_scoped { "d" } else { "c" },
        "",
        "",
    ]);
    if policy.service_version.as_str() >= "2026-04-06" {
        fields.extend(["", ""]);
    }
    fields.extend(["", "", "", "", ""]);
    let string_to_sign = fields.join("\n");
    let signing_key = BASE64
        .decode(&key.value)
        .context("decode Azure user delegation signing key")
        .map_err(AzureSasIssueError::Terminal)?;
    let mut mac = HmacSha256::new_from_slice(&signing_key)
        .context("initialize Azure user delegation signer")
        .map_err(AzureSasIssueError::Terminal)?;
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());

    let mut query = Serializer::new(String::new());
    for (name, value) in [
        ("sp", permissions),
        ("st", signed_start),
        ("se", signed_expiry),
        ("skoid", key.signed_oid.as_str()),
        ("sktid", key.signed_tid.as_str()),
        ("skt", key.signed_start.as_str()),
        ("ske", key.signed_expiry.as_str()),
        ("sks", key.signed_service.as_str()),
        ("skv", key.signed_version.as_str()),
    ] {
        query.append_pair(name, value);
    }
    if !signed_ip.is_empty() {
        query.append_pair("sip", signed_ip);
    }
    query
        .append_pair("spr", "https")
        .append_pair("sv", &policy.service_version)
        .append_pair("sr", if directory_scoped { "d" } else { "c" });
    if directory_scoped {
        query.append_pair("sdd", "1");
    }
    query.append_pair("sig", &signature);
    Ok(query.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::StsFallback;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    fn policy(version: &str) -> AzureUserDelegationPolicy {
        AzureUserDelegationPolicy {
            issuer_ref: "cred:issuer".to_owned(),
            service_version: version.to_owned(),
            signed_ip: None,
            hierarchical_namespace: false,
            tiers: vec![StorageCredentialTier::Read],
            fallback: StsFallback::Disabled,
        }
    }

    fn key() -> UserDelegationKey {
        UserDelegationKey {
            signed_oid: "oid".to_owned(),
            signed_tid: "tid".to_owned(),
            signed_start: "2026-09-09T00:00:00Z".to_owned(),
            signed_expiry: "2026-09-09T01:00:00Z".to_owned(),
            signed_service: "b".to_owned(),
            signed_version: "2026-04-06".to_owned(),
            value: BASE64.encode(b"delegation-key"),
        }
    }

    fn issuer(token_uri: String) -> Credential {
        Credential {
            kind: CredentialKind::AzureEntraClientSecret,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            expires_at: None,
            account_name: None,
            account_key: None,
            sas_token: None,
            service_account_json: None,
            access_token: None,
            subject: None,
            tenant_id: Some("tenant-a".to_owned()),
            client_id: Some("client-a".to_owned()),
            client_secret: Some("issuer-secret".to_owned()),
            refresh_token: None,
            scopes: vec!["https://storage.azure.com/.default".to_owned()],
            token_uri: Some(token_uri),
            issued_at: None,
            rotation_due: None,
        }
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        loop {
            let mut chunk = [0_u8; 4096];
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&chunk[..read]);
            let Some(header_end) = request.windows(4).position(|value| value == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or_default();
            if request.len() >= header_end + 4 + content_length {
                return String::from_utf8(request).unwrap();
            }
        }
    }

    fn between<'a>(value: &'a str, start: &str, end: &str) -> &'a str {
        value
            .split_once(start)
            .unwrap()
            .1
            .split_once(end)
            .unwrap()
            .0
    }

    async fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
        let reason = if status == 200 {
            "OK"
        } else {
            "Service Unavailable"
        };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    #[test]
    fn exact_permissions_are_ordered_for_each_tier() {
        assert_eq!(tier_permissions(StorageCredentialTier::Read), "rl");
        assert_eq!(tier_permissions(StorageCredentialTier::Append), "rcwl");
        assert_eq!(tier_permissions(StorageCredentialTier::Maintain), "rcwdl");
        assert_eq!(tier_permissions(StorageCredentialTier::Lock), "rcwdl");
    }

    #[test]
    fn signs_container_sas_for_supported_string_formats() {
        for version in ["2020-12-06", "2025-07-05", "2026-04-06"] {
            let sas = sign_container_sas(
                "account",
                "container",
                &policy(version),
                StorageCredentialTier::Read,
                "2026-09-09T00:00:00Z",
                "2026-09-09T00:30:00Z",
                &key(),
            )
            .unwrap();
            assert!(sas.contains("sp=rl"));
            assert!(sas.contains(&format!("sv={version}")));
            assert!(sas.contains("spr=https"));
            assert!(sas.contains("sr=c"));
            assert!(sas.contains("sig="));
            assert!(!sas.starts_with('?'));
        }
    }

    #[test]
    fn scopes_lock_sas_to_the_hns_locks_directory() {
        let mut lock_policy = policy("2026-04-06");
        lock_policy.hierarchical_namespace = true;
        lock_policy.tiers = vec![StorageCredentialTier::Lock];
        let sas = sign_container_sas(
            "account",
            "container",
            &lock_policy,
            StorageCredentialTier::Lock,
            "2026-09-09T00:00:00Z",
            "2026-09-09T00:30:00Z",
            &key(),
        )
        .unwrap();
        let fields = url::form_urlencoded::parse(sas.as_bytes()).collect::<BTreeMap<_, _>>();
        assert_eq!(fields.get("sp").map(|value| value.as_ref()), Some("rcwdl"));
        assert_eq!(fields.get("sr").map(|value| value.as_ref()), Some("d"));
        assert_eq!(fields.get("sdd").map(|value| value.as_ref()), Some("1"));
    }

    #[tokio::test]
    async fn exchanges_entra_secret_for_scoped_user_delegation_sas() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut token_stream, _) = listener.accept().await.unwrap();
            let token_request = read_request(&mut token_stream).await;
            write_response(
                &mut token_stream,
                200,
                "application/json",
                r#"{"access_token":"entra-token"}"#,
            )
            .await;

            let (mut key_stream, _) = listener.accept().await.unwrap();
            let key_request = read_request(&mut key_stream).await;
            let start = between(&key_request, "<Start>", "</Start>");
            let expiry = between(&key_request, "<Expiry>", "</Expiry>");
            let body = format!(
                "<UserDelegationKey><SignedOid>issuer-oid</SignedOid><SignedTid>tenant-a</SignedTid><SignedStart>{start}</SignedStart><SignedExpiry>{expiry}</SignedExpiry><SignedService>b</SignedService><SignedVersion>2026-04-06</SignedVersion><Value>{}</Value></UserDelegationKey>",
                BASE64.encode(b"delegation-key")
            );
            write_response(&mut key_stream, 200, "application/xml", &body).await;
            (token_request, key_request)
        });
        let endpoint = BTreeMap::from([
            ("url".to_owned(), Value::String(format!("http://{address}"))),
            ("account".to_owned(), Value::String("account-a".to_owned())),
            ("container".to_owned(), Value::String("repo-a".to_owned())),
        ]);
        let mut configured = policy("2026-04-06");
        configured.tiers = vec![StorageCredentialTier::Maintain];
        configured.signed_ip = Some("198.51.100.10".to_owned());
        let credential = issue(
            &issuer(format!("http://{address}/tenant-a/oauth2/v2.0/token")),
            &configured,
            &endpoint,
            StorageCredentialTier::Maintain,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        let (token_request, key_request) = server.await.unwrap();

        assert!(token_request.starts_with("POST /tenant-a/oauth2/v2.0/token "));
        assert!(token_request.contains("client_id=client-a"));
        assert!(token_request.contains("client_secret=issuer-secret"));
        assert!(token_request.contains("scope=https%3A%2F%2Fstorage.azure.com%2F.default"));
        assert!(key_request.starts_with("POST /?restype=service&comp=userdelegationkey "));
        assert!(key_request
            .to_ascii_lowercase()
            .contains("authorization: bearer entra-token"));
        assert!(key_request
            .to_ascii_lowercase()
            .contains("x-ms-version: 2026-04-06"));

        assert_eq!(credential.kind, CredentialKind::AzureSas);
        assert_eq!(credential.account_name.as_deref(), Some("account-a"));
        assert!(credential.client_secret.is_none());
        let sas = credential.sas_token.as_deref().unwrap();
        assert!(sas.contains("sp=rcwdl"));
        assert!(sas.contains("sip=198.51.100.10"));
        assert!(sas.contains("spr=https"));
        assert!(sas.contains("sr=c"));
        assert!(credential.expires_at.is_some());
    }

    #[tokio::test]
    async fn classifies_provider_status_for_fallback() {
        for (status, unavailable) in [(401, false), (403, false), (429, true), (503, true)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_request(&mut stream).await;
                write_response(&mut stream, status, "application/json", "{}").await;
            });
            let endpoint = BTreeMap::from([
                ("url".to_owned(), Value::String(format!("http://{address}"))),
                ("account".to_owned(), Value::String("account-a".to_owned())),
                ("container".to_owned(), Value::String("repo-a".to_owned())),
            ]);
            let result = issue(
                &issuer(format!("http://{address}/tenant-a/oauth2/v2.0/token")),
                &policy("2026-04-06"),
                &endpoint,
                StorageCredentialTier::Read,
                Duration::from_secs(60),
            )
            .await;
            assert_eq!(
                matches!(result, Err(AzureSasIssueError::Unavailable(_))),
                unavailable
            );
        }
    }
}
