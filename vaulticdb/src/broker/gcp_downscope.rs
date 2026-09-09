use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::{Client, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::{format_description::well_known::Rfc3339, Duration as TimeDuration, OffsetDateTime};
use url::form_urlencoded::Serializer;
use zeroize::Zeroizing;

use crate::topology::{Credential, CredentialKind, GcpDownscopePolicy, StorageCredentialTier};

const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const MAX_PROVIDER_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(super) enum GcpDownscopeIssueError {
    Unavailable(anyhow::Error),
    Terminal(anyhow::Error),
}

#[derive(Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    token_uri: String,
}

#[derive(Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    scope: &'static str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IamTokenResponse {
    access_token: String,
    expire_time: String,
}

#[derive(Deserialize)]
struct StsTokenResponse {
    access_token: String,
    expires_in: u64,
    issued_token_type: String,
    token_type: String,
}

pub(super) async fn issue(
    issuer: &Credential,
    policy: &GcpDownscopePolicy,
    endpoint: &BTreeMap<String, Value>,
    tier: StorageCredentialTier,
    ttl: Duration,
) -> std::result::Result<Credential, GcpDownscopeIssueError> {
    if issuer.kind != CredentialKind::GcpServiceAccountJson {
        return Err(terminal(
            "GCP downscope issuer must be a service account JSON credential",
        ));
    }
    if !policy.tiers.contains(&tier) {
        return Err(terminal("GCP downscope tier is not enabled"));
    }
    let raw_key = issuer
        .service_account_json
        .as_deref()
        .context("GCP downscope issuer service account JSON is missing")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    let source_key: ServiceAccountKey = serde_json::from_str(raw_key)
        .context("decode GCP issuer service account JSON")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    validate_google_endpoint(
        &source_key.token_uri,
        "https://oauth2.googleapis.com/token",
        "service account token",
    )?;
    validate_google_endpoint(
        &policy.iam_credentials_endpoint,
        "https://iamcredentials.googleapis.com",
        "IAM credentials",
    )?;
    validate_google_endpoint(
        &policy.token_exchange_endpoint,
        "https://sts.googleapis.com/v1/token",
        "token exchange",
    )?;

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build GCP issuer HTTP client")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    let bootstrap_token = service_account_token(&client, &source_key).await?;
    let source_token = impersonated_token(&client, policy, &bootstrap_token, ttl).await?;
    let boundary =
        credential_access_boundary(endpoint, tier).map_err(GcpDownscopeIssueError::Terminal)?;
    let downscoped = exchange_token(&client, policy, &source_token.access_token, &boundary).await?;

    let now = OffsetDateTime::now_utc();
    let sts_expiry = now
        .checked_add(TimeDuration::seconds(
            i64::try_from(downscoped.expires_in).unwrap_or(i64::MAX),
        ))
        .context("GCP downscoped token expiry overflow")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    let source_expiry = OffsetDateTime::parse(&source_token.expire_time, &Rfc3339)
        .context("GCP IAM access token expiry is invalid")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    let expires_at = std::cmp::min(sts_expiry, source_expiry)
        .format(&Rfc3339)
        .context("format GCP downscoped token expiry")
        .map_err(GcpDownscopeIssueError::Terminal)?;

    Ok(Credential {
        kind: CredentialKind::GcpAccessToken,
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        expires_at: Some(expires_at),
        account_name: None,
        account_key: None,
        sas_token: None,
        service_account_json: None,
        access_token: Some(downscoped.access_token),
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

async fn service_account_token(
    client: &Client,
    key: &ServiceAccountKey,
) -> std::result::Result<String, GcpDownscopeIssueError> {
    if key.client_email.is_empty() || key.private_key.is_empty() {
        return Err(terminal("GCP issuer service account JSON is incomplete"));
    }
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let claims = JwtClaims {
        iss: &key.client_email,
        scope: CLOUD_PLATFORM_SCOPE,
        aud: &key.token_uri,
        iat: now,
        exp: now + 3600,
    };
    let assertion = Zeroizing::new(
        encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(key.private_key.as_bytes())
                .context("decode GCP issuer private key")
                .map_err(GcpDownscopeIssueError::Terminal)?,
        )
        .context("sign GCP service account assertion")
        .map_err(GcpDownscopeIssueError::Terminal)?,
    );
    let body = Serializer::new(String::new())
        .append_pair("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
        .append_pair("assertion", assertion.as_str())
        .finish();
    let response = client
        .post(&key.token_uri)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(classify_transport)?;
    let body = require_json_success(response, "GCP service account token request").await?;
    let token: OAuthTokenResponse = serde_json::from_slice(&body)
        .context("decode GCP service account token response")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    if token.access_token.is_empty() {
        return Err(terminal(
            "GCP service account token response has no access token",
        ));
    }
    Ok(token.access_token)
}

async fn impersonated_token(
    client: &Client,
    policy: &GcpDownscopePolicy,
    bootstrap_token: &str,
    ttl: Duration,
) -> std::result::Result<IamTokenResponse, GcpDownscopeIssueError> {
    let mut endpoint = Url::parse(&policy.iam_credentials_endpoint)
        .context("GCP IAM credentials endpoint is invalid")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    endpoint.set_path(&format!(
        "/v1/projects/-/serviceAccounts/{}:generateAccessToken",
        policy.service_account
    ));
    let lifetime = ttl.as_secs().min(3600);
    let response = client
        .post(endpoint)
        .bearer_auth(bootstrap_token)
        .json(&json!({
            "scope": [CLOUD_PLATFORM_SCOPE],
            "lifetime": format!("{lifetime}s"),
        }))
        .send()
        .await
        .map_err(classify_transport)?;
    let body = require_json_success(response, "GCP IAM access token request").await?;
    let token: IamTokenResponse = serde_json::from_slice(&body)
        .context("decode GCP IAM access token response")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    if token.access_token.is_empty() || token.expire_time.is_empty() {
        return Err(terminal("GCP IAM access token response is incomplete"));
    }
    Ok(token)
}

async fn exchange_token(
    client: &Client,
    policy: &GcpDownscopePolicy,
    source_token: &str,
    boundary: &Value,
) -> std::result::Result<StsTokenResponse, GcpDownscopeIssueError> {
    let body = Serializer::new(String::new())
        .append_pair("grant_type", TOKEN_EXCHANGE_GRANT)
        .append_pair("requested_token_type", ACCESS_TOKEN_TYPE)
        .append_pair("subject_token_type", ACCESS_TOKEN_TYPE)
        .append_pair("subject_token", source_token)
        .append_pair("options", &boundary.to_string())
        .finish();
    let response = client
        .post(&policy.token_exchange_endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(classify_transport)?;
    let body = require_json_success(response, "GCP STS token exchange").await?;
    let token: StsTokenResponse = serde_json::from_slice(&body)
        .context("decode GCP STS token response")
        .map_err(GcpDownscopeIssueError::Terminal)?;
    if token.access_token.is_empty()
        || token.expires_in == 0
        || token.issued_token_type != ACCESS_TOKEN_TYPE
        || token.token_type != "Bearer"
    {
        return Err(terminal("GCP STS token response is invalid"));
    }
    Ok(token)
}

fn credential_access_boundary(
    endpoint: &BTreeMap<String, Value>,
    tier: StorageCredentialTier,
) -> Result<Value> {
    let bucket = endpoint
        .get("bucket")
        .and_then(Value::as_str)
        .context("GCS endpoint is missing bucket")?;
    let mut prefix = endpoint
        .get("prefix")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim_matches('/')
        .to_owned();
    if tier == StorageCredentialTier::Lock {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str("locks");
    }
    if !prefix.is_empty() {
        prefix.push('/');
    }
    let resource_prefix = format!("projects/_/buckets/{bucket}/objects/{prefix}");
    let resource_literal = serde_json::to_string(&resource_prefix)?;
    let prefix_literal = serde_json::to_string(&prefix)?;
    let permissions = match tier {
        StorageCredentialTier::Read => vec!["inRole:roles/storage.objectViewer"],
        StorageCredentialTier::Append => vec![
            "inRole:roles/storage.objectViewer",
            "inRole:roles/storage.objectCreator",
        ],
        StorageCredentialTier::Maintain | StorageCredentialTier::Lock => {
            vec!["inRole:roles/storage.objectAdmin"]
        }
    };
    Ok(json!({
        "accessBoundary": {
            "accessBoundaryRules": [{
                "availableResource": format!("//storage.googleapis.com/projects/_/buckets/{bucket}"),
                "availablePermissions": permissions,
                "availabilityCondition": {
                    "expression": format!(
                        "resource.name.startsWith({resource_literal}) || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith({prefix_literal})"
                    )
                }
            }]
        }
    }))
}

fn validate_google_endpoint(
    raw: &str,
    production: &str,
    name: &str,
) -> std::result::Result<(), GcpDownscopeIssueError> {
    let endpoint = Url::parse(raw)
        .with_context(|| format!("GCP {name} endpoint is invalid"))
        .map_err(GcpDownscopeIssueError::Terminal)?;
    let loopback_http = endpoint.scheme() == "http"
        && endpoint
            .host_str()
            .is_some_and(|host| matches!(host, "127.0.0.1" | "::1" | "localhost"));
    if endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || raw != production && !loopback_http
    {
        return Err(terminal(&format!("GCP {name} endpoint is invalid")));
    }
    Ok(())
}

async fn require_json_success(
    response: Response,
    operation: &str,
) -> std::result::Result<Vec<u8>, GcpDownscopeIssueError> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !status.is_success() {
        let error = anyhow::anyhow!("{operation} returned HTTP {status}");
        return if is_unavailable_status(status) {
            Err(GcpDownscopeIssueError::Unavailable(error))
        } else {
            Err(GcpDownscopeIssueError::Terminal(error))
        };
    }
    if !content_type.starts_with("application/json") {
        return Err(terminal(&format!("{operation} returned non-JSON content")));
    }
    read_provider_response(response, operation).await
}

async fn read_provider_response(
    response: Response,
    operation: &str,
) -> std::result::Result<Vec<u8>, GcpDownscopeIssueError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(classify_transport)?;
        if body.len().saturating_add(chunk.len()) > MAX_PROVIDER_RESPONSE_BYTES {
            return Err(terminal(&format!(
                "{operation} exceeded response size limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn classify_transport(error: reqwest::Error) -> GcpDownscopeIssueError {
    if error.is_timeout() || error.is_connect() {
        GcpDownscopeIssueError::Unavailable(anyhow::Error::new(error))
    } else {
        GcpDownscopeIssueError::Terminal(anyhow::Error::new(error))
    }
}

fn is_unavailable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

fn terminal(message: &str) -> GcpDownscopeIssueError {
    GcpDownscopeIssueError::Terminal(anyhow::anyhow!(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand08::rngs::OsRng;
    use rsa::{
        pkcs8::{EncodePrivateKey, LineEnding},
        RsaPrivateKey,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn endpoint(prefix: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("bucket".to_owned(), Value::String("repo-bucket".to_owned())),
            ("prefix".to_owned(), Value::String(prefix.to_owned())),
        ])
    }

    #[test]
    fn boundary_maps_tiers_and_canonical_prefixes() {
        let read =
            credential_access_boundary(&endpoint("repo"), StorageCredentialTier::Read).unwrap();
        let read = read.to_string();
        assert!(read.contains("roles/storage.objectViewer"));
        assert!(read.contains("repo/"));
        assert!(read.contains("objectListPrefix"));

        let append = credential_access_boundary(&endpoint("repo/"), StorageCredentialTier::Append)
            .unwrap()
            .to_string();
        assert!(append.contains("roles/storage.objectCreator"));
        assert!(!append.contains("roles/storage.objectAdmin"));

        let lock = credential_access_boundary(&endpoint("repo"), StorageCredentialTier::Lock)
            .unwrap()
            .to_string();
        assert!(lock.contains("repo/locks/"));
        assert!(lock.contains("roles/storage.objectAdmin"));
    }

    #[tokio::test]
    async fn exchanges_issuer_for_downscoped_token_without_returning_source_secrets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for index in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 64 * 1024];
                let read = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
                let response_body = match index {
                    0 => {
                        assert!(request.starts_with("POST /oauth "));
                        assert!(body.contains(
                            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
                        ));
                        assert!(body.contains("assertion="));
                        r#"{"access_token":"bootstrap-token"}"#
                    }
                    1 => {
                        assert!(request.contains("/v1/projects/-/serviceAccounts/target@example.iam.gserviceaccount.com:generateAccessToken"));
                        assert!(request
                            .to_ascii_lowercase()
                            .contains("authorization: bearer bootstrap-token"));
                        assert!(body.contains(CLOUD_PLATFORM_SCOPE));
                        r#"{"accessToken":"source-token","expireTime":"2099-01-01T00:00:00Z"}"#
                    }
                    _ => {
                        assert!(request.starts_with("POST /sts "));
                        let fields = url::form_urlencoded::parse(body.as_bytes())
                            .into_owned()
                            .collect::<BTreeMap<_, _>>();
                        assert_eq!(fields["subject_token"], "source-token");
                        assert!(fields["options"].contains("repo/locks/"));
                        assert!(fields["options"].contains("objectListPrefix"));
                        r#"{"access_token":"downscoped-token","expires_in":900,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer"}"#
                    }
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let private_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let private_key = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let issuer_json = serde_json::json!({
            "client_email": "issuer@example.iam.gserviceaccount.com",
            "private_key": private_key.as_str(),
            "token_uri": format!("http://{address}/oauth"),
        })
        .to_string();
        let issuer = Credential {
            kind: CredentialKind::GcpServiceAccountJson,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            expires_at: None,
            account_name: None,
            account_key: None,
            sas_token: None,
            service_account_json: Some(issuer_json.clone()),
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
        };
        let policy = GcpDownscopePolicy {
            issuer_ref: "cred:gcp-issuer".to_owned(),
            service_account: "target@example.iam.gserviceaccount.com".to_owned(),
            iam_credentials_endpoint: format!("http://{address}"),
            token_exchange_endpoint: format!("http://{address}/sts"),
            tiers: vec![StorageCredentialTier::Lock],
            fallback: crate::topology::StsFallback::Disabled,
        };

        let issued = issue(
            &issuer,
            &policy,
            &endpoint("repo"),
            StorageCredentialTier::Lock,
            Duration::from_secs(900),
        )
        .await
        .unwrap();
        assert_eq!(issued.kind, CredentialKind::GcpAccessToken);
        assert_eq!(issued.access_token.as_deref(), Some("downscoped-token"));
        assert!(issued.expires_at.is_some());
        assert!(issued.service_account_json.is_none());
        let encoded = serde_json::to_string(&issued).unwrap();
        assert!(!encoded.contains("bootstrap-token"));
        assert!(!encoded.contains("source-token"));
        assert!(!encoded.contains(private_key.as_str()));
        assert!(!encoded.contains(&issuer_json));
    }

    #[test]
    fn fallback_status_classification_is_unavailable_only() {
        assert!(is_unavailable_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_unavailable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_unavailable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_unavailable_status(StatusCode::BAD_REQUEST));
        assert!(!is_unavailable_status(StatusCode::UNAUTHORIZED));
        assert!(!is_unavailable_status(StatusCode::FORBIDDEN));
    }
}
