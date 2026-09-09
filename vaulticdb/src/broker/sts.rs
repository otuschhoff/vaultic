use std::{collections::BTreeMap, time::Duration};

use anyhow::{bail, Context, Result};
use aws_config::retry::RetryConfig;
use aws_sdk_sts::{
    config::{BehaviorVersion, Credentials as AwsCredentials, Region},
    error::SdkError,
    operation::assume_role::AssumeRoleError,
};
use serde_json::{json, Value};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::topology::{Credential, CredentialKind, S3StsPolicy, StorageCredentialTier};

pub(super) enum StsIssueError {
    Unavailable(anyhow::Error),
    Terminal(anyhow::Error),
}

pub(super) async fn issue(
    issuer: &Credential,
    policy: &S3StsPolicy,
    endpoint: &BTreeMap<String, Value>,
    tier: StorageCredentialTier,
    ttl: Duration,
) -> std::result::Result<Credential, StsIssueError> {
    if !matches!(
        issuer.kind,
        CredentialKind::AwsStatic | CredentialKind::S3Static
    ) {
        return Err(StsIssueError::Terminal(anyhow::anyhow!(
            "STS issuer must be a static S3 credential"
        )));
    }
    let access_key = issuer.access_key_id.as_deref().unwrap_or_default();
    let secret_key = issuer.secret_access_key.as_deref().unwrap_or_default();
    if access_key.is_empty() || secret_key.is_empty() {
        return Err(StsIssueError::Terminal(anyhow::anyhow!(
            "STS issuer credential is incomplete"
        )));
    }
    let role_arn = policy
        .roles
        .reference(tier)
        .context("STS role binding is not configured")
        .map_err(StsIssueError::Terminal)?;
    let session_policy = storage_session_policy(endpoint, tier).map_err(StsIssueError::Terminal)?;
    let credentials =
        AwsCredentials::new(access_key, secret_key, None, None, "vaultic-capsule-issuer");
    let config = aws_sdk_sts::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(policy.region.clone()))
        .endpoint_url(&policy.endpoint)
        .credentials_provider(credentials)
        .retry_config(RetryConfig::disabled())
        .build();
    let duration_seconds = i32::try_from(ttl.as_secs().max(900))
        .context("STS duration does not fit provider request")
        .map_err(StsIssueError::Terminal)?;
    let mut request = aws_sdk_sts::Client::from_conf(config)
        .assume_role()
        .role_arn(role_arn)
        .role_session_name(&policy.session_name)
        .duration_seconds(duration_seconds)
        .policy(session_policy);
    if let Some(external_id) = &policy.external_id {
        request = request.external_id(external_id);
    }
    let result = request.send().await.map_err(classify_error)?;
    let credentials = result
        .credentials()
        .context("STS response has no credentials")
        .map_err(StsIssueError::Terminal)?;
    let expires_at = OffsetDateTime::from_unix_timestamp(credentials.expiration().secs())
        .context("STS credential expiry is invalid")
        .and_then(|value| {
            value
                .format(&Rfc3339)
                .context("format STS credential expiry")
        })
        .map_err(StsIssueError::Terminal)?;
    Ok(Credential {
        kind: CredentialKind::S3Session,
        access_key_id: Some(credentials.access_key_id().to_owned()),
        secret_access_key: Some(credentials.secret_access_key().to_owned()),
        session_token: Some(credentials.session_token().to_owned()),
        expires_at: Some(expires_at),
        account_name: None,
        account_key: None,
        sas_token: None,
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

fn classify_error(error: SdkError<AssumeRoleError>) -> StsIssueError {
    match &error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => {
            StsIssueError::Unavailable(anyhow::Error::new(error))
        }
        SdkError::ServiceError(_)
            if error
                .raw_response()
                .is_some_and(|response| response.status().is_server_error()) =>
        {
            StsIssueError::Unavailable(anyhow::Error::new(error))
        }
        _ => StsIssueError::Terminal(anyhow::Error::new(error)),
    }
}

fn storage_session_policy(
    endpoint: &BTreeMap<String, Value>,
    tier: StorageCredentialTier,
) -> Result<String> {
    let field = |name: &str| {
        endpoint
            .get(name)
            .and_then(Value::as_str)
            .with_context(|| format!("S3 endpoint is missing {name:?}"))
    };
    let bucket = field("bucket")?;
    let mut prefix = field("prefix")?.trim_matches('/').to_owned();
    if tier == StorageCredentialTier::Lock {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str("locks");
    }
    if bucket.contains(['*', '?']) || prefix.contains(['*', '?']) {
        bail!("S3 bucket and prefix must not contain IAM wildcards");
    }
    let object_resource = if prefix.is_empty() {
        format!("arn:aws:s3:::{bucket}/*")
    } else {
        format!("arn:aws:s3:::{bucket}/{prefix}/*")
    };
    let list_prefix = if prefix.is_empty() {
        "*".to_owned()
    } else {
        format!("{prefix}/*")
    };
    let mut bucket_actions = vec!["s3:ListBucket"];
    let mut object_actions = vec!["s3:GetObject"];
    if tier != StorageCredentialTier::Read {
        bucket_actions.push("s3:ListBucketMultipartUploads");
        object_actions.extend([
            "s3:PutObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
        ]);
    }
    if matches!(
        tier,
        StorageCredentialTier::Maintain | StorageCredentialTier::Lock
    ) {
        object_actions.push("s3:DeleteObject");
    }
    serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": bucket_actions,
                "Resource": [format!("arn:aws:s3:::{bucket}")],
                "Condition": {"StringLike": {"s3:prefix": [list_prefix]}},
            },
            {
                "Effect": "Allow",
                "Action": object_actions,
                "Resource": [object_resource],
            },
        ],
    }))
    .context("encode STS session policy")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn issuer() -> Credential {
        serde_json::from_value(json!({
            "kind": "aws-static",
            "access_key_id": "issuer-access",
            "secret_access_key": "issuer-secret"
        }))
        .unwrap()
    }

    fn policy(endpoint: String) -> S3StsPolicy {
        S3StsPolicy {
            issuer_ref: "cred:issuer".to_owned(),
            endpoint,
            region: "us-east-1".to_owned(),
            session_name: "vaultic-test".to_owned(),
            external_id: Some("external-a".to_owned()),
            roles: crate::topology::CredentialBindings {
                storage_read: Some("arn:aws:iam::123456789012:role/read".to_owned()),
                storage_append: None,
                storage_maintain: None,
                storage_lock: None,
            },
            fallback: crate::topology::StsFallback::StaticOnUnavailable,
        }
    }

    fn endpoint() -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("bucket".to_owned(), Value::String("bucket-a".to_owned())),
            ("prefix".to_owned(), Value::String("repo-a".to_owned())),
        ])
    }

    #[test]
    fn generated_policy_never_escalates_tier() {
        let endpoint = endpoint();
        let read = storage_session_policy(&endpoint, StorageCredentialTier::Read).unwrap();
        assert!(
            !read.contains("PutObject")
                && !read.contains("DeleteObject")
                && !read.contains("MultipartUpload")
        );
        let append = storage_session_policy(&endpoint, StorageCredentialTier::Append).unwrap();
        assert!(
            append.contains("PutObject")
                && append.contains("AbortMultipartUpload")
                && append.contains("ListBucketMultipartUploads")
                && append.contains("ListMultipartUploadParts")
                && !append.contains("DeleteObject")
        );
        let maintain = storage_session_policy(&endpoint, StorageCredentialTier::Maintain).unwrap();
        assert!(maintain.contains("PutObject") && maintain.contains("DeleteObject"));
        let lock = storage_session_policy(&endpoint, StorageCredentialTier::Lock).unwrap();
        assert!(lock.contains("repo-a/locks/*") && lock.contains("DeleteObject"));
    }

    #[tokio::test]
    async fn assume_role_decodes_temporary_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 16 * 1024];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(
                request.contains("Action=AssumeRole") && request.contains("ExternalId=external-a")
            );
            let body = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>temporary-access</AccessKeyId><SecretAccessKey>temporary-secret</SecretAccessKey><SessionToken>temporary-token</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>request-a</RequestId></ResponseMetadata></AssumeRoleResponse>"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let issued = issue(
            &issuer(),
            &policy(format!("http://{address}")),
            &endpoint(),
            StorageCredentialTier::Read,
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| match error {
            StsIssueError::Unavailable(error) | StsIssueError::Terminal(error) => error,
        })
        .unwrap();
        assert_eq!(issued.kind, CredentialKind::S3Session);
        assert_eq!(issued.access_key_id.as_deref(), Some("temporary-access"));
        assert_eq!(issued.session_token.as_deref(), Some("temporary-token"));
        assert_eq!(issued.expires_at.as_deref(), Some("2030-01-01T00:00:00Z"));
    }

    #[tokio::test]
    async fn access_denied_is_terminal_not_fallback_eligible() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 16 * 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let body = r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>denied</Message></Error><RequestId>request-a</RequestId></ErrorResponse>"#;
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let result = issue(
            &issuer(),
            &policy(format!("http://{address}")),
            &endpoint(),
            StorageCredentialTier::Read,
            Duration::from_secs(30),
        )
        .await;
        assert!(matches!(result, Err(StsIssueError::Terminal(_))));
    }

    #[test]
    fn only_transport_timeouts_are_availability_failures_without_a_response() {
        let timeout: SdkError<AssumeRoleError> =
            SdkError::timeout_error(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
        assert!(matches!(
            classify_error(timeout),
            StsIssueError::Unavailable(_)
        ));
        let malformed: SdkError<AssumeRoleError> =
            SdkError::construction_failure(std::io::Error::other("invalid request"));
        assert!(matches!(
            classify_error(malformed),
            StsIssueError::Terminal(_)
        ));
    }
}
