//! Local, cloud, PKCS#11, and hardware key wrapping providers.

use std::{collections::HashMap, path::PathBuf};

use aes_gcm::{
    aead::{Aead, Payload},
    Aes256Gcm, KeyInit, Nonce,
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_kms::{primitives::Blob, Client as AwsKmsClient};
use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine,
};
use cryptoki::{
    context::{CInitializeArgs, Pkcs11},
    mechanism::{
        aead::GcmParams,
        rsa::{PkcsMgfType, PkcsOaepParams, PkcsOaepSource},
        Mechanism, MechanismType,
    },
    object::{Attribute, AttributeType, KeyType, ObjectClass},
    session::UserType,
    slot::Slot,
    types::{AuthPin, Ulong},
};
use hkdf::Hkdf;
use p256::{ecdh::EphemeralSecret, elliptic_curve::sec1::ToEncodedPoint, PublicKey};
use rand::RngCore;
use rand08::rngs::OsRng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, Default)]
pub struct ProviderCredentials {
    token_files: HashMap<String, PathBuf>,
}

impl ProviderCredentials {
    pub fn new(token_files: HashMap<String, PathBuf>) -> Self {
        Self { token_files }
    }

    fn token(&self, provider: &str) -> Result<Option<String>> {
        self.token_files
            .get(provider)
            .map(|path| token_from_file(path))
            .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct KeyContext<'a> {
    pub repository_id: &'a str,
    pub slot_id: &'a str,
    pub key_reference: &'a str,
    pub dek_version: u32,
    pub purpose: &'a str,
}

impl KeyContext<'_> {
    fn binding(&self) -> String {
        format!(
            "vaulticdb\0{}\0{}\0{}\0{}",
            self.repository_id, self.slot_id, self.dek_version, self.purpose
        )
    }

    fn aws_context(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "vaultic:repository".to_owned(),
                self.repository_id.to_owned(),
            ),
            ("vaultic:slot".to_owned(), self.slot_id.to_owned()),
            (
                "vaultic:dek-version".to_owned(),
                self.dek_version.to_string(),
            ),
            ("vaultic:purpose".to_owned(), self.purpose.to_owned()),
        ])
    }
}

#[async_trait]
pub trait KeyProvider: Send + Sync {
    fn name(&self) -> &'static str;
    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>>;
    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>>;
}

pub async fn from_config(
    provider: &str,
    credentials: &ProviderCredentials,
) -> Result<Option<Box<dyn KeyProvider>>> {
    match provider {
        "aws-kms" => Ok(Some(Box::new(AwsKmsProvider::from_environment().await))),
        "azure-key-vault" => Ok(credentials
            .token(provider)?
            .map(|value| Box::new(AzureKeyVaultProvider::new(value)) as Box<dyn KeyProvider>)),
        "gcp-kms" => Ok(credentials
            .token(provider)?
            .map(|value| Box::new(GoogleCloudKmsProvider::new(value)) as Box<dyn KeyProvider>)),
        "vault-transit" => Ok(credentials
            .token(provider)?
            .map(|value| Box::new(VaultTransitProvider::new(value)) as Box<dyn KeyProvider>)),
        "pkcs11" => Ok(credentials
            .token(provider)?
            .map(|value| Box::new(Pkcs11Provider::new(value)) as Box<dyn KeyProvider>)),
        "yubikey-piv" => Ok(credentials
            .token(provider)?
            .map(|value| Box::new(YubikeyPivProvider::new(value)) as Box<dyn KeyProvider>)),
        "fido2-hmac-secret" => credentials
            .token(provider)?
            .map(Fido2HmacSecretProvider::from_base64)
            .transpose()
            .map(|provider| provider.map(|value| Box::new(value) as Box<dyn KeyProvider>)),
        value => bail!("unsupported metadata key provider {value:?}"),
    }
}

fn token_from_file(path: &std::path::Path) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            bail!("cloud token file must not be accessible by group or others");
        }
    }
    let mut token = std::fs::read_to_string(path)
        .with_context(|| format!("read cloud token file {}", path.display()))?;
    while token.ends_with(char::is_whitespace) {
        token.pop();
    }
    if token.is_empty() {
        bail!("cloud token file is empty");
    }
    Ok(token)
}

pub async fn for_management(
    provider: &str,
    bearer_token: Option<String>,
) -> Result<Box<dyn KeyProvider>> {
    match provider {
        "aws-kms" if bearer_token.is_none() => {
            Ok(Box::new(AwsKmsProvider::from_environment().await))
        }
        "azure-key-vault" => Ok(Box::new(AzureKeyVaultProvider::new(
            bearer_token.context("Azure Key Vault requires a bearer-token file")?,
        ))),
        "gcp-kms" => Ok(Box::new(GoogleCloudKmsProvider::new(
            bearer_token.context("Google Cloud KMS requires a bearer-token file")?,
        ))),
        "vault-transit" => Ok(Box::new(VaultTransitProvider::new(
            bearer_token.context("Vault Transit requires a token file")?,
        ))),
        "pkcs11" => Ok(Box::new(Pkcs11Provider::new(
            bearer_token.context("PKCS#11 requires a PIN file")?,
        ))),
        "yubikey-piv" => Ok(Box::new(YubikeyPivProvider::new(
            bearer_token.context("YubiKey PIV requires a PIN file")?,
        ))),
        "fido2-hmac-secret" => Ok(Box::new(Fido2HmacSecretProvider::from_base64(
            bearer_token.context("FIDO2 hmac-secret output is required")?,
        )?)),
        "aws-kms" => bail!("AWS KMS uses the SDK credential chain, not a bearer token"),
        value => bail!("unsupported metadata key provider {value:?}"),
    }
}

pub(crate) struct AwsKmsProvider {
    client: AwsKmsClient,
}

impl AwsKmsProvider {
    pub(crate) async fn from_environment() -> Self {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        Self {
            client: AwsKmsClient::new(&config),
        }
    }
}

#[async_trait]
impl KeyProvider for AwsKmsProvider {
    fn name(&self) -> &'static str {
        "aws-kms"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        let output = self
            .client
            .encrypt()
            .key_id(context.key_reference)
            .plaintext(Blob::new(plaintext))
            .set_encryption_context(Some(context.aws_context()))
            .send()
            .await
            .context("AWS KMS Encrypt")?;
        Ok(output
            .ciphertext_blob()
            .context("AWS KMS returned no ciphertext")?
            .as_ref()
            .to_vec())
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        let output = self
            .client
            .decrypt()
            .key_id(context.key_reference)
            .ciphertext_blob(Blob::new(ciphertext))
            .set_encryption_context(Some(context.aws_context()))
            .send()
            .await
            .context("AWS KMS Decrypt")?;
        Ok(Zeroizing::new(
            output
                .plaintext()
                .context("AWS KMS returned no plaintext")?
                .as_ref()
                .to_vec(),
        ))
    }
}

pub(crate) struct AzureKeyVaultProvider {
    client: Client,
    bearer_token: Zeroizing<String>,
}

impl AzureKeyVaultProvider {
    pub(crate) fn new(bearer_token: String) -> Self {
        Self {
            client: Client::new(),
            bearer_token: Zeroizing::new(bearer_token),
        }
    }
}

#[derive(Serialize)]
struct AzureRequest<'a> {
    alg: &'static str,
    value: &'a str,
}

#[derive(Deserialize)]
struct AzureResponse {
    value: String,
}

#[async_trait]
impl KeyProvider for AzureKeyVaultProvider {
    fn name(&self) -> &'static str {
        "azure-key-vault"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        azure_call(self, context, "wrapkey", plaintext).await
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        azure_call(self, context, "unwrapkey", ciphertext)
            .await
            .map(Zeroizing::new)
    }
}

async fn azure_call(
    provider: &AzureKeyVaultProvider,
    context: &KeyContext<'_>,
    operation: &str,
    value: &[u8],
) -> Result<Vec<u8>> {
    validate_azure_key_reference(context.key_reference)?;
    let encoded = URL_SAFE_NO_PAD.encode(value);
    let response = provider
        .client
        .post(format!(
            "{}/{}?api-version=7.4",
            context.key_reference.trim_end_matches('/'),
            operation
        ))
        .bearer_auth(provider.bearer_token.as_str())
        .json(&AzureRequest {
            alg: "RSA-OAEP-256",
            value: &encoded,
        })
        .send()
        .await
        .context("Azure Key Vault request")?
        .error_for_status()
        .context("Azure Key Vault rejected request")?
        .json::<AzureResponse>()
        .await
        .context("decode Azure Key Vault response")?;
    URL_SAFE_NO_PAD
        .decode(response.value)
        .context("decode Azure Key Vault ciphertext")
}

fn validate_azure_key_reference(reference: &str) -> Result<()> {
    let url = reqwest::Url::parse(reference).context("parse Azure key reference")?;
    let host = url.host_str().unwrap_or_default();
    let segments = url
        .path_segments()
        .map(|segments| {
            segments
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if url.scheme() != "https"
        || !(host.ends_with(".vault.azure.net") || host.ends_with(".managedhsm.azure.net"))
        || segments.len() != 3
        || segments[0] != "keys"
    {
        bail!("Azure key reference must be a versioned Key Vault or Managed HSM HTTPS URL");
    }
    Ok(())
}

pub(crate) struct GoogleCloudKmsProvider {
    client: Client,
    bearer_token: Zeroizing<String>,
}

impl GoogleCloudKmsProvider {
    pub(crate) fn new(bearer_token: String) -> Self {
        Self {
            client: Client::new(),
            bearer_token: Zeroizing::new(bearer_token),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GoogleRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    plaintext: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ciphertext: Option<&'a str>,
    additional_authenticated_data: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleResponse {
    #[serde(default)]
    ciphertext: String,
    #[serde(default)]
    plaintext: String,
}

#[async_trait]
impl KeyProvider for GoogleCloudKmsProvider {
    fn name(&self) -> &'static str {
        "gcp-kms"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        let value = BASE64.encode(plaintext);
        google_call(self, context, "encrypt", Some(&value), None)
            .await
            .and_then(|response| {
                BASE64
                    .decode(response.ciphertext)
                    .context("decode Google Cloud KMS ciphertext")
            })
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        let value = BASE64.encode(ciphertext);
        google_call(self, context, "decrypt", None, Some(&value))
            .await
            .and_then(|response| {
                BASE64
                    .decode(response.plaintext)
                    .context("decode Google Cloud KMS plaintext")
            })
            .map(Zeroizing::new)
    }
}

async fn google_call(
    provider: &GoogleCloudKmsProvider,
    context: &KeyContext<'_>,
    operation: &str,
    plaintext: Option<&str>,
    ciphertext: Option<&str>,
) -> Result<GoogleResponse> {
    if !context.key_reference.starts_with("projects/")
        || !context.key_reference.contains("/cryptoKeys/")
        || context.key_reference.contains("/cryptoKeyVersions/")
    {
        bail!("Google Cloud KMS key reference must name a CryptoKey");
    }
    let aad = BASE64.encode(context.binding());
    provider
        .client
        .post(format!(
            "https://cloudkms.googleapis.com/v1/{}:{}",
            context.key_reference, operation
        ))
        .bearer_auth(provider.bearer_token.as_str())
        .json(&GoogleRequest {
            plaintext,
            ciphertext,
            additional_authenticated_data: &aad,
        })
        .send()
        .await
        .context("Google Cloud KMS request")?
        .error_for_status()
        .context("Google Cloud KMS rejected request")?
        .json()
        .await
        .context("decode Google Cloud KMS response")
}

pub(crate) struct VaultTransitProvider {
    client: Client,
    token: Zeroizing<String>,
}

impl VaultTransitProvider {
    pub(crate) fn new(token: String) -> Self {
        Self {
            client: Client::new(),
            token: Zeroizing::new(token),
        }
    }
}

#[derive(Serialize)]
struct VaultEncryptRequest<'a> {
    plaintext: &'a str,
    context: &'a str,
}

#[derive(Serialize)]
struct VaultDecryptRequest<'a> {
    ciphertext: &'a str,
    context: &'a str,
}

#[derive(Deserialize)]
struct VaultResponse {
    data: VaultResponseData,
}

#[derive(Deserialize)]
struct VaultResponseData {
    #[serde(default)]
    ciphertext: String,
    #[serde(default)]
    plaintext: String,
}

#[async_trait]
impl KeyProvider for VaultTransitProvider {
    fn name(&self) -> &'static str {
        "vault-transit"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        let endpoint = vault_transit_endpoint(context.key_reference, "encrypt")?;
        let plaintext = BASE64.encode(plaintext);
        let binding = BASE64.encode(context.binding());
        let response = self
            .client
            .post(endpoint)
            .header("X-Vault-Token", self.token.as_str())
            .json(&VaultEncryptRequest {
                plaintext: &plaintext,
                context: &binding,
            })
            .send()
            .await
            .context("Vault Transit encrypt request")?
            .error_for_status()
            .context("Vault Transit rejected encrypt request")?
            .json::<VaultResponse>()
            .await
            .context("decode Vault Transit encrypt response")?;
        if response.data.ciphertext.is_empty() {
            bail!("Vault Transit returned no ciphertext");
        }
        Ok(response.data.ciphertext.into_bytes())
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        let endpoint = vault_transit_endpoint(context.key_reference, "decrypt")?;
        let ciphertext =
            std::str::from_utf8(ciphertext).context("Vault Transit ciphertext is not UTF-8")?;
        let binding = BASE64.encode(context.binding());
        let response = self
            .client
            .post(endpoint)
            .header("X-Vault-Token", self.token.as_str())
            .json(&VaultDecryptRequest {
                ciphertext,
                context: &binding,
            })
            .send()
            .await
            .context("Vault Transit decrypt request")?
            .error_for_status()
            .context("Vault Transit rejected decrypt request")?
            .json::<VaultResponse>()
            .await
            .context("decode Vault Transit decrypt response")?;
        if response.data.plaintext.is_empty() {
            bail!("Vault Transit returned no plaintext");
        }
        BASE64
            .decode(response.data.plaintext)
            .context("decode Vault Transit plaintext")
            .map(Zeroizing::new)
    }
}

fn vault_transit_endpoint(reference: &str, operation: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(reference).context("parse Vault Transit key reference")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("Vault Transit key reference must be an HTTPS URL without credentials, query, or fragment");
    }
    let segments = url
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())
        .unwrap_or_default();
    let keys_index = segments
        .iter()
        .position(|segment| *segment == "keys")
        .context("Vault Transit key reference must contain /keys/{name}")?;
    if segments.first() != Some(&"v1")
        || keys_index < 2
        || keys_index + 2 != segments.len()
        || segments[keys_index + 1].is_empty()
    {
        bail!("Vault Transit key reference must end in /v1/{{mount}}/keys/{{name}}");
    }
    let mut endpoint = segments[..keys_index].join("/");
    endpoint.push('/');
    endpoint.push_str(operation);
    endpoint.push('/');
    endpoint.push_str(segments[keys_index + 1]);
    url.set_path(&endpoint);
    Ok(url)
}

pub(crate) struct Pkcs11Provider {
    pin: Zeroizing<String>,
}

impl Pkcs11Provider {
    pub(crate) fn new(pin: String) -> Self {
        Self {
            pin: Zeroizing::new(pin),
        }
    }

    fn crypt(&self, context: &KeyContext<'_>, input: &[u8], encrypt: bool) -> Result<Vec<u8>> {
        let reference = parse_pkcs11_reference(context.key_reference)?;
        let pkcs11 = Pkcs11::new(&reference.module_path).context("load PKCS#11 module")?;
        pkcs11
            .initialize(CInitializeArgs::OsThreads)
            .context("initialize PKCS#11 module")?;
        let session = pkcs11
            .open_ro_session(Slot::try_from(reference.slot_id)?)
            .context("open PKCS#11 session")?;
        session
            .login(
                UserType::User,
                Some(&AuthPin::new(self.pin.as_str().to_owned())),
            )
            .context("log in to PKCS#11 token")?;
        let keys = session.find_objects(&[
            Attribute::Class(ObjectClass::SECRET_KEY),
            Attribute::KeyType(KeyType::AES),
            Attribute::Label(reference.object.into_bytes()),
        ])?;
        if keys.len() != 1 {
            bail!("PKCS#11 key reference must resolve to exactly one AES secret key");
        }
        let aad = context.binding();
        if encrypt {
            let mut nonce = [0u8; 12];
            rand::rng().fill_bytes(&mut nonce);
            let params = GcmParams::new(&mut nonce, aad.as_bytes(), Ulong::from(128u64))?;
            let ciphertext = session.encrypt(&Mechanism::AesGcm(params), keys[0], input)?;
            let mut output = nonce.to_vec();
            output.extend_from_slice(&ciphertext);
            Ok(output)
        } else {
            if input.len() < 12 + 16 {
                bail!("PKCS#11 AES-GCM ciphertext is truncated");
            }
            let mut nonce = input[..12].to_vec();
            let params = GcmParams::new(&mut nonce, aad.as_bytes(), Ulong::from(128u64))?;
            session
                .decrypt(&Mechanism::AesGcm(params), keys[0], &input[12..])
                .context("PKCS#11 AES-GCM authentication failed")
        }
    }
}

#[async_trait]
impl KeyProvider for Pkcs11Provider {
    fn name(&self) -> &'static str {
        "pkcs11"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        let pin = self.pin.as_str().to_owned();
        let context = OwnedKeyContext::from(context);
        let plaintext = plaintext.to_vec();
        tokio::task::spawn_blocking(move || {
            Pkcs11Provider::new(pin).crypt(&context.as_borrowed(), &plaintext, true)
        })
        .await
        .context("join PKCS#11 wrap operation")?
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        let pin = self.pin.as_str().to_owned();
        let context = OwnedKeyContext::from(context);
        let ciphertext = ciphertext.to_vec();
        tokio::task::spawn_blocking(move || {
            Pkcs11Provider::new(pin)
                .crypt(&context.as_borrowed(), &ciphertext, false)
                .map(Zeroizing::new)
        })
        .await
        .context("join PKCS#11 unwrap operation")?
    }
}

pub struct YubikeyPivProvider {
    pin: Zeroizing<String>,
}

impl YubikeyPivProvider {
    pub fn new(pin: String) -> Self {
        Self {
            pin: Zeroizing::new(pin),
        }
    }

    fn crypt(&self, context: &KeyContext<'_>, input: &[u8], encrypt: bool) -> Result<Vec<u8>> {
        let reference = parse_yubikey_piv_reference(context.key_reference)?;
        let pkcs11 = Pkcs11::new(&reference.module_path).context("load YKCS11 module")?;
        pkcs11
            .initialize(CInitializeArgs::OsThreads)
            .context("initialize YKCS11 module")?;
        let session = pkcs11
            .open_ro_session(Slot::try_from(reference.slot_id)?)
            .context("open YubiKey PIV session")?;
        let public_keys = session.find_objects(&[
            Attribute::Class(ObjectClass::PUBLIC_KEY),
            Attribute::KeyType(KeyType::RSA),
            Attribute::Id(reference.key_id.clone()),
        ])?;
        if public_keys.len() != 1 {
            bail!("YubiKey PIV reference must resolve to exactly one RSA public key");
        }
        let attributes = session.get_attributes(
            public_keys[0],
            &[AttributeType::Modulus, AttributeType::PublicExponent],
        )?;
        let modulus = attributes.iter().find_map(|attribute| match attribute {
            Attribute::Modulus(value) => Some(value.as_slice()),
            _ => None,
        });
        let exponent = attributes.iter().find_map(|attribute| match attribute {
            Attribute::PublicExponent(value) => Some(value.as_slice()),
            _ => None,
        });
        let actual_fingerprint = rsa_public_key_fingerprint(
            modulus.context("YubiKey PIV public key has no RSA modulus")?,
            exponent.context("YubiKey PIV public key has no RSA exponent")?,
        );
        if actual_fingerprint != reference.public_key_fingerprint {
            bail!("YubiKey PIV public key fingerprint does not match key reference");
        }
        if !encrypt {
            session
                .login(
                    UserType::User,
                    Some(&AuthPin::new(self.pin.as_str().to_owned())),
                )
                .context("verify YubiKey PIV PIN")?;
        }
        let keys = if encrypt {
            public_keys
        } else {
            session.find_objects(&[
                Attribute::Class(ObjectClass::PRIVATE_KEY),
                Attribute::KeyType(KeyType::RSA),
                Attribute::Id(reference.key_id),
            ])?
        };
        if keys.len() != 1 {
            bail!("YubiKey PIV reference must resolve to exactly one RSA key");
        }
        let label = context.binding();
        let params = PkcsOaepParams::new(
            MechanismType::SHA256,
            PkcsMgfType::MGF1_SHA256,
            PkcsOaepSource::data_specified(label.as_bytes()),
        );
        if encrypt {
            session
                .encrypt(&Mechanism::RsaPkcsOaep(params), keys[0], input)
                .context("encrypt share with YubiKey PIV public key")
        } else {
            session
                .decrypt(&Mechanism::RsaPkcsOaep(params), keys[0], input)
                .context("decrypt share with YubiKey PIV private key (PIN or touch rejected)")
        }
    }
}

#[async_trait]
impl KeyProvider for YubikeyPivProvider {
    fn name(&self) -> &'static str {
        "yubikey-piv"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        let pin = self.pin.as_str().to_owned();
        let context = OwnedKeyContext::from(context);
        let plaintext = plaintext.to_vec();
        tokio::task::spawn_blocking(move || {
            YubikeyPivProvider::new(pin).crypt(&context.as_borrowed(), &plaintext, true)
        })
        .await
        .context("join YubiKey PIV wrap operation")?
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        let pin = self.pin.as_str().to_owned();
        let context = OwnedKeyContext::from(context);
        let ciphertext = ciphertext.to_vec();
        tokio::task::spawn_blocking(move || {
            YubikeyPivProvider::new(pin)
                .crypt(&context.as_borrowed(), &ciphertext, false)
                .map(Zeroizing::new)
        })
        .await
        .context("join YubiKey PIV unwrap operation")?
    }
}

pub struct Fido2HmacSecretProvider {
    secret: Zeroizing<[u8; 32]>,
}

impl Fido2HmacSecretProvider {
    pub fn from_base64(value: String) -> Result<Self> {
        let mut decoded = Zeroizing::new(
            BASE64
                .decode(value)
                .context("decode FIDO2 hmac-secret output")?,
        );
        let secret: [u8; 32] = decoded
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("FIDO2 hmac-secret output must be 32 bytes"))?;
        decoded.zeroize();
        Ok(Self {
            secret: Zeroizing::new(secret),
        })
    }

    fn crypt(&self, context: &KeyContext<'_>, input: &[u8], encrypt: bool) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(self.secret.as_slice()).expect("32-byte AES key");
        let aad = context.binding();
        if encrypt {
            let mut nonce = [0u8; 12];
            rand::rng().fill_bytes(&mut nonce);
            let ciphertext = cipher
                .encrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: input,
                        aad: aad.as_bytes(),
                    },
                )
                .map_err(|_| anyhow::anyhow!("encrypt share with FIDO2 hmac-secret failed"))?;
            let mut output = nonce.to_vec();
            output.extend_from_slice(&ciphertext);
            Ok(output)
        } else {
            if input.len() < 28 {
                bail!("FIDO2 hmac-secret ciphertext is truncated");
            }
            cipher
                .decrypt(
                    Nonce::from_slice(&input[..12]),
                    Payload {
                        msg: &input[12..],
                        aad: aad.as_bytes(),
                    },
                )
                .map_err(|_| anyhow::anyhow!("FIDO2 hmac-secret authentication failed"))
        }
    }
}

#[async_trait]
impl KeyProvider for Fido2HmacSecretProvider {
    fn name(&self) -> &'static str {
        "fido2-hmac-secret"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        self.crypt(context, plaintext, true)
    }

    async fn unwrap(
        &self,
        context: &KeyContext<'_>,
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        self.crypt(context, ciphertext, false).map(Zeroizing::new)
    }
}

const MACOS_SECURE_ENCLAVE_VERSION: u8 = 1;
const MACOS_SECURE_ENCLAVE_PUBLIC_KEY_BYTES: usize = 65;
const MACOS_SECURE_ENCLAVE_NONCE_BYTES: usize = 12;

struct MacosSecureEnclaveReference {
    application_tag: String,
    public_key: Vec<u8>,
}

pub struct MacosSecureEnclaveProvider {
    public_key: PublicKey,
}

impl MacosSecureEnclaveProvider {
    pub fn from_key_reference(reference: &str) -> Result<Self> {
        let reference = parse_macos_secure_enclave_reference(reference)?;
        Ok(Self {
            public_key: PublicKey::from_sec1_bytes(&reference.public_key)
                .context("decode macOS Secure Enclave public key")?,
        })
    }
}

#[async_trait]
impl KeyProvider for MacosSecureEnclaveProvider {
    fn name(&self) -> &'static str {
        "macos-secure-enclave"
    }

    async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
        let ephemeral_secret = EphemeralSecret::random(&mut OsRng);
        let ephemeral_public = PublicKey::from(&ephemeral_secret);
        let shared_secret = ephemeral_secret.diffie_hellman(&self.public_key);
        let key = macos_secure_enclave_key(shared_secret.raw_secret_bytes().as_slice(), context)?;
        let cipher = Aes256Gcm::new_from_slice(key.as_slice()).expect("32-byte AES key");
        let mut nonce = [0u8; MACOS_SECURE_ENCLAVE_NONCE_BYTES];
        rand::rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: context.binding().as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypt share for macOS Secure Enclave failed"))?;
        let mut output = Vec::with_capacity(
            1 + MACOS_SECURE_ENCLAVE_PUBLIC_KEY_BYTES
                + MACOS_SECURE_ENCLAVE_NONCE_BYTES
                + ciphertext.len(),
        );
        output.push(MACOS_SECURE_ENCLAVE_VERSION);
        output.extend_from_slice(ephemeral_public.to_encoded_point(false).as_bytes());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    async fn unwrap(
        &self,
        _context: &KeyContext<'_>,
        _ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        bail!("macOS Secure Enclave unwrap requires the native custodian helper")
    }
}

include!("providers/hardware.rs");

include!("providers/tests.rs");
