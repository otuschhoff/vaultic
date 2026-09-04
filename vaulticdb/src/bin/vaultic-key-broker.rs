#![allow(clippy::result_large_err)]

use std::{env, fs, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Mutex, Notify},
};
use vaulticdb::broker::{
    audit::{emit_rejection_event, emit_security_event},
    peer::inspect_peer,
    protocol::{handle_request, random_id, BrokerRequest, BrokerResponse, ConnectionProtocol},
    startup::{
        disable_core_dumps, identity_init, load, release_sign, remove_stale_socket, set_mode,
    },
    KeyBroker,
};

#[cfg(test)]
use vaulticdb::{
    broker::{
        audit::{rejection_event, security_event_json},
        peer::trusted_installation_path,
        protocol::{
            lease_challenge_response, AuthorizedOperation, OfflinePolicyMember, PeerProcess,
            PROTOCOL_VERSION,
        },
        startup::write_new_file,
        unix_time_ms, Capability, ContributionRejection,
    },
    encryption::recovery_capsule::{MemberProvider, UnlockPolicy},
};

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    disable_core_dumps();
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "identity-init")
    {
        return identity_init(&arguments[1..]);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "release-sign")
    {
        return release_sign(&arguments[1..]);
    }
    let config_path = arguments
        .first()
        .map(PathBuf::from)
        .or_else(|| env::var_os("VAULTIC_KEY_BROKER_CONFIG").map(PathBuf::from))
        .context("usage: vaultic-key-broker CONFIG.json | identity-init PRIVATE PUBLIC | release-sign PRIVATE EXECUTABLE COMPONENT VERSION IDENTITY OUTPUT")?;
    let startup = load(&config_path)?;
    let broker = startup.broker;
    let lock_notification = Arc::new(Notify::new());

    remove_stale_socket(&startup.socket_path).await?;
    let listener = UnixListener::bind(&startup.socket_path)
        .with_context(|| format!("bind broker socket {}", startup.socket_path.display()))?;
    set_mode(&startup.socket_path, 0o600)?;

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let broker = broker.clone();
                let endpoint_binding = startup.endpoint_binding.clone();
                let lock_notification = lock_notification.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_connection(stream, broker, endpoint_binding, lock_notification).await {
                        eprintln!("vaultic-key-broker: connection rejected: {error:#}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                broker.lock().await.lock();
                emit_security_event("critical", "lifecycle", "broker_locked_for_shutdown", &[]);
                break;
            }
        }
    }
    drop(listener);
    let _ = fs::remove_file(&startup.socket_path);
    Ok(())
}

async fn serve_connection(
    stream: UnixStream,
    broker: Arc<Mutex<KeyBroker>>,
    endpoint_binding: String,
    lock_notification: Arc<Notify>,
) -> Result<()> {
    let peer = inspect_peer(&stream)?;
    let connection_id = random_id();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut request = Vec::new();
    let mut protocol = ConnectionProtocol::default();
    loop {
        request.clear();
        let read = tokio::select! {
            read = reader.read_until(b'\n', &mut request) => read.context("read broker request")?,
            _ = lock_notification.notified() => break,
        };
        if read == 0 {
            break;
        }
        let response = if request.len() > MAX_REQUEST_BYTES {
            BrokerResponse::Error {
                code: "request_too_large",
                message: "broker request exceeds size limit".to_owned(),
            }
        } else {
            match serde_json::from_slice::<BrokerRequest>(&request) {
                Ok(request) => {
                    let requests_lock = request.requests_lock();
                    let response = handle_request(
                        request,
                        &broker,
                        &connection_id,
                        &peer,
                        &endpoint_binding,
                        &mut protocol,
                    )
                    .await
                    .unwrap_or_else(|error| {
                        emit_rejection_event(&error, &connection_id);
                        BrokerResponse::Error {
                            code: "request_rejected",
                            message: error.to_string(),
                        }
                    });
                    if requests_lock {
                        lock_notification.notify_waiters();
                    }
                    response
                }
                Err(error) => BrokerResponse::Error {
                    code: "invalid_request",
                    message: error.to_string(),
                },
            }
        };
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        writer.write_all(&encoded).await?;
        writer.flush().await?;
    }
    broker.lock().await.disconnect(&connection_id);
    emit_security_event(
        "notice",
        "lifecycle",
        "connection_closed_leases_revoked",
        &[("connection_id", connection_id)],
    );
    Ok(())
}

include!("vaultic-key-broker/tests.rs");
