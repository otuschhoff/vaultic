use std::{
    env,
    fs::File,
    io,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use ipnet::IpNet;
use sha2::{Digest, Sha256};
use slatedb::config::DbReaderOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReader, DbReaderMode, WriteBatch};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::{mpsc, watch, Mutex},
};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;
use vaulticdb::writer_role::{WriterRole as CoreWriterRole, WriterRoleState};

mod config;
mod error;
mod replication;
mod service;
mod storage;

pub mod proto {
    tonic::include_proto!("vaulticdb.v1");
}

use config::{Config, TransportConfig};
use proto::vaultic_db_server::VaulticDbServer;
use service::{publish_capsule_without_database, unix_time_ms_i64, DaemonState, Service};
use storage::Storage;

const PROTOCOL_VERSION: &str = "vaulticdb.v1";
const SCHEMA_VERSION: &str = "0";
const MAX_BATCH_ITEMS: u32 = 10_000;
const MAX_PAGE_ITEMS: u32 = 1_000;
const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 128;

include!("transport.rs");
