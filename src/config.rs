extern crate serde;
extern crate serde_json;

use std::fs::File;
use serde::Deserialize;
use std::path::{Path, PathBuf};

const DEFAULT_TIMEOUT_MIN_MS: u64 = 150;
const DEFAULT_TIMEOUT_MAX_MS: u64 = 300;
const DEFAULT_HEARTBEAT_INTERVAL: u64 = 50;
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0";

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
struct JsonRaftConfig {
    data_dir: Option<PathBuf>,
    hosts: Vec<String>,
    connection_number: Option<u32>,
    timeout_min_ms: Option<u64>,
    // Can have separate timeouts for heartbeats but meh
    timeout_max_ms: Option<u64>,
    heartbeat_interval: Option<u64>,
    listen_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftConfig {
    pub(crate) host: String,
    pub(crate) hosts: Vec<String>,
    pub(crate) data_dir: PathBuf,
    pub(crate) connection_number: Option<u32>,
    pub(crate) listen_addr: String,
    pub(crate) timeout_min_ms: u64,
    // Can have separate timeouts for heartbeats but meh
    pub(crate) timeout_max_ms: u64,
    pub(crate) heartbeat_interval: u64,
}

impl RaftConfig {
    pub fn mk_config(host: &str, hosts: Vec<String>) -> RaftConfig {
        let host = host.to_owned();
        assert!(hosts.contains(&host));
        RaftConfig {
            data_dir: PathBuf::from("data"),
            host: host.to_owned(),
            hosts,
            connection_number: None,
            listen_addr: DEFAULT_LISTEN_ADDR.to_owned(),
            timeout_min_ms: DEFAULT_TIMEOUT_MIN_MS,
            timeout_max_ms: DEFAULT_TIMEOUT_MAX_MS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }

    pub fn mk_configf<P: AsRef<Path>>(host: &str, hosts: Vec<String>, data_dir: P) -> RaftConfig {
        let host = host.to_owned();
        assert!(hosts.contains(&host));
        RaftConfig {
            data_dir: data_dir.as_ref().to_owned(),
            host: host.to_owned(),
            hosts,
            connection_number: None,
            listen_addr: DEFAULT_LISTEN_ADDR.to_owned(),
            timeout_min_ms: DEFAULT_TIMEOUT_MIN_MS,
            timeout_max_ms: DEFAULT_TIMEOUT_MAX_MS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }

    pub fn new(path: &str, host: &str) -> serde_json::Result<Self> {
        // Open the path in read-only mode, returns `io::Result<File>`
        let file = match File::open(&path) {
            Err(why) => panic!("couldn't open {}: {}", path, why),
            Ok(file) => file,
        };
        let config: JsonRaftConfig = serde_json::from_reader(file)?;

        let host = host.to_owned();
        assert!(config.hosts.contains(&host));

        Ok(RaftConfig {
            data_dir: config.data_dir.unwrap_or(PathBuf::from("data")),
            host: host.to_owned(),
            hosts: config.hosts,
            listen_addr: config.listen_addr.unwrap_or(DEFAULT_LISTEN_ADDR.to_owned()),
            connection_number: config.connection_number,
            timeout_min_ms: config.timeout_min_ms.unwrap_or(DEFAULT_TIMEOUT_MIN_MS),
            timeout_max_ms: config.timeout_max_ms.unwrap_or(DEFAULT_TIMEOUT_MAX_MS),
            heartbeat_interval: config.heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL),
        })
    }
}

#[macro_export]
macro_rules! mk_config {
    (host = $host: expr, hosts = $hosts: expr) => {{
        RaftConfig {
            data_dir: "data".as_ref().to_owned(),
            host: host.to_owned(),
            hosts,
            connection_number: None,
            timeout_min_ms: 150,
            timeout_max_ms: 300,
            heartbeat_interval: 20,
        }
    }};
    (host = $host: expr, hosts = $hosts: expr, data_dir = $data_dir: expr) => {{
        RaftConfig {
            data_dir: data_dir,
            host: host.to_owned(),
            hosts,
            connection_number: None,
            timeout_min_ms: 150,
            timeout_max_ms: 300,
            heartbeat_interval: 20,
        }
    }};
}
