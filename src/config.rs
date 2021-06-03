extern crate serde;
extern crate serde_json;

use std::path::{Path, PathBuf};

const DEFAULT_TIMEOUT_MIN_MS: u64 = 150;
const DEFAULT_TIMEOUT_MAX_MS: u64 = 300;
const DEFAULT_HEARTBEAT_INTERVAL: u64 = 50;
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0";


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftConfig {
    pub host: String,
    pub hosts: Vec<String>,
    pub data_dir: PathBuf,
    pub connection_number: Option<u32>,
    pub listen_addr: String,
    pub timeout_min_ms: u64,
    // Can have separate timeouts for heartbeats but meh
    pub timeout_max_ms: u64,
    pub heartbeat_interval: u64,
}

impl RaftConfig {
    pub fn new(host: &str, hosts: Vec<String>) -> Self {
        RaftConfig {
            host: host.to_owned(),
            hosts,
            data_dir: PathBuf::from("data"),
            connection_number: None,
            listen_addr: DEFAULT_LISTEN_ADDR.to_owned(),
            timeout_min_ms: DEFAULT_TIMEOUT_MIN_MS,
            timeout_max_ms: DEFAULT_TIMEOUT_MAX_MS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }

    pub fn new_with_dir<P: AsRef<Path>>(host: &str, hosts: Vec<String>, data_dir: P) -> Self {
        let mut data_buf = PathBuf::new();
        data_buf.push(data_dir);
        RaftConfig {
            host: host.to_owned(),
            hosts,
            data_dir: data_buf,
            connection_number: None,
            listen_addr: DEFAULT_LISTEN_ADDR.to_owned(),
            timeout_min_ms: DEFAULT_TIMEOUT_MIN_MS,
            timeout_max_ms: DEFAULT_TIMEOUT_MAX_MS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}
