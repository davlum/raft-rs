extern crate serde;
extern crate serde_json;

use std::fs::File;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use rand::Rng;


#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
struct JsonRaftConfig {
    data_dir: Option<String>,
    hosts: Vec<String>,
    connection_number: Option<u32>,
    timeout_min_ms: Option<u64>,
    // Can have separate timeouts for heartbeats but meh
    timeout_max_ms: Option<u64>,
    heartbeat_interval: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftConfig {
    data_dir: String,
    pub host: String,
    pub hosts: Vec<String>,
    timeout_min_ms: u64,
    // Can have separate timeouts for heartbeats but meh
    timeout_max_ms: u64,
    heartbeat_interval: u64,
}

impl RaftConfig {
    pub fn mk_config(host: &str, hosts: Vec<String>) -> RaftConfig {
        RaftConfig {
            data_dir: "data".to_owned(),
            host: host.to_owned(),
            hosts,
            timeout_min_ms: 150,
            timeout_max_ms: 300,
            heartbeat_interval: 20,
        }
    }

    pub fn new(config_path: &str, host: &str) -> serde_json::Result<Self> {
        let path = Path::new(&config_path);
        let display = path.display();
        // Open the path in read-only mode, returns `io::Result<File>`
        let file = match File::open(&path) {
            Err(why) => panic!("couldn't open {}: {}", display, why),
            Ok(file) => file,
        };
        let config: JsonRaftConfig = serde_json::from_reader(file)?;

        Ok(RaftConfig {
            data_dir: config.data_dir.unwrap_or("data".to_owned()),
            host: host.to_owned(),
            hosts: config.hosts,
            timeout_min_ms: config.timeout_min_ms.unwrap_or(150),
            timeout_max_ms: config.timeout_max_ms.unwrap_or(300),
            heartbeat_interval: config.heartbeat_interval.unwrap_or(20),
        })
    }

    fn get_timeout(&self) -> Duration {
        let range = self.timeout_min_ms..self.timeout_max_ms;
        let num = rand::thread_rng().gen_range(range);
        Duration::from_millis(num)
    }
}
