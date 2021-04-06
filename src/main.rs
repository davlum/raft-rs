use raftrs;
use std::env;
use raftrs::RaftConfig;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = RaftConfig::new(&args[1], &args[2]).unwrap();
    raftrs::run::<String>(config);
}
