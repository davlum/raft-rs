use serde::{Serialize, Deserialize};
use raftrs::config::RaftConfig;
use raftrs::rpc::{AppendResp, Committed};
use raftrs::{run, Client};

// YourType must implement Serialize and Deserialize
// in order to be sent over the network
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct YourType {
    foo: String
}

fn main() {
    let this_host = "127.0.0.1:3333";
    let hosts = vec![
        "127.0.0.1:3333".to_owned(),
        "127.0.0.1:3334".to_owned(),
        "127.0.0.1:3335".to_owned(),
    ];
    let config = RaftConfig::mk_config(this_host, hosts);
    let (mut client, receiver) = run::<YourType>(config);

    let data = YourType { foo: "bar".to_owned() };

    client.append_cmd(data);

    // Receiving this indicates that this data has
    // been committed to the log on all nodes. Apply
    // to the state machine.
    let resp = receiver.recv().unwrap();
    assert_eq!(resp.i, 0);
}
