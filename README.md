![build](https://github.com/davlum/raft-rs/workflows/raftrs/badge.svg)

# raftrs

An implementation of [raft](https://raft.github.io/raft.pdf) in rust.

![Under construction](https://i.redd.it/satf62q0k0f51.png)

The client is fairly straight forward. At a minimum pass in the hostname of
the current node and its peers, this will return a `Client` and the 
receiving end of a channel. After adding a command through the `append_cmd` 
method, receiving data on the channel indicates that the command has been 
replicated to all peers and can be committed.
```rust
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
```
## Example usage

A very simple example of a distributed key-value store - backed by 
[leveldb](https://github.com/google/leveldb) - can be found in the
[examples](examples/dist_leveldb.rs). It can be run with [Docker][docker].
The Docker container takes about 3 minutes to build. The client is very 
rudimentary and should be written using HTTP instead of TCP. In this example
the hosts can be found at localhost:8001, 8002 and 8003.

```bash
$ docker-compose up  # Or docker compose up -d to stay in the same shell
$ nc 127.0.0.1 8001  # In another shell, or the same one if ran with -d
put 1 bar
Ok: Appended at 0
$ nc 127.0.0.1 8002
get 1
Ok: bar
$ nc 127.0.0.1 8001
put 2 foo
Ok: Appended at 1
$ nc 127.0.0.1 8001
del 1
Ok: Appended at 2
$ 127.0.0.1 8002
get 1
Error: value does not exist
$ nc 127.0.0.1 8003
put 1 bar
Error: Leader is 127.0.0.1:8001
```

To simulate network partitioning/node failure we can use the following commands:
* `docker-compose restart raft1  #  or raft2 or raft3`
* `docker-compose stop raft1`
* `docker-compose start raft1`

[docker]: <https://www.docker.com/>
