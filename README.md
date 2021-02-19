# raft-rs

In one terminal run

```
cargo run
```

In another;
```
$ nc 127.0.0.1 7878
{"RV":{"node_id":"1","term":1}}
```

It should echo back the first time with the same reply, but any subsequent open and sends
of the same message should just close the connection.
For some reason on branch `doesnt`, the conditional seems to affect this behaviour. After the
first send, the channel sends back `receiving on a closed channel`.
