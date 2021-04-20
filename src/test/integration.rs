use crate::config::RaftConfig;
use std::thread;
use crate::run;
use crate::test::util::TestDir;
use std::time::Duration;
use crate::rpc::AppendResp;

#[test]
fn integration() {
    env_logger::builder().is_test(true).try_init();
    let hosts = vec![
        "127.0.0.1:3333".to_owned(),
        "127.0.0.1:3334".to_owned(),
        "127.0.0.1:3335".to_owned()
    ];
    let mut cli_vec = vec![];
    let mut recv_vec = vec![];
    for host in hosts.clone() {
        let dir = TestDir::new();
        let hosts = hosts.clone();
        let (cli, receiver) = run::<String>(RaftConfig::mk_configf(&host, hosts.clone(), dir));
        cli_vec.push((host, cli));
        recv_vec.push(receiver);
    };
    thread::sleep(Duration::from_secs(1));

    let mut leader = None;
    let mut leader_vec = vec![];
    for (host, mut cli) in cli_vec {
        match cli.append_cmd("foobar".to_owned()) {
            AppendResp::Appended(i) => {
                leader = Some(host);
                debug_assert_eq!(i, 0)
            }
            AppendResp::NotLeader(maybe_leader) => {
                leader_vec.push(maybe_leader)
            }
            AppendResp::Error(e) => panic!("{:?}", e)
        }
    }
    for maybe_leader in leader_vec {
        debug_assert_eq!(leader, maybe_leader);
    }
    for receiver in recv_vec {
        let resp = receiver.recv().unwrap();
        debug_assert_eq!(resp.0, 0);
    }
}
