#[cfg(test)]
mod integration {
    use crate::config::RaftConfig;
    use std::thread;
    use crate::run;
    use std::time::Duration;
    use crate::rpc::AppendResp;
    use rand;
    use log::info;
    use std::{
        fs,
        path::{Path, PathBuf},
    };
    use futures::StreamExt;

    pub struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        pub fn new() -> TestDir {
            let mut path_buf = PathBuf::new();
            path_buf.push("target");
            path_buf.push("test-data");
            path_buf.push(format!("test-{:020}", rand::random::<u64>()));
            fs::create_dir_all(&path_buf).unwrap();
            TestDir { path: path_buf }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self).expect("Unable to delete test data directory");
        }
    }

    impl AsRef<Path> for TestDir {
        fn as_ref(&self) -> &Path {
            self.path.as_ref()
        }
    }


    #[test]
    fn integration() {
        env_logger::builder().is_test(true).try_init().unwrap();
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
        thread::sleep(Duration::from_secs(2));

        for (expected_i, cmd) in vec!["foo", "bar"].iter().enumerate() {
            let mut leader = None;
            let mut leader_vec = vec![];
            for (host, mut cli) in cli_vec.clone() {
                match cli.append_cmd(cmd.to_string()) {
                    AppendResp::Appended(i) => {
                        leader = Some(host);
                        debug_assert_eq!(i, expected_i as u64)
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
            for receiver in &recv_vec {
                let resp = receiver.recv().unwrap();
                debug_assert_eq!(resp.cmd, cmd.to_owned());
                debug_assert_eq!(resp.i, expected_i as u64);
            }
        }
    }
}
