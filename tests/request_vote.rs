use raftrs;
use raftrs::*;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use serde::Serialize;

fn send_rpc<T: Serialize>(host: &str, req: RPCReq<T>) -> Result<RPCResp, RpcError> {
    thread::sleep(Duration::from_millis(10));
    let mut stream = TcpStream::connect(host);
    while let Err(_) = stream {
        thread::sleep(Duration::from_millis(10));
        stream = TcpStream::connect(host);
    }
    let stream = stream.unwrap();
    write_line(&stream, req);
    read_rpc(20, &stream)
}

#[test]
fn test_follower_replies_no_on_lower_term() {
    let host = "127.0.0.1:3343";
    let t = thread::spawn(move || run::<String>(RaftConfig::mk_config(host, vec![], Some(1))));
    let req: RPCReq<()> = RPCReq::RV(RequestVoteReq {
        node_id: NodeId("node1".to_string()),
        term: Term(0),
        last_log_index: 0,
        last_log_term: Term(0),
    });
    let resp = send_rpc(host, req).unwrap();
    let expected = RPCResp::RV(RequestVoteResp {
        term: Term(1),
        vote_granted: Voted::No,
    });
    t.join();
    assert_eq!(resp, expected)
}

#[test]
fn test_follower_replies_yes_on_equal_term() {
    let host = "127.0.0.1:3344";
    let t = thread::spawn(move || run::<String>(RaftConfig::mk_config(host, vec![], Some(1))));
    let req: RPCReq<()> = RPCReq::RV(RequestVoteReq {
        node_id: NodeId("node1".to_string()),
        term: Term(1),
        last_log_index: 0,
        last_log_term: Term(0),
    });
    let resp = send_rpc(host, req).unwrap();
    let expected = RPCResp::RV(RequestVoteResp {
        term: Term(1),
        vote_granted: Voted::Yes,
    });
    t.join();
    assert_eq!(resp, expected)
}

#[test]
fn test_follower_increments_term() {
    let host = "127.0.0.1:3345";
    let t = thread::spawn(move || run::<String>(RaftConfig::mk_config(host, vec![], Some(1))));
    let req: RPCReq<()> = RPCReq::RV(RequestVoteReq {
        node_id: NodeId("node1".to_string()),
        term: Term(2),
        last_log_index: 0,
        last_log_term: Term(0),
    });
    let resp = send_rpc(host, req).unwrap();
    let expected = RPCResp::RV(RequestVoteResp {
        term: Term(2),
        vote_granted: Voted::Yes,
    });
    t.join();
    assert_eq!(resp, expected)
}

#[test]
fn test_follower_replies_no_to_other_node() {
    let host = "127.0.0.1:3346";
    let t = thread::spawn(move || run::<String>(RaftConfig::mk_config(host, vec![], Some(1))));

    let req: RPCReq<()> = RPCReq::RV(RequestVoteReq {
        node_id: NodeId("node1".to_string()),
        term: Term(1),
        last_log_index: 0,
        last_log_term: Term(0),
    });
    let resp = send_rpc(host, req).unwrap();
    let expected = RPCResp::RV(RequestVoteResp {
        term: Term(1),
        vote_granted: Voted::Yes,
    });
    assert_eq!(resp, expected);
    let req: RPCReq<()> = RPCReq::RV(RequestVoteReq {
        node_id: NodeId("node2".to_string()),
        term: Term(2),
        last_log_index: 0,
        last_log_term: Term(0),
    });
    let resp = send_rpc(host, req).unwrap();

    let expected = RPCResp::RV(RequestVoteResp {
        term: Term(2),
        vote_granted: Voted::No,
    });
    t.join();
    assert_eq!(resp, expected)
}

#[test]
fn test_candidate_request_votes() {
    let hosts = vec![
        "127.0.0.1:3333".to_string(),
        "127.0.0.1:3334".to_string(),
        "127.0.0.1:3335".to_string()
    ];
    for host in hosts.clone() {
        let hosts = hosts.clone();
        thread::Builder::new()
            .name(host.clone())
            .spawn(move || run::<String>(RaftConfig::mk_config(&host, hosts.clone(), None)));
    };
    thread::sleep(Duration::from_secs(10))
}

//
// #[test]
// fn test_follower_replies_no_to_different_node_id() {
//     initialize();
//     let rv = RPCReq::RV(RequestVoteReq {
//         node_id: NodeId("node2".to_string()),
//         term: Term(1),
//         last_log_index: 0,
//         last_log_term: Term(0),
//     });
//     let req_vote_resp: RPCResp = send_and_receive(rv).unwrap();
//     match req_vote_resp {

//     }
// }
//
// #[test]
// fn test_follower_increases_term_on() {
//     initialize();
//     let rv = RPCReq::RV(RequestVoteReq {
//         node_id: NodeId("node2".to_string()),
//         term: Term(3),
//         last_log_index: 0,
//         last_log_term: Term(0),
//     });
//     let req_vote_resp: RPCResp = send_and_receive(rv).unwrap();
//     match req_vote_resp {
//         RPCResp::AE(ae) => {
//             panic!("Received wrong response type!");
//         }
//         RPCResp::RV(rv) => {
//             assert_eq!(rv.term, Term(3));
//             assert_eq!(rv.vote_granted, Voted::No);
//         }
//     }
// }
