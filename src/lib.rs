extern crate leveldb;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate futures;

use std::io::prelude::*;
use std::{str, io};
use std::time::{Duration, SystemTime};

use core::fmt;

use rand::Rng;
use serde::{Serialize, Deserialize};
use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, Error, LineWriter};
use std::thread;
use std::sync::{Arc, Mutex, mpsc, MutexGuard};
use std::sync::mpsc::{RecvTimeoutError, RecvError};
use std::any::Any;
use serde::de::DeserializeOwned;
use futures::channel::oneshot;
use std::cmp::max;
use std::path::Path;
use std::fs::File;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Follower,
    Leader,
    Candidate,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
struct JsonRaftConfig {
    hosts: Vec<String>,
    connection_number: Option<u32>,
    timeout_min_ms: Option<u64>,
    // Can have separate timeouts for heartbeats but meh
    timeout_max_ms: Option<u64>,
    heartbeat_interval: Option<u64>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftConfig {
    // db_path: &'static str,
    host: String,
    hosts: Vec<String>,
    connection_number: Option<u32>,
    timeout_min_ms: u64,
    // Can have separate timeouts for heartbeats but meh
    timeout_max_ms: u64,
    heartbeat_interval: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NodeId(pub String);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term(pub u32);

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}


#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum Voted {
    Yes,
    No,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
enum AppendedLogEntry {
    Succeeded,
    Failed,
}

struct LogEntry<T> {
    command: T,
    term: Term,
}

enum Commited {
    Commited,
    NotLeader(NodeId)
}

struct Store<T> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<T>>,
}

// Remove node_id here, it doesn't need to be in the mutex
pub struct Node<T> {
    store: Store<T>,
    state: State,
    node_id: NodeId,
    leader_id: Option<NodeId>,
    commit_index: usize,
    last_applied: usize,
    last_log_term: Term,
    last_log_index: usize,
}

type UnlockedNode<T> = Arc<Mutex<Node<T>>>;

struct LeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RequestVoteReq {
    pub node_id: NodeId,
    // aka candidate_id
    pub term: Term,
    pub last_log_index: usize,
    pub last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: Voted,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AppendEntryReq<T> {
    pub node_id: NodeId,
    // aka leader_id
    pub term: Term,
    pub prev_log_index: usize,
    pub prev_log_term: Term,
    entries: Vec<T>,
    pub leader_commit: usize,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AppendEntryResp {
    pub term: Term,
    success: AppendedLogEntry,
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum RPCReq<T> {
    AE(AppendEntryReq<T>),
    RV(RequestVoteReq),
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum RPCResp {
    AE(AppendEntryResp),
    RV(RequestVoteResp),

}

#[derive(Debug)]
pub enum RpcError {
    StreamError,
    DeserializationError,
    TimeoutError
}

impl ToString for RpcError {
    fn to_string(&self) -> String {
        match self {
            RpcError::StreamError => "StreamError".to_string(),
            RpcError::DeserializationError => "DeserializationError".to_string(),
            RpcError::TimeoutError => "TimeoutError".to_string()
        }
    }
}

impl From<io::Error> for RpcError {
    fn from(_: io::Error) -> Self {
        RpcError::StreamError
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(_: serde_json::Error) -> Self {
        RpcError::DeserializationError
    }
}

impl RaftConfig {

    pub fn mk_config(host: &str, hosts: Vec<String>, conn_num: Option<u32>) -> RaftConfig {
        RaftConfig {
            host: host.to_string(),
            hosts,
            connection_number: conn_num,
            timeout_min_ms: 150,
            timeout_max_ms: 300,
            heartbeat_interval: 20
        }
    }

    pub fn new(config_path: &str, host: &str) -> serde_json::Result<Self> {
        let path = Path::new(&config_path);
        let display = path.display();
        // Open the path in read-only mode, returns `io::Result<File>`
        let mut file = match File::open(&path) {
            Err(why) => panic!("couldn't open {}: {}", display, why),
            Ok(file) => file,
        };

        // Read the file contents into a string, returns `io::Result<usize>`
        let mut s = String::new();
        match file.read_to_string(&mut s) {
            Err(why) => panic!("couldn't read {}: {}", display, why),
            Ok(_) => print!("{} contains:\n{}", display, s),
        }

        let config: JsonRaftConfig = serde_json::from_str(&s)?;
        Ok(RaftConfig{
            host: host.to_string(),
            hosts: config.hosts,
            connection_number: config.connection_number,
            timeout_min_ms: config.timeout_min_ms.unwrap_or(150),
            timeout_max_ms: config.timeout_max_ms.unwrap_or(300),
            heartbeat_interval: config.heartbeat_interval.unwrap_or(20)
        })
    }

    fn get_timeout(&self) -> Duration {
        let range = self.timeout_min_ms..self.timeout_max_ms;
        let num = rand::thread_rng().gen_range(range);
        Duration::from_millis(num)
    }
}

impl<T: Serialize + DeserializeOwned + Send> Node<T>{
    pub fn new(host: String) -> Self {
        Node {
            state: State::Follower,
            leader_id: None,
            store: Store {
                current_term: Term(1),
                voted_for: None,
                log: vec![],

            },
            node_id: NodeId(host.to_string()),
            commit_index: 0,
            last_applied: 0,
            last_log_term: Term(0),
            last_log_index: 0,
        }
    }


}

fn recv_rpc<T>(timeout: Duration, state: State, rpc_reciver: &mpsc::Receiver<(oneshot::Sender<RPCResp>, RPCReq<T>)>) -> Result<(oneshot::Sender<RPCResp>, RPCReq<T>), RecvTimeoutError> {
    match state {
        State::Follower => rpc_reciver.recv_timeout(timeout),
        _ => rpc_reciver.recv().map_err(|_| RecvTimeoutError::Disconnected),
    }
}

pub fn write_line<T: Serialize>(stream: &TcpStream, data: T) -> std::io::Result<()> {
    let str = serde_json::to_string(&data).unwrap();
    let mut stream = LineWriter::new(stream);
    stream.write_all(str.as_bytes())?;
    stream.write_all(b"\n")
}

fn write_string_line(stream: &TcpStream, data: &String) -> std::io::Result<()> {
    let mut stream = LineWriter::new(stream);
    stream.write_all(data.as_bytes())?;
    stream.write_all(b"\n")
}

pub fn read_rpc<T: DeserializeOwned>(timeout: u128, stream: &TcpStream) -> Result<T, RpcError> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let now = SystemTime::now();
    while let Err(_) = reader.read_line(&mut line) {
        if now.elapsed().unwrap().as_millis() > timeout { // arbitrary 20 ms
            return Err(RpcError::TimeoutError);
        }
    }
    serde_json::from_str(&line).map_err(|_| RpcError::DeserializationError)
}

fn initiate_election<T: 'static + Serialize + Send>(config: RaftConfig, node: UnlockedNode<T>) {

    {
        let mut node = node.lock().unwrap();
        println!("{} Timed out. Becoming candidate and starting election", node.node_id);
        node.store.voted_for = Some(node.node_id.clone());
        node.store.current_term = Term(node.store.current_term.0 + 1);
        node.state = State::Candidate;
    }
    request_votes(config, node)
}

type RPCAgg = Result<Result<RPCResp, RpcError>, Box<dyn Any + Send>>;

fn fold_req_vote_responses(acc: (i32, Term), res: RPCAgg) -> (i32, Term) {
    let (votes, term) = acc;
    res.map_or(acc, |inner_res| inner_res.map_or(acc, |resp| match resp {
        RPCResp::AE(ae) => {
            let term = max(ae.term, term);
            println!("Got append entry, wrong response type");
            (votes, term)
        },
        RPCResp::RV(rv) => {
            let term = max(rv.term, term);
            match rv.vote_granted {
                Voted::Yes => (votes + 1, term),
                Voted::No => (votes, term)
            }
        }
    }))
}

fn fold_heartbeart_responses(acc: Term, res: RPCAgg) -> Term {
    let term = res.map_or(
        Term(0),
        |x| x.map_or(Term(0), |x| match x {
            RPCResp::AE(ae) => ae.term,
            RPCResp::RV(rv) => rv.term
    }));
    max(acc, term)
}

fn become_leader<T: Serialize + Send + 'static>(config: RaftConfig, node: UnlockedNode<T>) {
    {
        let mut node = node.lock().unwrap();
        node.state = State::Leader;
        node.leader_id = Some(node.node_id.clone());
        println!("{} became leader", node.node_id);
    }
    thread::spawn(move || send_heartbeats(config, node));
}

fn send_heartbeat<T: Serialize>(config: &RaftConfig, node: UnlockedNode<T>) -> State {
    let heartbeat = make_heartbeat(node.clone());
    let term = send_reqs_and_agg_resps(
        config.clone(),
        RPCReq::AE(heartbeat), Term(0),
        &fold_heartbeart_responses
    );
    let mut node = node.lock().unwrap();
    if term > node.store.current_term {
        node.state = State::Follower
    }
    return node.state
}

// This blindly send heartbeats. It should only send heartbeats
// when no legitimate AppendEntry requests are being sent to followers.
// If this function exits the node state must be State::Follower
fn send_heartbeats<T: Serialize>(config: RaftConfig, node: UnlockedNode<T>) {
    let mut state = get_state(node.clone());
    while state == State::Leader {
        state = send_heartbeat(&config, node.clone());
        thread::sleep(Duration::from_millis(config.heartbeat_interval));
    };
}

fn make_heartbeat<T>(node: UnlockedNode<T>) -> AppendEntryReq<T> {
    let node = node.lock().unwrap();
    AppendEntryReq {
        node_id: node.node_id.clone(),
        term: node.store.current_term,
        prev_log_index: node.last_log_index,
        prev_log_term: node.last_log_term,
        entries: vec![],
        leader_commit: node.commit_index
    }
}

fn send_reqs_and_agg_resps<T: Serialize, B>(config: RaftConfig, rpc: RPCReq<T>, init: B, fold_func: &dyn Fn(B, RPCAgg) -> B) -> B {
    let ser_req = serde_json::to_string(&rpc).unwrap();
    let timeout = config.heartbeat_interval as u128;
    let host = config.host.clone();
    config.hosts.into_iter()
        .filter(|h| h.to_string() != host)
        .map(|remote_host| {
            let ser_req = ser_req.clone();
            thread::spawn(move || send_and_receive_rpc(timeout, remote_host, &ser_req)).join()
        })
        .fold(init, fold_func)
}

fn request_votes<T: Serialize + Send + 'static>(config: RaftConfig, node: UnlockedNode<T>) {
    let now = SystemTime::now();
    let timeout = config.get_timeout();
    let req_vote = make_request_vote::<T>(node.clone());
    let this_term = req_vote.term;
    let (votes, term) = send_reqs_and_agg_resps::<T, (i32, Term)>(
        config.clone(),
        RPCReq::RV(req_vote),
        (1, this_term), // Starts at 1 because the node votes for itself.
        &fold_req_vote_responses
    );
    {
        let mut node = node.lock().unwrap();
        if term > node.store.current_term {
            node.state = State::Follower;
            node.store.current_term = term;
        }
        if node.state == State::Follower {
            println!("{} failed election becoming follower, ", node.node_id.clone());
            return
        }
    }
    let half = config.hosts.clone().len() / 2;
    if votes > half as i32 {
        become_leader(config.clone(), node);
        return
    }
    while now.elapsed().unwrap() < timeout {
        thread::sleep(Duration::from_millis(5))
    }
    let state = get_state(node.clone());
    if state == State::Candidate {
        initiate_election(config.clone(), node);
    }
}

fn make_request_vote<T>(node: UnlockedNode<T>) -> RequestVoteReq {
    let node = node.lock().unwrap();
    RequestVoteReq {
        node_id: node.node_id.clone(),
        term: node.store.current_term,
        last_log_index: node.last_log_index,
        last_log_term: node.last_log_term,
    }
}

fn get_state<T>(node: UnlockedNode<T>) -> State {
    node.lock().unwrap().state
}

pub fn srv<T: Send + 'static + Serialize>(config: RaftConfig, node: UnlockedNode<T>, rpc_receiver: &mpsc::Receiver<(oneshot::Sender<RPCResp>, RPCReq<T>)>) {
    loop {
        let state = get_state(node.clone());
        let node = node.clone();
        match recv_rpc(config.get_timeout(), state, rpc_receiver) {
            Ok((rpc_sender, rpc_req)) => {
                let rpc_resp = match rpc_req {
                    RPCReq::RV(rv) => srv_request_vote(node, &rv),
                    RPCReq::AE(ae) => srv_append_entry(node, &ae),
                };
                rpc_sender.send(rpc_resp).unwrap();
            }
            Err(RecvTimeoutError::Timeout) => {
                let config = config.clone();
                thread::spawn(move || initiate_election(config, node));
            }
            Err(RecvTimeoutError::Disconnected) => {
                println!("{:?}", RecvTimeoutError::Disconnected);
                break;
            }
        }
    }
}


fn srv_request_vote<T>(node: UnlockedNode<T>, req_vote_req: &RequestVoteReq) -> RPCResp {
    let mut node = node.lock().unwrap();

    if req_vote_req.term > node.store.current_term {
        node.store.current_term = req_vote_req.term;
        node.state = State::Follower;
    }

    if req_vote_req.term == node.store.current_term &&
        node.state == State::Follower &&
        (node.store.voted_for.is_none() || node.store.voted_for == Some(req_vote_req.node_id.clone())) &&
        req_vote_req.last_log_term >= node.last_log_term &&
        req_vote_req.last_log_index >= node.last_log_index
    {
        node.store.voted_for = Some(req_vote_req.node_id.clone());
        RPCResp::RV(RequestVoteResp {
            term: node.store.current_term,
            vote_granted: Voted::Yes,
        })
    } else {
        RPCResp::RV(RequestVoteResp {
            term: node.store.current_term,
            vote_granted: Voted::No,
        })
    }
}

fn srv_append_entry<T>(node: UnlockedNode<T>, append_entry_req: &AppendEntryReq<T>) -> RPCResp {
    let mut node = node.lock().unwrap();
    if append_entry_req.term < node.store.current_term {
        return RPCResp::AE(AppendEntryResp {
            term: node.store.current_term,
            success: AppendedLogEntry::Failed,
        });
    }
    if append_entry_req.term > node.store.current_term {
        node.state = State::Follower;
        node.store.current_term = append_entry_req.term;
    }
    if append_entry_req.term == node.store.current_term {
        if node.state == State::Candidate {
            node.state = State::Follower;
        }
    }
    node.leader_id = Some(append_entry_req.node_id.clone());
    RPCResp::AE(AppendEntryResp {
        term: node.store.current_term,
        success: AppendedLogEntry::Succeeded,
    })
}

fn send_and_receive_rpc(timeout: u128, host: String, ser_req: &String) -> Result<RPCResp, RpcError> {
    let stream = TcpStream::connect(host)?;
    write_string_line(&stream, ser_req)?;
    read_rpc(timeout, &stream)
}


fn run_listener<T: DeserializeOwned + Send + 'static>(config: RaftConfig, rpc_sender: mpsc::Sender<(oneshot::Sender<RPCResp>, RPCReq<T>)>) {
    let listener = TcpListener::bind(config.host).unwrap();
    let timeout = config.heartbeat_interval as u128;
    match config.connection_number {
        None => {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let rpc_sender = rpc_sender.clone();
                thread::spawn(move || handle_connection(timeout, stream, rpc_sender));
            }
        }
        Some(mut conn_num) => {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let rpc_sender = rpc_sender.clone();
                thread::spawn(move || handle_connection(timeout, stream, rpc_sender));

                conn_num = conn_num - 1;
                if conn_num == 0 {
                    break
                }
            }
        }
    }
}

fn handle_connection<T: DeserializeOwned>(timeout: u128, stream: TcpStream, rpc_sender: mpsc::Sender<(oneshot::Sender<RPCResp>, RPCReq<T>)> ) {
    let maybe_rpc_req = read_rpc(timeout, &stream);
    match maybe_rpc_req {
        Ok(rpc_req) => {
            let (sender, receiver) = oneshot::channel::<RPCResp>();
            rpc_sender.send((sender, rpc_req)).unwrap();
            futures::executor::block_on(async {
                match receiver.await {
                    Ok(rpc) => write_line(&stream, rpc).unwrap(),
                    Err(e) => write_string_line(&stream, &e.to_string()).unwrap(),
                }
            });
        },
        Err(e) => write_string_line(&stream, &e.to_string()).unwrap()
    }
}

pub fn run<T: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig) {
    let (rpc_sender, rpc_receiver) = mpsc::channel();
    let web_config = config.clone();
    let listener_handle = thread::spawn(move || run_listener(web_config, rpc_sender));

    let (cli_sender, raft_receiver) = mpsc::channel();
    let (raft_sender, cli_receiver) = mpsc::channel();

    let follower: Node<T> = Node::new(config.host.clone());
    let follower = Arc::new(Mutex::new(follower));
    srv(config.clone(), follower, &rpc_receiver)
    (cli_sender, cli_receiver)

}
