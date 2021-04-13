extern crate commitlog;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate futures;

use std::io::prelude::*;
use std::{str, io};
use std::time::{Duration, SystemTime};

use core::fmt;

use rand::Rng;
use commitlog::{message::*, *};
use serde::{Serialize, Deserialize};
use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, LineWriter};
use std::thread;
use std::sync::{Arc, Mutex, mpsc, MutexGuard};
use std::sync::mpsc::{RecvTimeoutError, Sender, Receiver, RecvError};
use std::any::Any;
use serde::de::{DeserializeOwned, Error};
use futures::channel::oneshot;
use std::cmp::{max, Ordering, min};
use std::path::Path;
use std::fs::File;
use crate::LogReadError::{SerdeError, LogError, IOError};
use std::iter::Filter;
use std::vec::IntoIter;

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Follower,
    Leader {
        next_indices: Vec<(String, u64)>,
        match_indices: Vec<(String, u64)>,
    },
    Candidate,
}

const LEADER: State = State::Leader {
    next_indices: vec![],
    match_indices: vec![],
};

const METADATA_PATH: &str = "/metadata.json";
const LOG_PATH: &str = "/log";

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
    host: String,
    hosts: Vec<String>,
    connection_number: Option<u32>,
    timeout_min_ms: u64,
    // Can have separate timeouts for heartbeats but meh
    timeout_max_ms: u64,
    heartbeat_interval: u64,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term(pub u64);

impl Term {
    fn inc_term(&self) -> Term {
        Term(self.0 + 1)
    }
}

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

#[derive(Serialize, Deserialize)]
struct LogEntry<T> {
    i: u64,
    cmd: T,
    term: Term,
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Metadata {
    current_term: Term,
    voted_for: Option<String>,
}

pub struct Node {
    log: CommitLog,
    data_dir: String,
    metadata: Metadata,
    state: State,
    leader_id: Option<String>,
    commit_index: Option<u64>,
    last_applied: Option<u64>,
    last_log: Option<(u64, Term)>,
}


struct LeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RequestVoteReq {
    pub node_id: String,
    // aka candidate_id
    pub term: Term,
    pub last_log: Option<(u64, Term)>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: Voted,
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntryReq<T> {
    pub node_id: String,
    // aka leader_id
    pub term: Term,
    // The metadata of the log preceding the entries sent.
    pub prev_log: Option<(u64, Term)>,
    entries: Vec<LogEntry<T>>,
    pub leader_commit: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AppendEntryResp {
    pub term: Term,
    success: AppendedLogEntry,
}

pub enum CommitResp {
    Commited,
    NotLeader(Option<String>),
    Error,
}

#[derive(Serialize, Deserialize)]
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
    StreamError(io::Error),
    DeserializationError(serde_json::Error),
    TimeoutError,
}

#[derive(Debug)]
enum LogReadError {
    SerdeError(serde_json::Error),
    LogError(commitlog::ReadError),
    IOError(io::Error),
}

pub enum Committed<T> {
    CMD(T),
    NotLeader(Option<String>),
}

impl From<serde_json::Error> for LogReadError {
    fn from(e: serde_json::Error) -> Self {
        SerdeError(e)
    }
}

impl From<commitlog::ReadError> for LogReadError {
    fn from(e: commitlog::ReadError) -> Self {
        LogError(e)
    }
}

impl From<io::Error> for LogReadError {
    fn from(e: io::Error) -> Self {
        IOError(e)
    }
}

impl ToString for RpcError {
    fn to_string(&self) -> String {
        match self {
            RpcError::StreamError(e) => e.to_string(),
            RpcError::DeserializationError(e) => e.to_string(),
            RpcError::TimeoutError => "TimeoutError".to_owned()
        }
    }
}

impl From<io::Error> for RpcError {
    fn from(e: io::Error) -> Self {
        RpcError::StreamError(e)
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(e: serde_json::Error) -> Self {
        RpcError::DeserializationError(e)
    }
}

impl RaftConfig {
    pub fn mk_config(host: &str, hosts: Vec<String>, conn_num: Option<u32>) -> RaftConfig {
        RaftConfig {
            data_dir: "data".to_owned(),
            host: host.to_owned(),
            hosts,
            connection_number: conn_num,
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
            connection_number: config.connection_number,
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

fn read_metadata(store_path: &str) -> Metadata {
    let s = store_path.to_owned() + METADATA_PATH;
    let path = Path::new(&s);
    match File::open(path) {
        Ok(file) => serde_json::from_reader(file).unwrap(),
        Err(e) => {
            let display = path.display();
            println!("Error opening {}: {}", display, e);
            Metadata {
                current_term: Term(1),
                voted_for: None,
            }
        }
    }
}

type Previous = Option<(u64, Term)>;

impl Node {
    pub fn new<T: Serialize + DeserializeOwned + Send>(data_dir: &str) -> Self {
        let metadata = read_metadata(data_dir);
        let opts = LogOptions::new(data_dir.to_owned() + LOG_PATH);
        let log = CommitLog::new(opts).unwrap();

        let mut node = Node {
            state: State::Follower,
            data_dir: data_dir.to_owned(),
            leader_id: None,
            metadata,
            commit_index: None,
            last_applied: None,
            log,
            last_log: None,
        };
        match node.read_last::<T>() {
            Ok((i, entry)) => {
                node.last_log = Some((i, entry.term));
                node
            }
            Err(_) => node
        }
    }

    fn append<T: Serialize + DeserializeOwned + Send>(&mut self, entry: &LogEntry<T>) -> Result<u64, AppendError> {
        let str = serde_json::to_string(entry).unwrap();
        let offset = self.log.append_msg(&str)?;
        assert_eq!(offset, entry.i);
        self.last_log = Some((offset, entry.term));
        Ok(offset)
    }

    fn append_cmd<T: Serialize + DeserializeOwned + Send>(&mut self, cmd: T) -> Result<u64, AppendError> {
        let i = self.last_log.map_or(0, |(i, _)| i + 1);
        let entry = LogEntry {
            i,
            cmd,
            term: self.metadata.current_term,
        };
        self.append(&entry)
    }

    fn read_from<T: Serialize + DeserializeOwned + Send>(&self, i: u64) -> Result<Vec<LogEntry<T>>, LogReadError> {
        let msgs = self.log.read(i, ReadLimit::default())?;
        msgs.iter().map(|msg| {
            let s = String::from_utf8_lossy(msg.payload());
            serde_json::from_str(s.as_ref())
        }).collect::<Result<Vec<LogEntry<T>>, serde_json::Error>>()
            .map_err(|e| LogReadError::SerdeError(e))
    }

    fn read_from_with_prev<T: Serialize + DeserializeOwned + Send>(&self, i: u64) -> Result<(Previous, Vec<LogEntry<T>>), LogReadError> {
        if i == 0 {
            // There are no previous logs to the ones being appended
            return self.read_from(i).map(|logs| (None, logs));
        }
        let prev_i = i - 1;
        let mut log_entries = self.read_from(prev_i)?;
        let prev_log_entry = log_entries.pop().unwrap();
        Ok((Some((prev_i, prev_log_entry.term)), log_entries))
    }

    fn write_metadata(&self) {
        let s = self.data_dir.clone() + METADATA_PATH;
        let path = Path::new(&s);
        let file = File::create(path).unwrap();
        serde_json::to_writer(file, &self.metadata).unwrap();
    }

    fn get_term(&self) -> Term {
        self.metadata.current_term
    }

    fn set_term(&mut self, term: Term) {
        self.metadata.current_term = term;
        self.write_metadata();
    }

    fn get_voted_for(&self) -> Option<String> {
        self.metadata.voted_for.clone()
    }

    fn set_voted_for(&mut self, host: &str) {
        self.metadata.voted_for = Some(host.to_owned());
        self.write_metadata();
    }

    fn read_last<T: DeserializeOwned>(&self) -> Result<(u64, LogEntry<T>), LogReadError> {
        let last_offset = self.log.last_offset();
        match last_offset {
            None => Err(LogError(ReadError::NoSuchSegment)),
            Some(i) => self.read_one(i).map(|x| (i, x))
        }
    }

    fn read_one<T: DeserializeOwned>(&self, i: u64) -> Result<LogEntry<T>, LogReadError> {
        let msgs = self.log.read(i, ReadLimit::default())?;
        let msg = msgs.iter().next().unwrap(); // Assume data because last line succeeded
        let s = String::from_utf8_lossy(msg.payload());
        serde_json::from_str(s.as_ref()).map_err(|e| LogReadError::SerdeError(e))
    }

    /// 5.4.1
    /// Raft determines which of two logs is more up-to-date by comparing the index and
    /// term of the last entries in the logs. If the logs have last entries with different terms,
    /// then the log with the later term is more up-to-date. If the logs end with the same term,
    /// then whichever log is longer is more up-to-date
    fn is_more_up_to_date(&self, cand_log: Previous) -> bool {
        match (self.last_log, cand_log) {
            (None, None) => false,
            (None, _) => false,
            (_, None) => true,
            (Some((i, t)), Some((cand_i, cand_t))) => {
                match t.cmp(&cand_t) {
                    Ordering::Less => false,
                    Ordering::Greater => true,
                    Ordering::Equal => i > cand_i
                }
            }
        }
    }

    // TODO: Optimize this to use read_from. Will be much faster.
    fn verify_entries<T: DeserializeOwned>(&mut self, logs: &Vec<LogEntry<T>>) {
        for log in logs {
            match self.read_one::<T>(log.i) {
                Ok(entry) => {
                    if entry.term != log.term {
                        self.log.truncate(log.i);
                        break;
                    }
                }
                Err(_) => ()
            }
        }
    }
}

fn cmp_discrim<T>(t1: &T, t2: &T) -> bool {
    std::mem::discriminant(t1) == std::mem::discriminant(t2)
}


fn recv_rpc<T>(timeout: Duration, is_follower: bool, rpc_reciver: &mpsc::Receiver<(oneshot::Sender<RPCResp>, RPCReq<T>)>) -> Result<(oneshot::Sender<RPCResp>, RPCReq<T>), RecvTimeoutError> {
    match is_follower {
        true => rpc_reciver.recv_timeout(timeout),
        false => rpc_reciver.recv().map_err(|_| RecvTimeoutError::Disconnected),
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
    serde_json::from_str(&line).map_err(|e| RpcError::DeserializationError(e))
}

fn initiate_election<T: 'static + Serialize + DeserializeOwned + Send>(config: RaftConfig, node: Arc<Mutex<Node>>) {
    {
        let mut node = node.lock().unwrap();
        println!("{} Timed out. Becoming candidate and starting election", config.host);
        node.set_voted_for(&config.host);
        let new_term = node.metadata.current_term.inc_term();
        node.set_term(new_term);
        node.state = State::Candidate;
    }
    request_votes::<T>(config, node)
}

type RPCAgg = Result<Result<RPCResp, RpcError>, Box<dyn Any + Send>>;

fn fold_req_vote_responses(acc: (i32, Term), res: RPCAgg) -> (i32, Term) {
    let (votes, term) = acc;
    let res = res.unwrap();
    match res {
        Ok(resp ) => match resp {
            RPCResp::RV(rv) => {
                let term = max(rv.term, term);
                match rv.vote_granted {
                    Voted::Yes => (votes + 1, term),
                    Voted::No => (votes, term)
                }
            }
            _ => {
                println!("Received wrong response type");
                acc
            }
        }
        Err(e) => {
            println!("{:?}", e);
            acc
        }
    }
}

fn get_state(node: Arc<Mutex<Node>>) -> State {
    node.lock().unwrap().state.clone()
}

fn make_remote_vec<B>(host: &str, hosts: &Vec<String>, f: Box<dyn Fn(String) -> B>) -> Vec<B> {
    hosts.clone().into_iter().filter(|h| h != host).map(f).collect()
}

fn become_leader<T: Serialize + Send + DeserializeOwned + 'static>(config: RaftConfig, node: Arc<Mutex<Node>>) {
    {
        let mut node = node.lock().unwrap();
        let next_index = node.log.last_offset().map_or(0, |i| i + 1);
        node.state = State::Leader {
            next_indices: make_remote_vec(&config.host, &config.hosts, Box::new(move |h: String| (h, next_index))),
            match_indices: make_remote_vec(&config.host, &config.hosts, Box::new(|h| (h, 0))),
        };

        node.leader_id = Some(config.host.clone());
        println!("{} became leader", config.host);
    }
    let timeout = config.heartbeat_interval;
    let mut state = get_state(node.clone());
    while let State::Leader { next_indices, match_indices: _ } = state {
        println!("Sending heartbeats...");
        for (remote, next_index) in next_indices {
            let host = config.host.clone();
            let node = node.clone();
            thread::spawn(move || send_append_entry::<T>(
                timeout as u128,
                host,
                remote,
                next_index,
                node.clone())
            );
        }
        thread::sleep(Duration::from_millis(timeout));
        state = get_state(node.clone());
    };
}


fn send_append_entry<T: Send + Serialize + DeserializeOwned>(timeout: u128, host: String, remote: String, next_index: u64, node: Arc<Mutex<Node>>) {
    let last_index = node.lock().unwrap().last_log.map(|(i, _)| i);
    let ae = if last_index >= Some(next_index) {
        println!("Entries to append to {}", &remote);
        let (prev_log_entry, logs) = node.lock().unwrap().read_from_with_prev::<T>(next_index).unwrap();
        make_heartbeat(host.clone(), node.clone(), prev_log_entry, logs)
    } else {
        println!("No entries to append to {}", &remote);
        let prev_log = node.lock().unwrap().last_log;
        make_heartbeat(host.clone(), node.clone(), prev_log, vec![])
    };
    let len = ae.entries.len();
    let ser_req = serde_json::to_string(&RPCReq::AE(ae)).unwrap();
    let maybe_rpc = send_and_receive_rpc(timeout, remote.clone(), &ser_req);
    process_append_entry::<T>(remote, maybe_rpc, node, len as u64)
}

fn make_heartbeat<T>(host: String, node: Arc<Mutex<Node>>, prev_log: Option<(u64, Term)>, entries: Vec<LogEntry<T>>) -> AppendEntryReq<T> {
    let node = node.lock().unwrap();
    AppendEntryReq {
        node_id: host,
        term: node.metadata.current_term,
        prev_log,
        entries,
        leader_commit: node.commit_index,
    }
}

fn process_append_entry<T: DeserializeOwned>(remote_host: String, maybe_rpc: Result<RPCResp, RpcError>, node: Arc<Mutex<Node>>, entries_len: u64) {
    match maybe_rpc {
        Ok(rpc) => match rpc {
            RPCResp::AE(ae_resp) => {
                {
                    println!("Received heartbeat resp");
                    let mut node = node.lock().unwrap();
                    if ae_resp.term > node.metadata.current_term {
                        node.set_term(ae_resp.term);
                        node.state == State::Follower;
                        let leader = Some(remote_host.clone());
                        node.leader_id = leader.clone();
                    }
                    // If it's not greater it must be equal as follower
                    // should have increased term to match leader... I think
                    assert_eq!(ae_resp.term, node.metadata.current_term);
                }
                match get_state(node.clone()) {
                    State::Leader {next_indices, match_indices } => {
                        process_append_entry_response::<T>(remote_host.clone(), ae_resp, node.clone(), next_indices, match_indices, entries_len);
                    }
                    _ => println!("Not leader")
                };
            }
            RPCResp::RV(_) => {}
        },
        Err(e) => {
            println!("{:?}", e);
        }
    }
}

fn process_append_entry_response<T: DeserializeOwned>(remote: String, ae_resp: AppendEntryResp, node: Arc<Mutex<Node>>, mut next_indices: Vec<(String, u64)>, mut match_indices: Vec<(String, u64)>, entries_len: u64) {
    let mut node = node.lock().unwrap();
    match ae_resp.success {
        AppendedLogEntry::Succeeded => {
            let index = next_indices.clone().into_iter().position(|(h, _)| h == remote).unwrap();
            let (_, next_i) = &mut next_indices[index];
            *next_i = *next_i + entries_len;
            let index = match_indices.clone().into_iter().position(|(h, _)| h == remote).unwrap();
            let (_, match_i) = &mut match_indices[index];
            *match_i = *next_i - 1;
            for i in node.commit_index.unwrap() + 1..node.log.last_offset().unwrap() {
                let entry = node.read_one::<T>(i).unwrap();
                if entry.term == node.metadata.current_term {
                    let mut matches = 1; // Count yourself
                    for (_, match_i) in &match_indices {
                        if match_i >= &i {
                            matches += 1;
                        }
                    }
                    if matches > match_indices.len() / 2 {
                        node.commit_index = Some(i);
                    }
                }
            }
        }
        AppendedLogEntry::Failed => {
            let index = next_indices.clone().into_iter().position(|(h, _)| h == remote).unwrap();
            let (_, i) = &mut next_indices[index];
            *i = *i - 1;
        }
    }
}

fn request_votes<T: Serialize + Send + DeserializeOwned + 'static>(config: RaftConfig, node: Arc<Mutex<Node>>) {
    let now = SystemTime::now();
    let random_timeout = config.get_timeout();
    let req_vote = make_request_vote(config.host.clone(), node.clone());
    let this_term = req_vote.term;
    let rpc_req = RPCReq::RV::<T>(req_vote);
    let ser_req = serde_json::to_string(&rpc_req).unwrap();
    let timeout = config.heartbeat_interval as u128;
    let host = config.host.clone();
    let (votes, term) = config.hosts.clone().into_iter()
        .filter(|h| h.to_owned() != host)
        .map(|remote_host| {
            let ser_req = ser_req.clone();
            thread::spawn(move || send_and_receive_rpc(timeout, remote_host, &ser_req)).join()
        })
        .fold((1, this_term), fold_req_vote_responses);
    {
        let mut node = node.lock().unwrap();
        if term > node.metadata.current_term {
            node.state = State::Follower;
            node.set_term(term);
        }
        if node.state == State::Follower {
            println!("{} failed election becoming follower, ", config.host.clone());
            return;
        }
    }
    let half = config.hosts.clone().len() / 2;
    println!("Votes are {}, half is {}", votes, half);
    if votes > half as i32 {
        become_leader::<T>(config.clone(), node);
        return;
    }
    while now.elapsed().unwrap() < random_timeout {
        thread::sleep(Duration::from_millis(5))
    }
    if eq_state(node.clone(), State::Candidate) {
        initiate_election::<T>(config.clone(), node);
    }
}

fn make_request_vote(host: String, node: Arc<Mutex<Node>>) -> RequestVoteReq {
    let node = node.lock().unwrap();
    RequestVoteReq {
        node_id: host,
        term: node.metadata.current_term,
        last_log: node.last_log,
    }
}

fn eq_state(node: Arc<Mutex<Node>>, other: State) -> bool {
    let state = std::mem::discriminant(&node.lock().unwrap().state);
    state == std::mem::discriminant(&other)
}

pub fn srv<T: Send + DeserializeOwned + 'static + Serialize>(config: RaftConfig, node: Arc<Mutex<Node>>, rpc_receiver: &mpsc::Receiver<(oneshot::Sender<RPCResp>, RPCReq<T>)>) {
    loop {
        let is_follower = eq_state(node.clone(), State::Follower);
        let node = node.clone();
        match recv_rpc(config.get_timeout(), is_follower, rpc_receiver) {
            Ok((rpc_sender, rpc_req)) => {
                let rpc_resp = match rpc_req {
                    RPCReq::RV(rv) => srv_request_vote::<T>(node, &rv),
                    RPCReq::AE(ae) => srv_append_entry(node, &ae),
                };
                rpc_sender.send(rpc_resp).unwrap();
            }
            Err(RecvTimeoutError::Timeout) => {
                let config = config.clone();
                thread::spawn(move || initiate_election::<T>(config, node));
            }
            Err(RecvTimeoutError::Disconnected) => {
                println!("{:?}", RecvTimeoutError::Disconnected);
                break;
            }
        }
    }
}


fn srv_request_vote<T>(node: Arc<Mutex<Node>>, req_vote_req: &RequestVoteReq) -> RPCResp {
    let mut node = node.lock().unwrap();

    if req_vote_req.term > node.metadata.current_term {
        node.set_term(req_vote_req.term);
        node.state = State::Follower;
    }

    if req_vote_req.term == node.metadata.current_term &&
        node.state == State::Follower &&
        (node.metadata.voted_for.is_none() || node.metadata.voted_for == Some(req_vote_req.node_id.clone())) &&
        !node.is_more_up_to_date(req_vote_req.last_log)
    {
        node.set_voted_for(&req_vote_req.node_id);
        RPCResp::RV(RequestVoteResp {
            term: node.metadata.current_term,
            vote_granted: Voted::Yes,
        })
    } else {
        RPCResp::RV(RequestVoteResp {
            term: node.metadata.current_term,
            vote_granted: Voted::No,
        })
    }
}

fn srv_append_entry<T: DeserializeOwned + Serialize + Send>(node: Arc<Mutex<Node>>, ae: &AppendEntryReq<T>) -> RPCResp {
    let mut node = node.lock().unwrap();
    if ae.term < node.metadata.current_term {
        return RPCResp::AE(AppendEntryResp {
            term: node.metadata.current_term,
            success: AppendedLogEntry::Failed,
        });
    }
    if ae.term > node.metadata.current_term {
        node.state = State::Follower;
        node.set_term(ae.term);
    }
    if ae.term == node.metadata.current_term {
        if node.state == State::Candidate {
            node.state = State::Follower;
        }
    }
    node.leader_id = Some(ae.node_id.clone());

    if node.last_log != ae.prev_log {
        return RPCResp::AE(AppendEntryResp {
            term: node.metadata.current_term,
            success: AppendedLogEntry::Failed,
        });
    }

    node.verify_entries(&ae.entries);
    let mut last_index = 0;
    for entry in &ae.entries {
        last_index = node.append(entry).unwrap();
    }

    if ae.leader_commit > node.commit_index {
        node.commit_index = min(ae.leader_commit, Some(last_index))
    }

    RPCResp::AE(AppendEntryResp {
        term: node.metadata.current_term,
        success: AppendedLogEntry::Succeeded,
    })
}

fn send_and_receive_rpc(timeout: u128, host: String, ser_req: &String) -> Result<RPCResp, RpcError> {
    let stream = TcpStream::connect(host)?;
    write_string_line(&stream, ser_req)?;
    read_rpc(timeout, &stream)
}

fn run_raft_listener<T: DeserializeOwned + Send + 'static>(
    config: RaftConfig,
    rpc_sender: mpsc::Sender<(oneshot::Sender<RPCResp>, RPCReq<T>)>,
) {
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
                    break;
                }
            }
        }
    }
}

fn handle_connection<T: DeserializeOwned>(timeout: u128, stream: TcpStream, rpc_sender: mpsc::Sender<(oneshot::Sender<RPCResp>, RPCReq<T>)>) {
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
        }
        Err(e) => write_string_line(&stream, &e.to_string()).unwrap()
    }
}

fn run_client<T>(sender: mpsc::Sender<Committed<T>>, receiver: mpsc::Receiver<T>, config: &RaftConfig, node: Arc<Mutex<Node>>)
where
    T: DeserializeOwned + Serialize + Send,
{
    loop {
        let cmd = match receiver.recv() {
            Ok(cmd) => cmd,
            Err(e) => panic!("{:?}", e)
        };
        let timeout = config.heartbeat_interval as u128;
        match node.lock().unwrap().state.clone() {
            State::Leader { match_indices, next_indices } => {
                {
                    let mut appender = node.lock().unwrap();
                    appender.append_cmd(cmd);
                }
                for (remote, next_index) in next_indices {
                    send_append_entry::<T>(timeout, config.host.clone(), remote, next_index, node.clone());
                }
                let node = node.lock().unwrap();
                if node.last_applied < node.commit_index {
                    for i in node.last_applied.unwrap() + 1..node.commit_index.unwrap() {
                        match node.read_one(i) {
                            Ok(entry) => sender.send(Committed::CMD(entry.cmd)).unwrap(),
                            Err(e) => println!("{:?}", e)
                        }
                    }
                }
            }
            _ => sender.send(Committed::NotLeader(node.lock().unwrap().leader_id.clone())).unwrap()
        };
    }
}

pub fn run<T: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig) -> (mpsc::Sender<T>, mpsc::Receiver<Committed<T>>) {
    let (rpc_sender, rpc_receiver) = mpsc::channel();
    let web_config = config.clone();
    thread::spawn(move || run_raft_listener::<T>(web_config, rpc_sender));
    let (cli_sender, raft_receiver) = mpsc::channel();
    let (raft_sender, cli_receiver) = mpsc::channel();
    let node: Node = Node::new::<T>(&config.data_dir);
    let node = Arc::new(Mutex::new(node));
    srv(config.clone(), node.clone(), &rpc_receiver);
    thread::spawn(move || run_client(raft_sender, raft_receiver, &config, node));
    (cli_sender, cli_receiver)
}
