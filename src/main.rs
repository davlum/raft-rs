
mod test;

extern crate leveldb;
extern crate rand;
extern crate serde;
extern crate serde_json;

use std::str;
use std::io::prelude::*;
use serde::{Serialize, Deserialize};
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::{Options,WriteOptions,ReadOptions};
use std::path::Path;
use std::sync::{mpsc, Mutex, Arc};
use std::time::{Duration, SystemTime, Instant};
use rand::distributions::{Distribution, Uniform};
use std::marker::PhantomData;
use std::net::TcpListener;
use std::collections::HashMap;
use std::thread::JoinHandle;
use std::io::BufReader;
use crate::RPC::{AE, RV};
use core::fmt;
use std::str::FromStr;
use std::fmt::Error;
use std::{thread, time};
use rand::prelude::*; // 0.8.2
use std::sync::mpsc::SendError;




struct Follower;
struct Leader;
struct Candidate;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NodeId(String);

impl fmt::Display for NodeId {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.0)
  }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Term(u32);

impl Term {
  pub fn inc_term(&mut self) -> Term {
    Term(self.0 + 1)
  }
}

impl fmt::Display for Term {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.0)
  }
}

struct RaftConfig<'a> {
  db_path: &'a str,
  hosts: Vec<&'a str>,
  election_timeout_min_ms: u64,
  election_timeout_max_ms: u64,
  heartbeat_timeout_ms: u64
}

struct Node<'a, State> {
  config: &'a RaftConfig<'a>,
  voted: bool,
  node_id: NodeId,
  elected: Option<NodeId>,
  term: Term,
  _state: PhantomData<State>,
  db: &'a Database<i32>,
  log: &'a mut HashMap<String, String>
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVote {
  node_id: NodeId,
  term: Term
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntry {
  node_id: NodeId,
  term: Term,
  key: String,
  value: String
}

#[derive(Serialize, Deserialize, Debug)]
enum RPC {
  AE(AppendEntry),
  RV(RequestVote)
}

enum IPC {
  RPC(RPC),
  NoReply
}

enum HandleErr {
  Send(SendError<IPC>),
  Time(mpsc::RecvTimeoutError)
}

impl From<SendError<IPC>> for HandleErr {
  fn from(e: SendError<IPC>) -> Self {
    HandleErr::Send(e)
  }
}

impl From<mpsc::RecvTimeoutError> for HandleErr {
  fn from(e: mpsc::RecvTimeoutError) -> Self {
    HandleErr::Time(e)
  }
}


fn serialize_cmd(cmd: &RPC) -> Result<String, serde_json::Error> {
  serde_json::to_string(cmd)
}

fn parse_cmd(str: &str) ->  Result<RPC, serde_json::Error> {
  serde_json::from_str(str)
}


fn create_db(path: &str) -> Database<i32> {
  let path = Path::new(path);

  let mut options = Options::new();
  options.create_if_missing = true;
  let db = match Database::open(path, options) {
    Ok(db) => { db },
    Err(e) => { panic!("failed to open database: {:?}", e) }
  };
  println!("Db created");
  return db
}

impl<'t> Node<'t, Follower> {

  fn new(config: &'t RaftConfig, node_id: &str, db: &'t Database<i32>, log: &'t mut HashMap<String, String>) -> Self {
    return Self{
      config,
      _state: PhantomData,
      voted: false,
      node_id: NodeId(node_id.to_string()),
      elected: None,
      term: Term(1),
      db,
      log
    }
  }

  fn run(&mut self, sender: &mpsc::Sender<IPC>, receiver: &mpsc::Receiver<RPC>) -> Node<Candidate> {
    loop {
      match self.handle_req(&sender, receiver) {
        Ok(_) => println!("Received good request."),
        Err(e) => {
          match e {
            HandleErr::Send(e) => println!("{}", e.to_string()),
            HandleErr::Time(e) => break
          }
        }
      }
    }
    println!("No parseable requests received for {} ms. Timing out.", self.config.heartbeat_timeout_ms);
    return Node{
      config: self.config,
      _state: PhantomData,
      voted: true,
      node_id: self.node_id.clone(),
      elected: None,
      term: self.term.inc_term(),
      db: self.db,
      log: self.log
    }
  }

  fn handle_req(&mut self, sender: &mpsc::Sender<IPC>, receiver: &mpsc::Receiver<RPC>) -> Result<(), HandleErr> {
    let duration = Duration::from_millis(self.config.heartbeat_timeout_ms);
    let cmd = receiver.recv_timeout(duration)?;
    let x = match cmd {
      AE(ae) => self.handle_append_entry(ae, sender),
      RV(rv) => self.handle_request_vote(rv, sender)
    }?;
    return Ok(x)
  }

  fn handle_request_vote(&mut self, rv: RequestVote, sender: &mpsc::Sender<IPC>) -> Result<(), SendError<IPC>> {
    if self.voted {
      // sender.send(IPC::NoReply)
      return Ok(())
    } else {
      self.voted = true;
      sender.send(IPC::RPC(RV(rv)))
    }
  }

  fn handle_append_entry(&mut self, ae: AppendEntry, sender: &mpsc::Sender<IPC>) -> Result<(), SendError<IPC>> {
    if ae.term < self.term {
      println!("Term too low. Won't append");
      sender.send(IPC::NoReply)
    } else {
      self.log.insert(ae.key.clone(), ae.value.clone());
      sender.send(IPC::RPC(AE(ae)))
    }
  }
}

fn get_response(line: String, sender: &mpsc::Sender<RPC>, receiver: &mpsc::Receiver<IPC>) -> Option<String> {
  match parse_cmd(&line[..]) {
    Ok(rpc) => {
      sender.send(rpc);
      match receiver.recv() {
        Ok(cmd) => match cmd {
          IPC::NoReply => None,
          IPC::RPC(rpc) => match serialize_cmd(&rpc) {
            Ok(s) => Some(s),
            Err(e) => Some(e.to_string())
          }
        },
        Err(e) => Some(e.to_string())
      }
    }
    Err(e) => Some(e.to_string())
  }
}

fn run_listener(sender: mpsc::Sender<RPC>, receiver: mpsc::Receiver<IPC>) -> JoinHandle<()> {
  thread::spawn(move ||  {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    for stream in listener.incoming() {
      let mut stream = stream.unwrap();
      let mut reader = BufReader::new(&stream);

      let mut line = String::new();
      reader.read_line(&mut line).unwrap();

      let resp = get_response(line, &sender, &receiver);
      if resp.is_some() {
        stream.write(&resp.unwrap().as_bytes());
        stream.flush().unwrap();
      }
    }
  })
}

fn get_rand_waittime(min: u64, max: u64) -> Duration {
  let mut rng = rand::thread_rng();
  let wait_time = Uniform::from(min..max);
  Duration::from_millis(wait_time.sample(&mut rng))
}


fn run_with_config(config: RaftConfig) {
  let mut log = HashMap::new();
  let db = create_db(config.db_path);
  let (web_sender, node_receiver) = mpsc::channel();
  let (node_sender, web_receiver) = mpsc::channel();

  let node_id = "node1";
  let follower = &mut Node::new(&config, node_id, &db, &mut log);
  let listener = run_listener(web_sender, web_receiver);
  let candidate = follower.run(&node_sender, &node_receiver);
}

fn main() {
  let config = RaftConfig {
    db_path: "demo",
    hosts: vec!["node1:7878", "node2:7878", "node3:7878"],
    election_timeout_min_ms: 150,
    election_timeout_max_ms: 300,
    heartbeat_timeout_ms: 10000
  };
  run_with_config(config)
}



