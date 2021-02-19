
mod test;

extern crate leveldb;
extern crate rand;
extern crate serde;
extern crate serde_json;

use std::str;
use std::io::prelude::*;
use serde::{Serialize, Deserialize};
use std::sync::mpsc;
use std::time::Duration;
use std::marker::PhantomData;
use std::net::TcpListener;
use std::collections::HashMap;
use std::thread::JoinHandle;
use std::io::BufReader;
use crate::RPC::{AE, RV};
use core::fmt;
use std::thread;
use std::sync::mpsc::SendError;


struct Follower;
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

struct RaftConfig {
  heartbeat_timeout_ms: u64
}

struct Node<'a, State> {
  config: &'a RaftConfig,
  voted: bool,
  node_id: NodeId,
  term: Term,
  _state: PhantomData<State>,
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



impl<'t> Node<'t, Follower> {

  fn new(config: &'t RaftConfig, node_id: &str, log: &'t mut HashMap<String, String>) -> Self {
    return Self{
      config,
      _state: PhantomData,
      voted: false,
      node_id: NodeId(node_id.to_string()),
      term: Term(1),
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
            HandleErr::Time(_e) => break
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
      term: self.term.inc_term(),
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
      // I don't get how this conditional results in a `receiving on a closed channel`
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

fn run_with_config(config: RaftConfig) {
  let mut log = HashMap::new();
  let (web_sender, node_receiver) = mpsc::channel();
  let (node_sender, web_receiver) = mpsc::channel();

  let node_id = "node1";
  let follower = &mut Node::new(&config, node_id, &mut log);
  let _listener = run_listener(web_sender, web_receiver);
  let _candidate = follower.run(&node_sender, &node_receiver);
}

fn main() {
  let config = RaftConfig {
    heartbeat_timeout_ms: 10000
  };
  run_with_config(config)
}



