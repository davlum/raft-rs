mod test;

extern crate leveldb;
extern crate rand;
extern crate serde;
extern crate serde_json;

use std::io::prelude::*;
use std::{str, io};
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, SystemTimeError};

use core::fmt;

use rand::Rng;
use serde::{Serialize, Deserialize};
use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, Error};
use std::thread;
use std::sync::{Arc, Mutex, mpsc};

struct Follower;
struct Leader;
struct Candidate;

struct RaftConfig<'a> {
  host: &'a str,
  db_path: &'a str,
  hosts: Vec<&'a str>,
  election_timeout_min_ms: u64,
  election_timeout_max_ms: u64,
  heartbeat_timeout_ms: u64
}

const RAFT_CONFIG: &'static RaftConfig = &RaftConfig{
  host: "127.0.0.1:3333",
  db_path: "asdf",
  hosts: vec![],
  election_timeout_min_ms: 0,
  election_timeout_max_ms: 0,
  heartbeat_timeout_ms: 0
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct NodeId(String);

enum FollowerTransitions {
  ToCandidate,
  StayFollower
}

enum CandidateTransitions {
  VoterResponded(Voted),
  StayCandidate,
  ToFollower
}

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


#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
enum Voted {
  Yes,
  No
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
enum AppendedLogEntry {
  Succeeded,
  Failed
}

struct LogEntry<T: Send> {
  command: T,
  term: Term
}

struct PersistentState<T: Send> {
  current_term: Term,
  voted_for: Option<NodeId>,
  log: Vec<LogEntry<T>>,
  last_log_term: Term,
  last_log_index: usize
}

struct VolatileState {
  node_id: NodeId,
  commit_index: usize,
  last_applied: usize,
}

struct Node<State, T: Send> {
  _state: PhantomData<State>,
  persistent: PersistentState<T>,
  volatile: VolatileState
}

struct LeaderState {
  next_index: Vec<usize>,
  match_index: Vec<usize>
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteReq {
  node_id: NodeId, // aka candidate_id
  term: Term,
  last_log_index: usize,
  last_log_term: Term
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteResp {
  term: Term,
  vote_granted: Voted
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntryReq {
  node_id: NodeId, // aka leader_id
  term: Term,
  prev_log_index: usize,
  prev_log_term: Term,
  // entries: Vec<T>,
  leader_commit: usize
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntryResp {
  term: Term,
  success: AppendedLogEntry
}

#[derive(Serialize, Deserialize, Debug)]
enum RPCReq {
  AE(AppendEntryReq),
  RV(RequestVoteReq)
}

fn more_up_to_date_log(left_term: Term, left_index: usize, right_term: Term, right_index: usize) -> bool {
  if left_term > right_term { return true; }

  if left_index > right_index { return true; }

  false
}

fn get_timeout_millis() -> u128 {
  rand::thread_rng().gen_range(10000..20000)
}

impl<T: Send> Node<Follower, T> {

  fn srv(&mut self, rpc: RPCReq, mut stream: TcpStream) -> io::Result<()> {
    let str = match rpc {
      RPCReq::RV(rv) => {
        let rv_resp = self.srv_request_vote(&rv);
        serde_json::to_string(&rv_resp)
      },
      RPCReq::AE(ae) => {
        let ae_resp = self.srv_append_entry(&ae);
        serde_json::to_string(&ae_resp)
      }
    }?;
    stream.write(str.as_bytes())?;
    stream.flush()?;
    Ok(())
  }

  fn srv_request_vote(&mut self, req_vote_req: &RequestVoteReq) -> RequestVoteResp {
    if req_vote_req.term < self.persistent.current_term ||
        (self.persistent.voted_for.is_some() &&
         self.persistent.voted_for != Some(req_vote_req.node_id.clone())) ||
        more_up_to_date_log(
          self.persistent.last_log_term,
          self.persistent.last_log_index,
          req_vote_req.last_log_term,
          req_vote_req.last_log_index
        )
    {
      RequestVoteResp {
        term: self.persistent.current_term,
        vote_granted: Voted::No
      }
    } else {
      self.persistent.voted_for = Some(req_vote_req.node_id.clone());

      RequestVoteResp {
        term: self.persistent.current_term,
        vote_granted: Voted::Yes
      }
    }
  }

  fn srv_append_entry(&mut self, append_entry_req: &AppendEntryReq) -> AppendEntryResp {
    AppendEntryResp {
      term: self.persistent.current_term,
      success: AppendedLogEntry::Succeeded
    }
  }
}

// impl<T> Node<Candidate, T> {
//
//   fn srv(&mut self, c: Chan<(), CandidateProtocol>) -> CandidateTransitions {
//     let rv = RequestVoteReq {
//       node_id: self.volatile.node_id.clone(),
//       term: self.persistent.current_term,
//       // Is this data fetched from persistent state? Why not keep it in volatile
//       last_log_index: self.persistent.last_log_index,
//       last_log_term: self.persistent.last_log_term
//     };
//     let c = c.send(rv);
//     match c.recv_timeout(get_timeout()) {
//       Ok((_, req_vote_resp)) => {
//         if req_vote_resp.term > self.persistent.current_term {
//           return CandidateTransitions::ToFollower;
//         }
//         CandidateTransitions::VoterResponded(req_vote_resp.vote_granted)
//       },
//       Err(_) => CandidateTransitions::StayCandidate
//     }
//   }
// }

fn handle_stream(follower: Arc<Mutex<Node<Follower, String>>>, stream: TcpStream, timeout_reset_sender: &mpsc::Sender<()>) -> Result<(), serde_json::Error> {
  let mut reader = BufReader::new(&stream);
  let line = &mut String::new();
  while let Err(_) = reader.read_line(line) {

  }
  let rpc: RPCReq = serde_json::from_str(line)?;
  timeout_reset_sender.send(());
  println!("Parsed rpc and cleared timer");

  follower.lock().unwrap().srv(rpc, stream);

  Ok(())
}

fn run_timer(timer_reset_receiver: mpsc::Receiver<()>, timeout_sender: mpsc::Sender<()>) {
  let mut now = SystemTime::now();
  let mut max_wait = get_timeout_millis();
  loop {
    match timer_reset_receiver.try_recv() {
      Ok(_) => {
        now = SystemTime::now();
        max_wait = get_timeout_millis();
      },
      Err(mpsc::TryRecvError::Empty) => {
        match now.elapsed() {
          Ok(elapsed) => {
            if elapsed.as_millis() > max_wait {
              timeout_sender.send(()).unwrap();
            }
          },
          Err(e) => { println!("Error: {:?}", e)}
        }
      },
      Err(mpsc::TryRecvError::Disconnected) => println!("Timer reset receiver channel Disconnected")
    }
  }
}

fn run_listener(follower: Arc<Mutex<Node<Follower, String>>>, host: &str, timeout_receiver: mpsc::Receiver<()>, timeout_reset_sender: mpsc::Sender<()>) {
  let listener = TcpListener::bind(host).unwrap();
  listener.set_nonblocking(true).expect("Cannot set non-blocking");

  for stream in listener.incoming() {
    // An assumption is made here that an Open TCP stream will send data very shortly afterwards
    let clone = follower.clone();
    let timeout_reset_sender = timeout_reset_sender.clone();
    match (&timeout_receiver.try_recv(), stream) {
      (Ok(_), _) => break,
      (Err(mpsc::TryRecvError::Empty), Ok(s)) => {
        thread::spawn(move|| handle_stream(clone,s, &timeout_reset_sender));
      },
      (Err(mpsc::TryRecvError::Empty), Err(e)) if e.kind() == io::ErrorKind::WouldBlock => continue,
      (broadcast_err, listener_err) => { println!("{:?} {:?}", broadcast_err, listener_err) },
    }
  }
}

fn run(raft_config: &'static RaftConfig) -> std::io::Result<()> {
  let mut follower: Node<Follower, String> = Node {
    _state: PhantomData,
    persistent: PersistentState {
      current_term: Term(1),
      voted_for: None,
      log: vec![],
      last_log_term: Term(0),
      last_log_index: 0
    },
    volatile: VolatileState {
      node_id: NodeId(raft_config.host.to_string()),
      commit_index: 0,
      last_applied: 0
    }
  };


  let arc_follower = Arc::new(Mutex::new(follower));
  let (timer_reset_sender, timer_reset_receiver) = mpsc::channel();
  let (timeout_sender, mut timeout_receiver) = mpsc::channel();

  let timer_handle = thread::spawn(move || run_timer(timer_reset_receiver, timeout_sender));
  let listener_handle = thread::spawn(move || run_listener(arc_follower, raft_config.host, timeout_receiver, timer_reset_sender));

  listener_handle.join();
  // Must signal to timer after receiving RPC
  println!("Didn't receive good requests becoming candidate");
  // let follower = arc_follower.lock().unwrap();
  // let mut candidate = Node {
  //   _state: PhantomData,
  //   persistent: PersistentState {
  //     current_term: Term(1) + follower.persistent.current_term,
  //     voted_for: Some(follower.volatile.node_id),
  //     log: follower.persistent.log,
  //     last_log_term: follower.persistent.last_log_term,
  //     last_log_index: follower.persistent.last_log_index
  //   },
  //   volatile: VolatileState {
  //     node_id: follower.volatile.node_id,
  //     commit_index: follower.volatile.commit_index,
  //     last_applied: follower.volatile.last_applied
  //   }
  // };
  // for node_id in raft_config.hosts {
  //
  // }
  // for stream in listener.incoming() {
  //   let mut rx1 = tx_clone.subscribe();
  //   let stream = stream.unwrap();
  //   let follower_clone = arc_follower.clone();
  //   thread::spawn(move || {
  //     match rx1.try_recv() {
  //       Ok(_) => println!("Timed out"),
  //       Err(broadcast::TryRecvError::Empty) => { handle_client(follower_clone, stream); },
  //       Err(broadcast::TryRecvError::Closed) => println!("Channel closed"),
  //       _ => {}
  //     };
  //   });
  // }

  Ok(())
}

fn main() {
  run(RAFT_CONFIG);
}
