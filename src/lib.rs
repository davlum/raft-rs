mod log;
mod metadata;
mod config;
mod rpc;
mod test;

extern crate commitlog;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate futures;

use std::cmp::{Ordering, min};
use std::str;


use metadata::{MetadataStore, Term};
use rpc::LogEntry;
use log::Log;
use crate::rpc::{RequestVoteReq, RequestVoteResp, RPCResp, Voted, AppendEntryReq, AppendEntryResp, AppendedLogEntry};
use std::marker::PhantomData;
use crate::config::RaftConfig;
use crate::State::Follower;
use std::fmt::Debug;
use crate::log::LogResult;

const LEADER: State = State::Leader {
    next_indices: vec![],
    match_indices: vec![],
};

const CANDIDATE: State = State::Candidate { acquired_votes: vec![] };

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Follower,
    Leader {
        next_indices: Vec<(String, u64)>,
        match_indices: Vec<(String, u64)>,
    },
    Candidate {
        acquired_votes: Vec<String>
    },
}

pub(crate) struct Node<E, L, M> {
    _data: PhantomData<E>,
    log: L,
    data_dir: String,
    metadata: M,
    state: State,
    leader_id: Option<String>,
    commit_index: Option<u64>,
    last_applied: Option<u64>,
    last_log: Option<(u64, Term)>,
}

type Previous = Option<(u64, Term)>;

#[derive(Default)]
pub(crate) struct Trans<T, RPC> {
    pub logs: Vec<LogEntry<T>>,
    pub msgs: Vec<(String, RPC)>,
}

fn cmp_discrim<T>(t1: &T, t2: &T) -> bool {
    std::mem::discriminant(t1) == std::mem::discriminant(t2)
}

impl<L: Log<LogEntry<T>>, M: MetadataStore, T> Node<T, L, M> {
    fn new() -> Self {
        Node {
            _data: Default::default(),
            log: Log::new(None).unwrap(),
            data_dir: "".to_owned(),
            metadata: MetadataStore::new(None),
            state: State::Follower,
            leader_id: None,
            commit_index: None,
            last_applied: None,
            last_log: None
        }
    }
    // pub fn new(data_dir: &str) -> Self {
    //     let metadata = MetadataStore::new(Some(data_dir));
    //     let log = Log::new(Some(data_dir)).expect("Could not open log");
    //     let maybe_last = log.last::<Entry>();
    //     match maybe_last {
    //
    //     }
    //     let mut node = Node {
    //         state: State::Follower,
    //         data_dir: data_dir.to_owned(),
    //         leader_id: None,
    //         metadata,
    //         commit_index: None,
    //         last_applied: None,
    //         log,
    //         last_log: None,
    //     };
    //     match node.last::<Entry>() {
    //         Ok((i, entry)) => {
    //             node.last_log = Some((i, entry.term));
    //             node
    //         }
    //         Err(_) => node
    //     }
    // }
    fn recv_request_vote_req(&mut self, config: &RaftConfig, rv: RequestVoteReq) -> Trans<T, RequestVoteResp> {
        debug_assert!(config.hosts.contains(&rv.node_id));
        if rv.term > self.metadata.get_term() {
            self.become_follower(rv.term)
        }

        return if rv.term == self.metadata.get_term() &&
            !cmp_discrim(&self.state, &CANDIDATE) &&
            (self.metadata.get_voted_for().is_none() ||
                self.metadata.get_voted_for() == Some(rv.node_id.clone())) &&
            !self.is_more_up_to_date(rv.last_log)
        {
            self.metadata.set_voted_for(Some(rv.node_id.clone()));

            Trans {
                logs: vec![],
                msgs: vec![(rv.node_id, RequestVoteResp {
                    node_id: config.host.clone(),
                    term: self.metadata.get_term(),
                    vote_granted: Voted::Yes,
                })],
            }
        } else {
            Trans {
                logs: vec![],
                msgs: vec![(rv.node_id, RequestVoteResp {
                    node_id: config.host.clone(),
                    term: self.metadata.get_term(),
                    vote_granted: Voted::No,
                })],
            }
        }
    }

    fn recv_request_vote_resp(&mut self, config: &RaftConfig, rv: RequestVoteResp) -> Trans<T, AppendEntryReq<T>> {
        debug_assert!(config.hosts.contains(&rv.node_id));
        if rv.term > self.metadata.get_term() {
            self.become_follower(rv.term)
        }
        let trans = Trans { logs: vec![], msgs: vec![] };
        match self.state.clone() {
            State::Candidate { mut acquired_votes } => {
                match rv.vote_granted {
                    Voted::Yes => {
                        if !acquired_votes.contains(&rv.node_id) {
                            acquired_votes.push(rv.node_id)
                        }
                        if acquired_votes.len() > config.hosts.len() / 2 {
                            let next_indices = self.become_leader(config);
                            let mut entries = vec![];
                            for (dest, next_index) in &next_indices {
                                let ae = self.mk_append_entry_req(config.host.clone(), *next_index);
                                entries.push((dest.to_owned(), ae))
                            }
                            Trans { logs: vec![], msgs: entries }
                        } else {
                            trans
                        }
                    }
                    Voted::No => trans
                }

            },
            _ => trans
        }
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

    fn mk_append_entry_req(&self, host: String, next_index: u64) -> AppendEntryReq<T> {
        let last_index = self.last_log.map(|(i, _)| i);
        if last_index >= Some(next_index) {
            let (prev, rest) = self.log.read_from_with_prev(next_index).unwrap();
            let prev = prev.map(|entry| (entry.i, entry.term));
            AppendEntryReq {
                node_id: host,
                term: self.metadata.get_term(),
                prev_log: prev,
                entries: rest,
                leader_commit: self.commit_index
            }
        } else {
            AppendEntryReq {
                node_id: host,
                term: self.metadata.get_term(),
                prev_log: None,
                entries: vec![],
                leader_commit: self.commit_index
            }
        }
    }

    fn become_follower(&mut self, term: Term) {
        self.state = State::Follower;
        self.metadata.set_voted_for(None);
        self.metadata.set_term(term);
    }

    fn become_candidate(&mut self, host: String) {
        self.state = State::Candidate { acquired_votes: vec![host.to_owned()] };
        self.metadata.set_voted_for(Some(host));
        self.metadata.inc_term();
    }

    fn become_leader(&mut self, config: &RaftConfig) -> Vec<(String, u64)> {
        let next_index = self.log.last_i().map_or(0, |i| i + 1);
        let next_f = Box::new(move |h: String| (h, next_index));
        let next_indices = make_remote_vec(&config.host, &config.hosts, next_f);
        self.state = State::Leader {
            next_indices: next_indices.clone(),
            match_indices: make_remote_vec(&config.host, &config.hosts, Box::new(|h| (h, 0))),
        };
        self.leader_id = Some(config.host.clone());
        self.metadata.set_voted_for(None);
        next_indices
    }

    fn recv_append_entry_req(&mut self, config: &RaftConfig, ae: AppendEntryReq<T>) -> Trans<T, AppendEntryResp> {
        if ae.term < self.metadata.get_term() {
            let resp = self.mk_append_entry_resp(config.host.clone(), AppendedLogEntry::Failed);
            return Trans {
                logs: vec![],
                msgs: vec![(ae.node_id, resp)]
            }
        }
        if ae.term > self.metadata.get_term() {
            self.become_follower(ae.term);
        }
        if ae.term == self.metadata.get_term() && cmp_discrim(&self.state, &CANDIDATE) {
            // If terms are equal and this we are in election, then receival of AppendEntries
            // indicates that this node won
            self.become_follower(ae.term);
        }
        self.leader_id = Some(ae.node_id.clone());

        self.verify_entries(&ae.entries);

        if self.last_log != ae.prev_log {
            // Try to clean up the follower logs here

            let resp = self.mk_append_entry_resp(config.host.clone(), AppendedLogEntry::Failed);
            return Trans {
                logs: vec![],
                msgs: vec![(ae.node_id, resp)]
            }
        }


        let mut last_index = 0;
        for entry in &ae.entries {
            last_index = self.log.append(entry).unwrap();
        }

        if ae.leader_commit > self.commit_index {
            self.commit_index = min(ae.leader_commit, Some(last_index))
        }

        let resp = self.mk_append_entry_resp(config.host.clone(), AppendedLogEntry::Succeeded);
        return Trans {
            logs: vec![],
            msgs: vec![(ae.node_id, resp)]
        }
    }

    fn mk_append_entry_resp(&self, host: String, success: AppendedLogEntry) -> AppendEntryResp {
        AppendEntryResp {
            node_id: host,
            term: self.metadata.get_term(),
            success
        }
    }

    fn verify_entries(&mut self, logs: &Vec<LogEntry<T>>) -> LogResult<()> {
        let first_i = logs[0].i.clone();
        let host_logs = self.log.read_from(first_i)?;
        for (host_log, log) in host_logs.into_iter().zip(logs) {
            if host_log.term != log.term {
                self.log.drop(host_log.i);
                break;
            }
        }
        Ok(())
    }
}

fn make_remote_vec<B>(host: &str, hosts: &Vec<String>, f: Box<dyn Fn(String) -> B>) -> Vec<B> {
    hosts.clone().into_iter().filter(|h| h != host).map(f).collect()
}
