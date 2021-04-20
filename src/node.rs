use std::cmp::{Ordering, min};
use std::str;

use log::{info, trace};
use crate::metadata::{MetadataStore, Term};
use crate::rpc::{LogEntry, AppendResp, RPCReq};
use crate::aplog::Log;
use crate::rpc::{RequestVoteReq, RequestVoteResp, Voted, AppendEntryReq, AppendEntryResp, AppendedLogEntry};
use std::marker::PhantomData;
use crate::config::RaftConfig;
use std::fmt::Debug;
use std::path::PathBuf;


#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum State {
    Follower,
    Leader { peer_states: Vec<PeerState> },
    Candidate { acquired_votes: Vec<String> },
}

pub(crate) const LEADER: State = State::Leader { peer_states: vec![] };

const CANDIDATE: State = State::Candidate { acquired_votes: vec![] };

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerState {
    pub(crate) host: String,
    pub(crate) next_index: u64,
    pub(crate) match_index: u64,
    pub(crate) entries_len: usize,
}

pub(crate) struct Node<E, L, M> {
    _data: PhantomData<E>,
    pub(crate) log: L,
    pub(crate) metadata: M,
    pub(crate) state: State,
    leader_id: Option<String>,
    pub(crate) commit_index: Option<u64>,
    pub(crate) last_applied: Option<u64>,
}

pub(crate) type Msgs<RPC> = Vec<(String, RPC)>;

pub(crate) fn cmp_discrim<T>(t1: &T, t2: &T) -> bool {
    std::mem::discriminant(t1) == std::mem::discriminant(t2)
}

impl<L: Log<LogEntry<T>>, M: MetadataStore, T> Node<T, L, M> {
    pub(crate) fn new() -> Self {
        Node {
            _data: Default::default(),
            log: Log::new(None).unwrap(),
            metadata: MetadataStore::new(None),
            state: State::Follower,
            leader_id: None,
            commit_index: None,
            last_applied: None,
        }
    }

    pub(crate) fn new_from_file(data_dir: PathBuf) -> Self {
        Node {
            _data: Default::default(),
            log: Log::new(Some(data_dir.clone())).unwrap(),
            metadata: MetadataStore::new(Some(data_dir)),
            state: State::Follower,
            leader_id: None,
            commit_index: None,
            last_applied: None,
        }
    }

    pub(crate) fn recv_request_vote_req(&mut self, config: &RaftConfig, rv: RequestVoteReq) -> RequestVoteResp {
        debug_assert!(config.hosts.contains(&rv.node_id));
        if rv.term > self.metadata.get_term() {
            self.become_follower(rv.term)
        }

        if rv.term == self.metadata.get_term() &&
            !cmp_discrim(&self.state, &CANDIDATE) &&
            (self.metadata.get_voted_for().is_none() ||
                self.metadata.get_voted_for() == Some(rv.node_id.clone())) &&
            !self.is_more_up_to_date(rv.last_log)
        {
            self.metadata.set_voted_for(Some(rv.node_id.clone()));

            RequestVoteResp {
                node_id: config.host.clone(),
                term: self.metadata.get_term(),
                vote_granted: Voted::Yes,
            }
        } else {
            RequestVoteResp {
                node_id: config.host.clone(),
                term: self.metadata.get_term(),
                vote_granted: Voted::No,
            }
        }
    }

    pub(crate) fn recv_request_vote_resp(&mut self, config: &RaftConfig, rv: RequestVoteResp) -> Msgs<AppendEntryReq<T>> {
        debug_assert!(config.hosts.contains(&rv.node_id));
        if rv.term > self.metadata.get_term() {
            self.become_follower(rv.term)
        }
        match self.state.clone() {
            State::Candidate { mut acquired_votes } => {
                match rv.vote_granted {
                    Voted::Yes => {
                        if !acquired_votes.contains(&rv.node_id) {
                            acquired_votes.push(rv.node_id)
                        }
                        if acquired_votes.len() > config.hosts.len() / 2 {
                            let peer_states = self.become_leader(config);
                            self.mk_append_entry_reqs(config, peer_states)
                        } else {
                            vec![]
                        }
                    }
                    Voted::No => vec![]
                }
            }
            _ => vec![]
        }
    }

    fn mk_append_entry_reqs(&mut self, config: &RaftConfig, mut peer_states: Vec<PeerState>) -> Msgs<AppendEntryReq<T>> {
        let msgs = peer_states.iter_mut().map(|peer_state| {
            (peer_state.host.clone(), self.mk_append_entry_req(&config.host, peer_state))
        }).collect();
        self.state = State::Leader { peer_states };
        msgs
    }

    /// 5.4.1
    /// Raft determines which of two logs is more up-to-date by comparing the index and
    /// term of the last entries in the logs. If the logs have last entries with different terms,
    /// then the log with the later term is more up-to-date. If the logs end with the same term,
    /// then whichever log is longer is more up-to-date
    fn is_more_up_to_date(&self, cand_log: Option<(u64, Term)>) -> bool {
        let last_log = self.log.last().map(|e| (e.i, e.term));
        match (last_log, cand_log) {
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

    fn mk_append_entry_req(&self, host: &str, peer_state: &mut PeerState) -> AppendEntryReq<T> {
        let last_log = self.get_last_log();
        let next_index = peer_state.next_index;
        if last_log.map(|(i, _)| i) >= Some(next_index) {
            let (prev, rest) = self.log.read_from_with_prev(next_index).unwrap();
            let prev = prev.map(|entry| (entry.i, entry.term));
            peer_state.entries_len = rest.len();
            AppendEntryReq {
                node_id: host.to_owned(),
                term: self.metadata.get_term(),
                prev_log: prev,
                entries: rest,
                leader_commit: self.commit_index,
            }
        } else {
            peer_state.entries_len = 0;
            AppendEntryReq {
                node_id: host.to_owned(),
                term: self.metadata.get_term(),
                prev_log: last_log,
                entries: vec![],
                leader_commit: self.commit_index,
            }
        }
    }

    fn become_follower(&mut self, term: Term) {
        self.state = State::Follower;
        self.metadata.set_voted_for(None);
        self.metadata.set_term(term);
        self.leader_id = None;
    }

    pub(crate) fn become_candidate(&mut self, host: String) {
        info!("Node {} becoming candidate", &host);
        self.state = State::Candidate { acquired_votes: vec![host.to_owned()] };
        self.metadata.set_voted_for(Some(host));
        self.metadata.inc_term();
    }

    pub(crate) fn become_leader(&mut self, config: &RaftConfig) -> Vec<PeerState> {
        info!("Node {} becoming leader", &config.host);
        let next_index = self.log.last_i().map_or(0, |i| i + 1);
        let f = move |h: String| PeerState {
            host: h,
            next_index,
            match_index: 0,
            entries_len: 0,
        };
        let peer_states = make_remote_vec(&config.host, &config.hosts, Box::new(f));
        self.state = State::Leader { peer_states: peer_states.clone() };
        self.leader_id = Some(config.host.clone());
        self.metadata.set_voted_for(None);
        peer_states
    }

    pub(crate) fn recv_append_entry_req(&mut self, config: &RaftConfig, ae: AppendEntryReq<T>) -> AppendEntryResp {
        if ae.term < self.metadata.get_term() {
            return AppendEntryResp {
                node_id: config.host.clone(),
                term: self.metadata.get_term(),
                success: AppendedLogEntry::Failed,
            };
        }
        if ae.term >= self.metadata.get_term() {
            self.become_follower(ae.term);
        }
        // Append entries seems legit. set leader;
        self.leader_id = Some(ae.node_id.clone());

        let failed = AppendEntryResp {
            node_id: config.host.clone(),
            term: self.metadata.get_term(),
            success: AppendedLogEntry::Failed,
        };

        match (ae.prev_log, self.get_last_log(), ae.entries.last()) {
            (_, _, None) => self.append_entries(config, &ae),
            (_, None, _) => self.append_entries(config, &ae),
            (prev_log, Some((last_i, last_t)), Some(log)) => {
                if log.i == last_i && log.term == last_t {
                    // Logs are the same, don't bother appending
                    return AppendEntryResp {
                        node_id: config.host.clone(),
                        term: self.metadata.get_term(),
                        success: AppendedLogEntry::Succeeded,
                    };
                }

                if prev_log == Some((last_i, last_t)) {
                    return self.append_entries(config, &ae);
                }

                match prev_log {
                    None => {
                        self.log.drop(0); // Drop everything after i
                        self.append_entries(config, &ae)
                    }
                    Some((prev_i, prev_term)) => {
                        match self.log.read_one(prev_i) {
                            None => failed,
                            Some(entry) => {
                                debug_assert_eq!(entry.i, prev_i);
                                if prev_term == entry.term {
                                    self.log.drop(prev_i + 1); // Drop everything after i
                                    self.append_entries(config, &ae)
                                } else { failed }
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn recv_append_entry_resp(&mut self, config: &RaftConfig, ae: AppendEntryResp) -> Msgs<AppendEntryReq<T>> {
        if ae.term > self.metadata.get_term() {
            self.become_follower(ae.term);
            return vec![];
        }
        match self.state.clone() {
            State::Leader { mut peer_states } => {
                match ae.success {
                    AppendedLogEntry::Succeeded => {
                        let i = get_index(&ae.node_id, &peer_states);
                        let peer_state = &mut peer_states[i];
                        if peer_state.entries_len > 0 {
                            peer_state.next_index += peer_state.entries_len as u64;
                            let next_index = peer_state.next_index;
                            peer_state.match_index = next_index - 1;

                            let start = self.commit_index.map_or(0, |x| x + 1);
                            // TODO return errors ye GOB
                            for entry in self.log.read_from(start).unwrap() {
                                if entry.term == self.metadata.get_term() {
                                    let matches = peer_states.clone().iter().fold(1, |acc, peer_state| {
                                        if peer_state.match_index >= entry.i { acc + 1 } else { acc }
                                    });
                                    if matches > config.hosts.len() / 2 {
                                        self.commit_index = Some(entry.i);
                                    }
                                }
                            }
                        }
                    }
                    AppendedLogEntry::Failed => {
                        let i = get_index(&ae.node_id, &peer_states);
                        let peer_state = &mut peer_states[i];
                        peer_state.next_index -= 1;
                    }
                };
                self.mk_append_entry_reqs(config, peer_states)
            }
            _ => vec![]
        }
    }

    pub(crate) fn recv_timeout(&mut self, config: &RaftConfig) -> Msgs<RPCReq<T>> {
        match self.state.clone() {
            State::Leader { peer_states } => {
                trace!("Sending Append Entry Requests to peers");
                self.mk_append_entry_reqs(config, peer_states).into_iter()
                    .map(|(s, ae)| (s, RPCReq::AE(ae)))
                    .collect()
            }
            _ => {
                self.become_candidate(config.host.clone());
                let node_id = config.host.clone();
                let term = self.metadata.get_term();
                let last_log = self.get_last_log();
                let f = move |h: String| (h, RPCReq::RV(RequestVoteReq {
                    node_id: node_id.clone(),
                    term,
                    last_log,
                }));
                make_remote_vec(&config.host, &config.hosts, Box::new(f))
            }
        }
    }

    pub(crate) fn recv_append_req(&mut self, config: &RaftConfig, cmd: T) -> (AppendResp, Msgs<RPCReq<T>>) {
        match self.state.clone() {
            State::Leader { peer_states } => {
                info!("Appending client request to log");
                let i = self.get_last_log().map_or(0, |(i, _)| i);
                self.log.append(&LogEntry { i, cmd, term: self.metadata.get_term() });
                let msgs = self.mk_append_entry_reqs(config, peer_states).into_iter()
                    .map(|(s, ae)| (s, RPCReq::AE(ae)))
                    .collect();
                (AppendResp::Appended(i), msgs)
            }
            _ => (AppendResp::NotLeader(self.leader_id.clone()), vec![])
        }
    }

    fn append_entries(&mut self, config: &RaftConfig, ae: &AppendEntryReq<T>) -> AppendEntryResp {
        let last_index = if ae.entries.len() > 0 {
            let mut last_index = 0;
            for entry in &ae.entries {
                last_index = self.log.append(entry).unwrap();
                debug_assert_eq!(last_index, entry.i);
            }
            self.log.flush();
            Some(last_index)
        } else { self.get_last_log().map(|(i, _)| i) };

        if ae.leader_commit > self.commit_index {
            self.commit_index = min(ae.leader_commit, last_index)
        }
        AppendEntryResp {
            node_id: config.host.clone(),
            term: self.metadata.get_term(),
            success: AppendedLogEntry::Succeeded,
        }
    }

    fn get_last_log(&self) -> Option<(u64, Term)> {
        self.log.last().map(|e| (e.i, e.term))
    }
}

fn make_remote_vec<B>(host: &str, hosts: &Vec<String>, f: Box<dyn Fn(String) -> B>) -> Vec<B> {
    hosts.clone().into_iter().filter(|h| h != host).map(f).collect()
}

fn get_index(host: &str, indices: &Vec<PeerState>) -> usize {
    // Unwrap, If host isn't in this list we got problems.
    indices.iter().position(|s| s.host == host).unwrap()
}
