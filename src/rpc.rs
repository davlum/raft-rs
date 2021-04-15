use crate::metadata::Term;

extern crate serde;
extern crate serde_json;

use std::io;
use serde::{Serialize, Deserialize};
use std::fmt::{Debug, Formatter};
use std::fmt;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Voted {
    Yes,
    No,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum AppendedLogEntry {
    Succeeded,
    Failed,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct LogEntry<T> {
    pub(crate) i: u64,
    cmd: T,
    pub(crate) term: Term,
}

impl<T> Debug for LogEntry<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogEntry")
            .field("i", &self.i)
            .field("term", &self.term)
            .finish()
    }
}

impl<T> PartialEq for LogEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term &&
            self.i == other.i
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct RequestVoteReq {
    pub node_id: String,
    // aka candidate_id
    pub term: Term,
    pub last_log: Option<(u64, Term)>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) struct RequestVoteResp {
    pub node_id: String,
    pub term: Term,
    pub vote_granted: Voted,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AppendEntryReq<T> {
    pub node_id: String,
    // aka leader_id
    pub term: Term,
    // The metadata of the log preceding the entries sent.
    pub prev_log: Option<(u64, Term)>,
    pub(crate) entries: Vec<LogEntry<T>>,
    pub leader_commit: Option<u64>,
}

impl<T: Clone> Debug for AppendEntryReq<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendEntryReq")
            .field("node_id", &self.node_id)
            .field("term", &self.term)
            .field("prev_log", &self.prev_log)
            .field("leader_commit", &self.leader_commit)
            .field("entries", &self.entries)
            .finish()
    }
}

impl<T: Clone> PartialEq for AppendEntryReq<T> {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id &&
            self.term == other.term &&
            self.prev_log == other.prev_log &&
            self.leader_commit == other.leader_commit &&
            self.entries == other.entries
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppendEntryResp {
    pub(crate) node_id: String,
    pub term: Term,
    pub(crate) success: AppendedLogEntry,
}

pub enum CommitResp {
    Commited,
    NotLeader(Option<String>),
    Error,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum RPCReq<T> {
    AE(AppendEntryReq<T>),
    RV(RequestVoteReq),
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) enum RPCResp {
    AE(AppendEntryResp),
    RV(RequestVoteResp),
}

#[derive(Debug)]
pub(crate) enum RpcError {
    StreamError(io::Error),
    DeserializationError(serde_json::Error),
    TimeoutError,
}

pub enum Committed<T> {
    CMD(T),
    NotLeader(Option<String>),
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
