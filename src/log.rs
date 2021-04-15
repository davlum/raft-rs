use commitlog::{message::*, *};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use crate::rpc::LogEntry;

const METADATA_PATH: &str = "/metadata.json";
const LOG_PATH: &str = "/log";

#[derive(Debug)]
pub enum LogError {
    SerdeError(String),
    ReadError(String),
    AppendError(String),
    IOError(String),
}


pub(crate) type LogResult<T> = Result<T, LogError>;

pub(crate) struct MemLog<T>(pub(crate) Vec<T>);

pub(crate) trait Log<T> {
    fn new(path: Option<&str>) -> LogResult<Self> where Self: Sized;
    fn read_from(&self, offset: u64) -> LogResult<Vec<T>>;
    fn read_from_with_prev(&self, offset: u64) -> LogResult<(Option<T>, Vec<T>)>;
    fn last_i(&self) -> Option<u64>;
    fn last(&self) -> Option<T>;
    fn append(&mut self, val: &T) -> LogResult<u64>;
    /// Truncates a file after the offset supplied. The resulting log will
    /// contain entries up to the offset.
    fn drop(&mut self, offset: u64);
}

impl<T: Clone> Log<T> for MemLog<T> {
    fn new(_: Option<&str>) -> LogResult<Self> {
        Ok(MemLog(Vec::new()))
    }

    fn read_from(&self, offset: u64) -> LogResult<Vec<T>> {
        Ok(self.0[offset as usize..].to_vec())
    }

    fn read_from_with_prev(&self, offset: u64) -> LogResult<(Option<T>, Vec<T>)> {
        if offset == 0 {
            return self.read_from(offset).map(|vec| (None, vec));
        }
        let entries = self.read_from(offset - 1);
        entries.map(|vec| {
            // We know there's at least one element
            let (first, rest) = vec.split_first().unwrap();
            (Some(first).cloned(), rest.to_vec())
        })
    }

    fn last_i(&self) -> Option<u64> {
        let len = self.0.len() as u64;
        if len == 0 { None } else { Some(len - 1) }
    }
    fn last(&self) -> Option<T> {
        self.0.last().cloned()
    }

    fn append(&mut self, val: &T) -> LogResult<u64> {
        self.0.push(val.to_owned());
        Ok(self.last_i().unwrap())
    }

    fn drop(&mut self, offset: u64) {
        self.0.truncate(offset as usize);
    }
}

impl<T: Serialize + DeserializeOwned> Log<T> for CommitLog {
    fn new(path: Option<&str>) -> LogResult<Self> {
        let opts = match path {
            None => LogOptions::new("data".to_owned() + LOG_PATH),
            Some(path) => LogOptions::new(path.to_owned() + LOG_PATH)
        };
        CommitLog::new(opts).map_err(|e| LogError::IOError(e.to_string()))
    }

    fn read_from(&self, offset: u64) -> LogResult<Vec<T>> {
        let msgs = self.read(offset, ReadLimit::default())
            .map_err(|e| LogError::SerdeError(e.to_string()))?;
        msgs.iter().map(|msg| {
            let s = String::from_utf8_lossy(msg.payload());
            serde_json::from_str(s.as_ref())
                .map_err(|e| LogError::SerdeError(e.to_string()))
        }).collect()
    }

    fn read_from_with_prev(&self, offset: u64) -> LogResult<(Option<T>, Vec<T>)> {
        if offset == 0 {
            return self.read_from(offset).map(|vec| (None, vec));
        }
        let msgs = self.read(offset, ReadLimit::default())
            .map_err(|e| LogError::SerdeError(e.to_string()))?;
        let mut entries = msgs.iter().map(|msg| {
            let s = String::from_utf8_lossy(msg.payload());
            serde_json::from_str(s.as_ref())
                .map_err(|e| LogError::SerdeError(e.to_string()))
        });
        let prev = entries.next().unwrap().ok();
        entries.collect::<LogResult<Vec<T>>>().map(|vec| (prev, vec))
    }

    fn last_i(&self) -> Option<u64> {
        self.last_offset()
    }

    fn last(&self) -> Option<T> {
        match self.last_offset() {
            None => None,
            Some(offset) => {
                let msgs = self.read(offset, ReadLimit::default()).ok()?;
                let mut entries = msgs.iter().map(|msg| {
                    let s = String::from_utf8_lossy(msg.payload());
                    serde_json::from_str(s.as_ref())
                        .map_err(|e| LogError::SerdeError(e.to_string()))
                });
                entries.next().unwrap().ok()
            }
        }
    }

    fn append(&mut self, val: &T) -> Result<u64, LogError> {
        let str = serde_json::to_string(val)
            .map_err(|e| LogError::SerdeError(e.to_string()))?;
        self.append_msg(str)
            .map_err(|e| LogError::ReadError(e.to_string()))
    }

    fn drop(&mut self, offset: u64) {
        self.truncate(offset);
    }
}

