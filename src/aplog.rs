use commitlog::{message::*, *};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use crate::metadata::DEFAULT_DIR;

const LOG_PATH: &str = "log";

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
    fn new(path: Option<PathBuf>) -> LogResult<Self> where Self: Sized;
    fn read_from(&self, offset: u64) -> LogResult<Vec<T>>;
    fn read_one(&self, offset: u64) -> Option<T>;
    fn read_from_with_prev(&self, offset: u64) -> LogResult<(Option<T>, Vec<T>)>;
    fn last_i(&self) -> Option<u64>;
    fn last(&self) -> Option<T>;
    fn append(&mut self, val: &T) -> LogResult<u64>;
    /// Truncates a file after the offset supplied. The resulting log will
    /// contain entries up to the offset.
    fn drop(&mut self, offset: u64);
    fn flush(&mut self) -> LogResult<()>;
}

impl<T: Clone> Log<T> for MemLog<T> {
    fn new(_: Option<PathBuf>) -> LogResult<Self> {
        Ok(MemLog(Vec::new()))
    }

    fn read_from(&self, offset: u64) -> LogResult<Vec<T>> {
        Ok(self.0[offset as usize..].to_vec())
    }

    fn read_one(&self, offset: u64) -> Option<T> {
        self.0.get(offset as usize).cloned()
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
        self.0.truncate(offset as usize + 1);
    }

    fn flush(&mut self) -> LogResult<()> {
        Ok(())
    }
}

pub(crate) struct TypedCommitLog<T> {
    _data: PhantomData<T>,
    log: CommitLog,
}

impl<T: Serialize + DeserializeOwned> Log<T> for TypedCommitLog<T> {
    fn new(path: Option<PathBuf>) -> LogResult<Self> {
        let mut path_buf = PathBuf::new();
        let base = path.unwrap_or(PathBuf::from(DEFAULT_DIR));
        path_buf.push(base);
        path_buf.push(LOG_PATH);
        let opts = LogOptions::new(path_buf);
        CommitLog::new(opts)
            .map(|log| TypedCommitLog::<T> {
                _data: Default::default(),
                log,
            })
            .map_err(|e| LogError::IOError(e.to_string()))
    }

    fn read_from(&self, offset: u64) -> LogResult<Vec<T>> {
        let msgs = self.log.read(offset, ReadLimit::default())
            .map_err(|e| LogError::SerdeError(e.to_string()))?;
        msgs.iter().map(|msg| {
            let s = String::from_utf8_lossy(msg.payload());
            serde_json::from_str(s.as_ref())
                .map_err(|e| LogError::SerdeError(e.to_string()))
        }).collect()
    }

    fn read_one(&self, offset: u64) -> Option<T> {
        if Some(offset) > self.log.last_offset() {
            return None;
        } else {
            let msgs = self.log.read(offset, ReadLimit::default()).ok()?;
            let entry = msgs.iter().last()?;
            let s = String::from_utf8_lossy(entry.payload());
            serde_json::from_str(s.as_ref())
                .map_err(|e| LogError::SerdeError(e.to_string()))
                .ok()
        }
    }

    fn read_from_with_prev(&self, offset: u64) -> LogResult<(Option<T>, Vec<T>)> {
        if offset == 0 {
            return self.read_from(offset).map(|vec| (None, vec));
        }
        let msgs = self.log.read(offset, ReadLimit::default())
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
        self.log.last_offset()
    }

    fn last(&self) -> Option<T> {
        match self.log.last_offset() {
            None => None,
            Some(offset) => self.read_one(offset)
        }
    }

    fn append(&mut self, val: &T) -> Result<u64, LogError> {
        let str = serde_json::to_string(val)
            .map_err(|e| LogError::SerdeError(e.to_string()))?;
        self.log.append_msg(str)
            .map_err(|e| LogError::ReadError(e.to_string()))
    }

    /// TODO: truncate as implemented in commitlog can't drop the first element. Open a PR
    fn drop(&mut self, offset: u64) {
        self.log.truncate(offset);
    }

    fn flush(&mut self) -> LogResult<()> {
        self.log.flush().map_err(|e| LogError::IOError(e.to_string()))
    }
}

