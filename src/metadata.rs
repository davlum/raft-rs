use log::warn;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const TERM_PATH: &str = "term";
const VOTED_FOR_PATH: &str = "voted_for";
pub(crate) const DEFAULT_DIR: &str = "data";

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Term(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Metadata {
    current_term: Term,
    voted_for: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FileMetadata {
    term_path: PathBuf,
    voted_for_path: PathBuf,
    metadata: Metadata
}

pub(crate) trait MetadataStore {
    fn new(path: Option<PathBuf>) -> Self;
    fn get_term(&self) -> Term;
    fn get_voted_for(&self) -> Option<String>;
    fn set_term(&mut self, term: Term);
    fn inc_term(&mut self) -> Term;
    fn set_voted_for(&mut self, voted_for: Option<String>);
}

impl MetadataStore for Metadata {
    fn new(_: Option<PathBuf>) -> Self {
        Metadata {
            current_term: Term(0),
            voted_for: None
        }
    }

    fn get_term(&self) -> Term {
        self.current_term
    }

    fn get_voted_for(&self) -> Option<String> {
        self.voted_for.clone()
    }

    fn set_term(&mut self, term: Term) {
        self.current_term = term;
    }

    fn inc_term(&mut self) -> Term {
        self.current_term = Term(self.current_term.0 + 1);
        self.current_term
    }

    fn set_voted_for(&mut self, voted_for: Option<String>) {
        self.voted_for = voted_for
    }
}

fn read_from_file<P: AsRef<Path>>(path: P) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

fn write_to_file<P: AsRef<Path>>(val: &str, path: P) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(val.as_bytes())?;
    Ok(())
}

/// Plz dear god propagate these errors
impl MetadataStore for FileMetadata {
    fn new(data_dir: Option<PathBuf>) -> Self {
        let base = data_dir.unwrap_or(PathBuf::from(DEFAULT_DIR));
        let mut term_path = PathBuf::new();
        term_path.push(&base);
        term_path.push(TERM_PATH);
        let term = match read_from_file(&term_path) {
            Ok(term) => term.trim().parse::<u64>().expect("Could not parse term from file"),
            Err(e) => { warn!("{:?}", e); 0 }
        };
        let mut voted_for_path = PathBuf::new();
        voted_for_path.push(base);
        voted_for_path.push(VOTED_FOR_PATH);
        let voted_for = match read_from_file(&voted_for_path) {
            Ok(s) => if s == "".trim() { None } else { Some(s) },
            Err(e) => { warn!("{:?}", e); None }
        };

        FileMetadata {
            term_path,
            voted_for_path,
            metadata: Metadata {
                current_term: Term(term),
                voted_for
            },
        }
    }

    fn get_term(&self) -> Term {
        self.metadata.current_term
    }

    fn get_voted_for(&self) -> Option<String> {
        self.metadata.voted_for.clone()
    }

    fn set_term(&mut self, term: Term) {
        self.metadata.current_term = term;
        write_to_file(&term.0.to_string(), &self.term_path).unwrap();
    }

    fn inc_term(&mut self) -> Term {
        self.metadata.current_term = Term(self.metadata.current_term.0 + 1);
        write_to_file(&self.metadata.current_term.0.to_string(), &self.term_path).unwrap();
        self.metadata.current_term
    }

    fn set_voted_for(&mut self, voted_for: Option<String>) {
        self.metadata.voted_for = voted_for.clone();
        write_to_file(&voted_for.unwrap_or("".to_owned()), &self.voted_for_path).unwrap();
    }
}
