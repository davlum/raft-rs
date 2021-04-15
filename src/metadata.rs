extern crate serde;
extern crate serde_json;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Term(pub u64);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Metadata {
    current_term: Term,
    voted_for: Option<String>,
}

pub(crate) trait MetadataStore {
    fn new(path: Option<&str>) -> Self;
    fn get_term(&self) -> Term;
    fn get_voted_for(&self) -> Option<String>;
    fn set_term(&mut self, term: Term);
    fn inc_term(&mut self) -> Term;
    fn set_voted_for(&mut self, voted_for: Option<String>);
}

impl MetadataStore for Metadata {
    fn new(_: Option<&str>) -> Self {
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
