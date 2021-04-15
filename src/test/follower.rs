#[cfg(test)]
mod request_vote_req {
    use crate::Node;
    use crate::log::MemLog;
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{RequestVoteReq, Voted, RequestVoteResp, LogEntry};


    #[test]
    fn test_replies_yes_to_equal_or_higher_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.metadata.set_term(Term(1));
        let config = RaftConfig::mk_config("1",hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: None
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::Yes
        })])
    }

    #[test]
    fn test_replies_no_to_lower_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.metadata.set_term(Term(2));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: None
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::No
        })])
    }

    #[test]
    fn test_increments_term_on_higher_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: None
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        })])
    }

    #[test]
    fn test_replies_no_when_already_voted_and_term_is_equal() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.metadata.set_voted_for(Some("3".to_owned()));
        node.metadata.set_term(Term(1));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: None
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::No
        })])
    }

    #[test]
    fn test_replies_yes_when_already_voted_and_term_is_higher() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.metadata.set_voted_for(Some("3".to_owned()));
        node.metadata.set_term(Term(1));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: None
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        })])
    }

    #[test]
    fn test_replies_no_when_has_log_to_none_log() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.last_log = Some((2, Term(1)));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: None
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::No
        })])
    }

    #[test]
    fn test_replies_no_to_lower_log_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.last_log = Some((1, Term(2)));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((2, Term(1))))
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::No
        })])
    }

    #[test]
    fn test_replies_yes_to_higher_log_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.last_log = Some((1, Term(1)));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: (Some((2, Term(2))))
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        })])
    }

    #[test]
    fn test_replies_no_to_equal_log_term_lower_index() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.last_log = Some((2, Term(1)));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((1, Term(1))))
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::No
        })])
    }

    #[test]
    fn test_replies_no_to_equal_log_term_equal_index() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.last_log = Some((2, Term(1)));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((2, Term(1))))
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::Yes
        })])
    }

    #[test]
    fn test_replies_no_to_equal_log_term_larger_index() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.last_log = Some((2, Term(1)));
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((3, Term(1))))
            }
        );
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![("2".to_owned(), RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::Yes
        })])
    }
}

#[cfg(test)]
mod request_vote_resp {
    use crate::Node;
    use crate::log::MemLog;
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{RequestVoteResp, Voted, LogEntry};

    #[test]
    fn test_doesnt_reply() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_resp(
            &config,
            RequestVoteResp {
                node_id: "2".to_string(),
                term: Term(2),
                vote_granted: Voted::No
            }
        );
        assert_eq!(node.metadata.get_term(), Term(2));
        debug_assert_eq!(res.logs, vec![]);
        debug_assert_eq!(res.msgs, vec![]);
    }
}
