#[cfg(test)]
mod request_vote_req {
    use crate::aplog::{MemLog, Log};
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{RequestVoteReq, Voted, RequestVoteResp, LogEntry};
    use crate::node::Node;


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
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::Yes
        })
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
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::No
        })
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
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        })
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
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::No
        })
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
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        })
    }

    #[test]
    fn test_replies_no_when_has_log_to_none_log() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.log.append(&LogEntry{ i: 2, cmd: (), term: Term(1) });
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: None
            }
        );
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::No
        })
    }

    #[test]
    fn test_replies_no_to_lower_log_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.log.append(&LogEntry{ i: 1, cmd: (), term: Term(2) });
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((2, Term(1))))
            }
        );
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::No
        })
    }

    #[test]
    fn test_replies_yes_to_higher_log_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.log.append(&LogEntry{ i: 1, cmd: (), term: Term(1) });
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: (Some((2, Term(2))))
            }
        );
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        });
    }

    #[test]
    fn test_replies_no_to_equal_log_term_lower_index() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.log.append(&LogEntry{ i: 2, cmd: (), term: Term(1) });
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((1, Term(1))))
            }
        );
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::No
        })
    }

    #[test]
    fn test_replies_no_to_equal_log_term_equal_index() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.log.append(&LogEntry{ i: 2, cmd: (), term: Term(1) });
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((2, Term(1))))
            }
        );
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::Yes
        })
    }

    #[test]
    fn test_replies_no_to_equal_log_term_larger_index() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.log.append(&LogEntry{ i: 2, cmd: (), term: Term(1) });
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(1),
                last_log: (Some((3, Term(1))))
            }
        );
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(1),
            vote_granted: Voted::Yes
        })
    }
}

#[cfg(test)]
mod request_vote_resp {
    use crate::node::Node;
    use crate::aplog::MemLog;
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
        debug_assert_eq!(res, vec![]);
    }
}

#[cfg(test)]
mod append_entry_req {
    use crate::node::Node;
    use crate::aplog::{MemLog, Log};
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{LogEntry, AppendEntryReq, AppendEntryResp, AppendedLogEntry};

    #[test]
    fn test_replies_success_on_empty_entries() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: None,
                entries: vec![],
                leader_commit: None
            }
        );
        debug_assert_eq!(node.commit_index, None);
        debug_assert_eq!(node.metadata.get_term(), Term(1));
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Succeeded
        })
    }

    #[test]
    fn test_appends_entries_when_no_prev_log() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let logs = vec![
            LogEntry { i: 0, cmd: (), term: Term(1) },
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: None,
                entries: logs.clone(),
                leader_commit: Some(1)
            }
        );
        debug_assert_eq!(node.commit_index, Some(1));
        debug_assert_eq!(node.metadata.get_term(), Term(1));
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Succeeded
        });
        debug_assert_eq!(node.log.0, logs);
    }

    #[test]
    fn test_appends_entries_when_prev_log_is_equal() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut logs = vec![
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
            LogEntry { i: 3, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let log = LogEntry { i: 0, cmd: (), term: Term(1) };
        node.log.append(&log);
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: Some((log.i, log.term)),
                entries: logs.clone(),
                leader_commit: Some(5)
            }
        );
        debug_assert_eq!(node.commit_index, Some(3));
        debug_assert_eq!(node.metadata.get_term(), Term(1));
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Succeeded
        });
        logs.insert(0, log.clone());
        debug_assert_eq!(node.log.0, logs);
    }

    #[test]
    fn test_truncates_until_matching_and_appends() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut logs = vec![
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
            LogEntry { i: 3, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let log1 = LogEntry { i: 0, cmd: (), term: Term(1) };
        let log2 = LogEntry { i: 1, cmd: (), term: Term(2) };
        node.log.append(&log1);
        node.log.append(&log2);
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: Some((log1.i, log1.term)),
                entries: logs.clone(),
                leader_commit: Some(2)
            }
        );
        debug_assert_eq!(node.commit_index, Some(2));
        debug_assert_eq!(node.metadata.get_term(), Term(1));
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Succeeded
        });
        logs.insert(0, log1.clone());
        debug_assert_eq!(node.log.0, logs);
    }

    #[test]
    fn test_replies_failed_on_lower_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let logs = vec![
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
            LogEntry { i: 3, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.metadata.set_term(Term(2));
        let log1 = LogEntry { i: 0, cmd: (), term: Term(1) };
        node.log.append(&log1);
        let log2 = LogEntry { i: 1, cmd: (), term: Term(2) };
        node.log.append(&log2);
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: Some((log1.i, log1.term)),
                entries: logs.clone(),
                leader_commit: Some(1)
            }
        );
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(2),
            success: AppendedLogEntry::Failed
        });
        debug_assert_eq!(node.log.0, vec![log1, log2]);
    }

    #[test]
    fn test_replies_failed_when_no_matching_previous_log() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let logs = vec![
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
            LogEntry { i: 3, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let log1 = LogEntry { i: 0, cmd: (), term: Term(1) };
        node.log.append(&log1);
        let log2 = LogEntry { i: 1, cmd: (), term: Term(2) };
        node.log.append(&log2);
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: Some((3, Term(2))),
                entries: logs.clone(),
                leader_commit: Some(1)
            }
        );
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Failed
        });
        debug_assert_eq!(node.log.0, vec![log1, log2]);
    }

    #[test]
    fn test_no_op_succeeds_when_already_contains_logs() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let logs = vec![
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
            LogEntry { i: 3, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        for log in &logs {
            node.log.append(log).unwrap();
        }
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: Some((0, Term(1))),
                entries: logs.clone(),
                leader_commit: Some(1)
            }
        );
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Succeeded
        });
        debug_assert_eq!(node.log.0, logs);
    }

    #[test]
    fn test_succeeds_when_prev_log_matches_last_log() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let logs = vec![
            LogEntry { i: 0, cmd: (), term: Term(1) },
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
        ];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        for log in &logs {
            node.log.append(log).unwrap();
        }
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_append_entry_req(
            &config,
            AppendEntryReq {
                node_id: "2".to_string(),
                term: Term(1),
                prev_log: Some((2, Term(1))),
                entries: vec![],
                leader_commit: Some(2)
            }
        );
        debug_assert_eq!(node.commit_index, Some(2));
        debug_assert_eq!(res, AppendEntryResp{
            node_id: "1".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Succeeded
        });
        debug_assert_eq!(node.log.0, logs);
    }

}
