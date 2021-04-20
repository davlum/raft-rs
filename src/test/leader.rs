#[cfg(test)]
mod append_entry_resp {
    use crate::node::{Node, State, PeerState};
    use crate::aplog::{MemLog, Log};
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{AppendedLogEntry, LogEntry, AppendEntryResp, AppendEntryReq};


    #[test]
    fn test_sends_sets_indexes_correctly() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let logs = vec![
            LogEntry { i: 0, cmd: (), term: Term(1) },
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
        ];
        for log in &logs {
            node.log.append(log).unwrap();
        }
        let config = RaftConfig::mk_config("1", hosts);
        node.become_leader(&config);

        debug_assert_eq!(node.state, State::Leader{
            peer_states: vec![
                PeerState{ host: "2".to_owned(), next_index: 3, match_index: None, entries_len: 0},
                PeerState{ host: "3".to_owned(), next_index: 3, match_index: None, entries_len: 0},
            ]
        });
    }

    #[test]
    fn test_decrements_next_index_on_failure() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let logs = vec![
            LogEntry { i: 0, cmd: (), term: Term(1) },
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
        ];
        for log in &logs {
            node.log.append(log).unwrap();
        }
        node.metadata.set_term(Term(1));
        let config = RaftConfig::mk_config("1", hosts);
        // The match_index for each node will be 3
        node.become_leader(&config);
        let res = node.recv_append_entry_resp(
            &config,
            AppendEntryResp {
                node_id: "2".to_string(),
                term: Term(1),
                success: AppendedLogEntry::Failed
            }
        );
        debug_assert_eq!(node.state, State::Leader{
            peer_states: vec![
                PeerState{ host: "2".to_owned(), next_index: 2, match_index: None, entries_len: 1},
                PeerState{ host: "3".to_owned(), next_index: 3, match_index: None, entries_len: 0},
            ]
        });
        debug_assert_eq!(res, vec![
            ("2".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: Some((1, Term(1))), leader_commit: None,
                entries: vec![LogEntry { i: 2, cmd: (), term: Term(1) }] }),
            ("3".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: Some((2, Term(1))), leader_commit: None, entries: vec![] })])
    }

    #[test]
    fn test_decrements_next_index_on_two_failures() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let logs = vec![
            LogEntry { i: 0, cmd: (), term: Term(1) },
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
        ];
        for log in &logs {
            node.log.append(log).unwrap();
        }
        node.metadata.set_term(Term(1));
        let config = RaftConfig::mk_config("1", hosts);
        // The match_index for each node will be 3
        node.become_leader(&config);
        let _ = node.recv_append_entry_resp(
            &config,
            AppendEntryResp {
                node_id: "2".to_string(),
                term: Term(1),
                success: AppendedLogEntry::Failed
            }
        );
        let res = Node::recv_append_entry_resp(&mut node, &config, AppendEntryResp {
            node_id: "2".to_string(),
            term: Term(1),
            success: AppendedLogEntry::Failed
        });
        debug_assert_eq!(node.state, State::Leader{
            peer_states: vec![
                PeerState{ host: "2".to_owned(), next_index: 1, match_index:None, entries_len: 2},
                PeerState{ host: "3".to_owned(), next_index: 3, match_index:None, entries_len: 0},
            ]
        });
        debug_assert_eq!(res, vec![
            ("2".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: Some((0, Term(1))), leader_commit: None,
                entries: vec![LogEntry { i: 1, cmd: (), term: Term(1) }, LogEntry { i: 2, cmd: (), term: Term(1) }]}),
            ("3".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: Some((2, Term(1))), leader_commit: None, entries: vec![] })])
    }

    #[test]
    fn test_increments_next_index_by_entries_len_and_indices() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        let logs = vec![
            LogEntry { i: 0, cmd: (), term: Term(1) },
            LogEntry { i: 1, cmd: (), term: Term(1) },
            LogEntry { i: 2, cmd: (), term: Term(1) },
        ];
        for log in &logs {
            node.log.append(log).unwrap();
        }
        let config = RaftConfig::mk_config("1", hosts);
        node.become_leader(&config);
        node.metadata.set_term(Term(1));
        node.state = State::Leader{
            peer_states: vec![
                PeerState{ host: "2".to_owned(), next_index: 2, match_index:None, entries_len: 1},
                PeerState{ host: "3".to_owned(), next_index: 3, match_index:None, entries_len: 0},
            ]
        };
        let res = node.recv_append_entry_resp(
            &config,
            AppendEntryResp {
                node_id: "2".to_string(),
                term: Term(1),
                success: AppendedLogEntry::Succeeded
            }
        );
        debug_assert_eq!(node.state, State::Leader{
            peer_states: vec![
                PeerState{ host: "2".to_owned(), next_index: 3, match_index: Some(2), entries_len: 0},
                PeerState{ host: "3".to_owned(), next_index: 3, match_index: None, entries_len: 0},
            ]
        });
        debug_assert_eq!(res, vec![
            ("2".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: Some((2, Term(1))), leader_commit: Some(2), entries: vec![] }),
            ("3".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: Some((2, Term(1))), leader_commit: Some(2), entries: vec![] })
        ])
    }

}
