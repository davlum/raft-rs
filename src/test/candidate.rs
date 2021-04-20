#[cfg(test)]
mod request_vote_req {
    use crate::node::{Node, State};
    use crate::aplog::MemLog;
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{RequestVoteReq, Voted, RequestVoteResp, LogEntry};

    #[test]
    fn test_replies_no_because_is_candidate() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.become_candidate("1".to_owned());
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
    fn test_becomes_follower_on_higher_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.become_candidate("1".to_owned());
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_req(
            &config,
            RequestVoteReq {
                node_id: "2".to_owned(),
                term: Term(2),
                last_log: None
            }
        );
        debug_assert_eq!(node.state, State::Follower);
        debug_assert_eq!(res, RequestVoteResp{
            node_id: "1".to_owned(),
            term: Term(2),
            vote_granted: Voted::Yes
        })
    }

}

#[cfg(test)]
mod request_vote_resp {
    use crate::node::{Node, State, PeerState};
    use crate::aplog::MemLog;
    use crate::metadata::{Metadata, Term, MetadataStore};
    use crate::config::RaftConfig;
    use crate::rpc::{RequestVoteResp, Voted, AppendEntryReq, LogEntry};

    #[test]
    fn test_no_state_change_on_no() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.become_candidate("1".to_owned());
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_resp(
            &config,
            RequestVoteResp {
                node_id: "2".to_string(),
                term: Term(1),
                vote_granted: Voted::No
            }
        );
        debug_assert_eq!(res, vec![]);
    }

    #[test]
    fn test_becomes_follower_on_higher_term() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.become_candidate("1".to_owned());
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_resp(
            &config,
            RequestVoteResp {
                node_id: "2".to_string(),
                term: Term(2),
                vote_granted: Voted::No
            }
        );
        debug_assert_eq!(node.state, State::Follower);
        debug_assert_eq!(res, vec![]);
    }

    #[test]
    fn test_no_state_change_on_already_received_vote() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.become_candidate("1".to_owned());
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_resp(
            &config,
            RequestVoteResp {
                node_id: "1".to_string(),
                term: Term(1),
                vote_granted: Voted::Yes
            }
        );
        debug_assert_eq!(res, vec![]);
    }

    #[test]
    fn test_becomes_leader_on_majority_votes() {
        let hosts = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let mut node: Node<(), MemLog<LogEntry<()>>, Metadata> = Node::new();
        node.become_candidate("1".to_owned());
        let config = RaftConfig::mk_config("1", hosts);
        let res = node.recv_request_vote_resp(
            &config,
            RequestVoteResp {
                node_id: "2".to_string(),
                term: Term(1),
                vote_granted: Voted::Yes
            }
        );
        debug_assert_eq!(node.state, State::Leader{
            peer_states: vec![
                PeerState{ host: "2".to_owned(), next_index: 0, match_index:None, entries_len: 0},
                PeerState{ host: "3".to_owned(), next_index: 0, match_index:None, entries_len: 0},
            ]
        });
        debug_assert_eq!(res, vec![
            ("2".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: None, leader_commit: None, entries: vec![] }),
            ("3".to_owned(), AppendEntryReq { node_id: "1".to_owned(), term: Term(1), prev_log: None, leader_commit: None, entries: vec![] })
        ]);
    }
}
