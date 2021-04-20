mod node;
mod aplog;
mod metadata;
mod config;
mod rpc;
mod test;

use log::{error, info, trace};
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::config::RaftConfig;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use crate::node::{Node, State, Msgs};
use crate::rpc::{RPCResp, RPCReq, RpcError, AppendResp, LogEntry, AppendEntryReq, Committed};
use crate::metadata::FileMetadata;
use std::time::{Duration, SystemTime};
use std::sync::mpsc::RecvTimeoutError;
use std::net::{TcpStream, TcpListener};
use std::io::{LineWriter, Write, BufReader, BufRead};
use crate::aplog::TypedCommitLog;
use futures::channel::oneshot;
use rand::Rng;

fn read_rpc<T: DeserializeOwned>(timeout: u128, stream: &TcpStream) -> Result<T, RpcError> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let now = SystemTime::now();
    while let Err(_) = reader.read_line(&mut line) {
        if now.elapsed().unwrap().as_millis() > timeout {
            return Err(RpcError::TimeoutError);
        }
    }
    trace!("Received data is {}", &line);
    serde_json::from_str(&line).map_err(|e| RpcError::DeserializationError(e))
}

fn write_line<T: Serialize>(stream: &TcpStream, data: T) -> std::io::Result<()> {
    let str = serde_json::to_string(&data).unwrap();
    let mut stream = LineWriter::new(stream);
    stream.write_all(str.as_bytes())?;
    stream.write_all(b"\n")
}

fn write_string_line(stream: &TcpStream, data: &String) -> std::io::Result<()> {
    let mut stream = LineWriter::new(stream);
    stream.write_all(data.as_bytes())?;
    stream.write_all(b"\n")
}

fn send_and_receive_rpc<T: Serialize + DeserializeOwned>(config: RaftConfig, node: PersistNode<T>, host: String, rpc: RPCReq<T>) -> Msgs<AppendEntryReq<T>> {
    let stream = TcpStream::connect(host).unwrap();
    write_line(&stream, rpc).unwrap();
    let rpc_resp: RPCResp = read_rpc(config.heartbeat_interval.into(), &stream).unwrap();
    let node = &mut *node.lock().unwrap();
    match rpc_resp {
        RPCResp::AE(ae) => node.recv_append_entry_resp(&config, ae),
        RPCResp::RV(rv) => node.recv_request_vote_resp(&config, rv)
    }
}

fn run_tcp_listener<T: DeserializeOwned + Send + 'static>(
    config: RaftConfig,
    rpc_sender: mpsc::Sender<(oneshot::Sender<RPCResp>, RPCReq<T>)>,
) {
    let listener = TcpListener::bind(config.host).unwrap();
    let timeout = config.heartbeat_interval as u128;
    match config.connection_number {
        None => {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let rpc_sender = rpc_sender.clone();
                thread::spawn(move || handle_connection(timeout, stream, rpc_sender));
            }
        }
        Some(mut conn_num) => {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let rpc_sender = rpc_sender.clone();
                thread::spawn(move || handle_connection(timeout, stream, rpc_sender));

                conn_num = conn_num - 1;
                if conn_num == 0 {
                    break;
                }
            }
        }
    }
}

fn handle_connection<T: DeserializeOwned>(timeout: u128, stream: TcpStream, rpc_sender: mpsc::Sender<(oneshot::Sender<RPCResp>, RPCReq<T>)>) {
    let maybe_rpc_req = read_rpc(timeout, &stream);
    match maybe_rpc_req {
        Ok(rpc_req) => {
            let (sender, receiver) = oneshot::channel::<RPCResp>();
            rpc_sender.send((sender, rpc_req)).unwrap();
            futures::executor::block_on(async {
                match receiver.await {
                    Ok(rpc) => write_line(&stream, rpc).unwrap(),
                    Err(e) => write_string_line(&stream, &e.to_string()).unwrap(),
                }
            });
        }
        Err(e) => write_string_line(&stream, &e.to_string()).unwrap()
    }
}

fn get_timeout<T>(config: &RaftConfig, node: PersistNode<T>) -> Duration {
    match node.lock().unwrap().state {
        State::Leader { .. } => Duration::from_millis(config.heartbeat_interval),
        _ => {
            let range = config.timeout_min_ms..config.timeout_max_ms;
            let num = rand::thread_rng().gen_range(range);
            Duration::from_millis(num)
        }
    }
}

fn srv<T: Send + DeserializeOwned + 'static + Serialize>(
    config: RaftConfig,
    node: PersistNode<T>,
    rpc_receiver: mpsc::Receiver<(oneshot::Sender<RPCResp>, RPCReq<T>)>,
    commit_sender: mpsc::Sender<Committed>) {
    loop {
        match rpc_receiver.recv_timeout(get_timeout(&config, node.clone())) {
            Ok((rpc_sender, rpc_req)) => {
                let rpc_resp: RPCResp;
                {
                    let node = &mut *node.lock().unwrap();
                    rpc_resp = match rpc_req {
                        RPCReq::RV(rv) => RPCResp::RV(node.recv_request_vote_req(&config, rv)),
                        RPCReq::AE(ae) => RPCResp::AE(node.recv_append_entry_req(&config, ae)),
                    };
                }
                rpc_sender.send(rpc_resp).unwrap();
            }
            Err(RecvTimeoutError::Timeout) => {
                let config = config.clone();
                let node = node.clone();
                thread::spawn(move || run_timeout(config, node));
            }
            Err(RecvTimeoutError::Disconnected) => {
                error!("{:?}", RecvTimeoutError::Disconnected);
                break;
            }
        }
        let node = node.lock().unwrap();
        match (node.last_applied, node.commit_index) {
            (Some(last_applied), Some(commit_index)) => {
                for i in last_applied..=commit_index {
                    commit_sender.send(Committed(i)).unwrap();
                }
            }
            (_, _) => ()
        }
    }
}


type PersistNode<T> = Arc<Mutex<Node<T, TypedCommitLog<LogEntry<T>>, FileMetadata>>>;

fn run_timeout<T: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig, node: PersistNode<T>) {
    let msgs: Msgs<RPCReq<T>>;
    {
        let unlocked_node = &mut *node.lock().unwrap();
        msgs = unlocked_node.recv_timeout(&config);
    }
    process_msgs(config, node, msgs);
}

/// Recursively send messages unless the Append Entries are empty
fn process_msgs<T: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig, node: PersistNode<T>, msgs: Msgs<RPCReq<T>>) {
    for (host, rpc) in msgs {
        let t_config = config.clone();
        let t_node = node.clone();

        let msgs_handle = thread::spawn(move || send_and_receive_rpc(t_config, t_node, host, rpc)).join();
        match msgs_handle {
            Ok(ae_msgs) => {
                let msgs = ae_msgs.into_iter().filter_map(|(host, ae)| {
                    if ae.entries.len() > 0 {
                        Some((host, RPCReq::AE(ae)))
                    } else { None }
                }).collect();
                process_msgs(config.clone(), node.clone(), msgs)
            },
            Err(e) => error!("{:?}", e)
        }
    }
}


pub struct Client<T> {
    node: PersistNode<T>,
    config: RaftConfig,
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Client<T> {
    fn new(config: RaftConfig, node: PersistNode<T>) -> Self {
        Client { node, config }
    }

    fn append_cmd(&mut self, cmd: T) -> AppendResp {
        let unlocked_node = &mut *self.node.lock().unwrap();
        let (resp, msgs) = unlocked_node.recv_append_req(&self.config, cmd);
        let config = self.config.clone();
        let node = self.node.clone();
        if msgs.len() > 0 {

            thread::spawn(move || process_msgs(config, node, msgs));
        }
        resp
    }
}

pub fn run<T: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig) -> (Client<T>, mpsc::Receiver<Committed>) {
    let (rpc_sender, rpc_receiver) = mpsc::channel();
    let web_config = config.clone();
    thread::spawn(move || run_tcp_listener::<T>(web_config, rpc_sender));
    let (commit_sender, commit_receiver) = mpsc::channel();
    let node: Node<T, TypedCommitLog<LogEntry<T>>, FileMetadata> = Node::new_from_file(config.data_dir.clone());
    let node = Arc::new(Mutex::new(node));
    let client = Client::new(config.clone(), node.clone());
    thread::spawn(move || srv(config, node, rpc_receiver, commit_sender));
    (client, commit_receiver)
}
