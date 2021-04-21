use std::{env, thread, str};
use std::path::Path;
use std::sync::{mpsc, Mutex, Arc};

use leveldb::database::Database;
use leveldb::database::kv::KV;
use leveldb::options::{Options, WriteOptions, ReadOptions};
use db_key::from_u8;

use log::{error, trace};
use serde::{Serialize, Deserialize};

use raftrs::config::RaftConfig;
use raftrs::rpc::{AppendResp, Committed};
use raftrs::{run, Client};
use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, BufRead, LineWriter, Write};
use std::num::ParseIntError;


const LEVEL_DB_PATH: &str = "data/leveldb";

#[derive(Serialize, Deserialize, Debug)]
enum Cmd {
    Get(i32),
    Del(i32),
    Put {
        k: i32,
        v: String,
    },
}


#[derive(Clone, Debug)]
struct DeserError(String);

const DESER_MSG: &str = "Could not deserialize CMD";

impl From<ParseIntError> for DeserError {
    fn from(e: ParseIntError) -> Self {
        DeserError(e.to_string())
    }
}

impl Cmd {
    fn from_str(s: &str) -> Result<Cmd, DeserError> {
        let deser_err = DeserError(DESER_MSG.to_owned());
        let mut ss = s.split_whitespace();
        match ss.next() {
            Some("get") => {
                let k = ss.next().ok_or(deser_err)?;
                Ok(Cmd::Get(k.parse::<i32>()?))
            }
            Some("put") => {
                let k = ss.next().ok_or(deser_err.clone())?;
                let v = ss.next().ok_or(deser_err)?;
                Ok(Cmd::Put{ k: k.parse::<i32>()?, v: v.to_owned()})
            }
            Some("del") => {
                let k = ss.next().ok_or(deser_err)?;
                Ok(Cmd::Del(k.parse::<i32>()?))
            }
            _ => Err(deser_err)
        }
    }
}

struct DB {
    raft_client: Client<Cmd>,
    leveldb: Database<i32>,
}

impl DB {
    fn new(raft_client: Client<Cmd>) -> Self {
        let mut options = Options::new();
        options.create_if_missing = true;
        let path = Path::new(LEVEL_DB_PATH);
        let leveldb = match Database::open(path, options) {
            Ok(db) => { db }
            Err(e) => { panic!("failed to open database: {:?}", e) }
        };
        DB { raft_client, leveldb }
    }

    fn get(&self, k: i32) -> String {
        let opts = ReadOptions::new();
        match self.leveldb.get(opts, k) {
            Ok(None) => "Error: value does not exist".to_owned(),
            Ok(Some(bytes)) => match str::from_utf8(&bytes) {
                Ok(str) => format!("Ok: {}", str.to_owned()),
                Err(e) => format!("Error: {}", e.to_string())
            }
            Err(e) => format!("Error: {}", e)
        }
    }
}

/// This is needed in order to translate the Docker bridge network hostnames
/// to what is being used in the example
fn leader_translation(s: Option<String>) -> &'static str {
    let s = s.unwrap_or("unknown".to_owned());
    match s.as_str() {
        "raft1:3333" => "127.0.0.1:8001",
        "raft2:3333" => "127.0.0.1:8002",
        "raft3:3333" => "127.0.0.1:8003",
        _ => "unknown"
    }
}

fn handle_client(db: Arc<Mutex<DB>>, stream: TcpStream) -> std::io::Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    while let Err(_) = reader.read_line(&mut line) {}
    let mut db = db.lock().unwrap();
    let resp = match Cmd::from_str(&line) {
        Ok(Cmd::Get(k)) => db.get(k),
        Ok(cmd) => {
            match db.raft_client.append_cmd(cmd) {
                AppendResp::Appended(i) => format!("Ok: Appended at {}", i),
                AppendResp::NotLeader(leader) => format!("Error: Leader is {}", leader_translation(leader)),
                AppendResp::Error(e) => format!("Error: {}", e)
            }
        }
        Err(e) => format!("Error: {:?}", e)
    };
    let mut stream = LineWriter::new(&stream);
    stream.write_all(resp.as_bytes())?;
    stream.write_all(b"\n")
}

fn apply_committed(receiver: mpsc::Receiver<Committed<Cmd>>, db: Arc<Mutex<DB>>) {
    while let Ok(commited) = receiver.recv() {
        match commited.cmd {
            Cmd::Get(_) => trace!("Get should be handled by webserver"),
            Cmd::Del(k) => {
                let opts = WriteOptions::new();
                db.lock().unwrap().leveldb.delete(opts, k).unwrap()
            }
            Cmd::Put { k, v } => {
                let opts = WriteOptions::new();
                db.lock().unwrap().leveldb.put(opts, k, v.as_bytes()).unwrap()
            }
        }
    }
}

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let mut hosts = vec![];
    for h in args[1].split(",") {
        hosts.push(h.to_owned())
    }
    let host = &args[2];
    let config = RaftConfig::mk_config(host, hosts);

    let listener = TcpListener::bind("0.0.0.0:8000").unwrap();

    let (cli, receiver) = run::<Cmd>(config);

    let leveldb = DB::new(cli);
    let db = Arc::new(Mutex::new(leveldb));
    let commit_db = db.clone();
    thread::spawn(move || apply_committed(receiver, commit_db));

    for stream in listener.incoming() {
        let db = db.clone();
        thread::spawn(move || handle_client(db, stream.unwrap()));
    }
}
