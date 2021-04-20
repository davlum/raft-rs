#![feature(proc_macro_hygiene, decl_macro)]

use std::{env, thread};
use std::str;
use std::path::Path;
use std::sync::{mpsc, Mutex, Arc};

use leveldb::database::Database;
use leveldb::database::kv::KV;
use leveldb::database::error::Error;
use leveldb::options::{Options, WriteOptions, ReadOptions};
use db_key::from_u8;

use log::{error, trace};
use serde::{Serialize, Deserialize};
use rocket::State;

use raftrs::config::RaftConfig;
use raftrs::rpc::{AppendResp, Committed};
use raftrs::{run, Client};

const LEVEL_DB_PATH: &str = "data/leveldb";

#[macro_use]
extern crate rocket;

#[derive(Serialize, Deserialize, Debug)]
enum Cmd {
    Get(String),
    Del(String),
    Put {
        k: String,
        v: String,
    },
}

fn committed_to_resp(append_resp: AppendResp) -> Result<String, String> {
    match append_resp {
        AppendResp::Appended(i) => Ok(format!("Appended at {}", i.to_string())),
        AppendResp::NotLeader(leader) => match leader {
            None => Err("This server is not the leader".to_owned()),
            Some(leader_id) => Err(format!("This server is not the leader, leader is: {}", leader_id))
        }
        AppendResp::Error(e) => Err(format!("Error: {}", e.to_string()))
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

    fn get(&self, k: i32) -> Result<String, Error> {
        let opts = ReadOptions::new();
        let maybe_val = self.leveldb.get(opts, k)?;
        match maybe_val {
            None => Err(Error::new("Value does not exist".to_owned())),
            Some(bytes) => match str::from_utf8(&bytes) {
                Ok(str) => Ok(str.to_owned()),
                Err(e) => Err(Error::new(e.to_string()))
            }
        }
    }
}

#[get("/<k>")]
fn get(db: State<Arc<Mutex<DB>>>, k: String) -> Result<String, Error> {
    let k = from_u8::<i32>(k.as_bytes());
    db.lock().unwrap().get(k)
}

#[put("/<k>/<v>")]
fn put(db: State<Arc<Mutex<DB>>>, k: String, v: String) -> Result<String, String> {
    let append_resp = db.lock().unwrap().raft_client.append_cmd(Cmd::Put { k, v });
    committed_to_resp(append_resp)
}

#[delete("/<k>")]
fn delete(db: State<Arc<Mutex<DB>>>, k: String) -> Result<String, String> {
    let append_resp = db.lock().unwrap().raft_client.append_cmd(Cmd::Del(k));
    committed_to_resp(append_resp)
}

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let mut hosts = vec![];
    for h in args[1].split(",") {
        hosts.push(h.to_owned())
    }
    println!("Hosts are {:?}", hosts);
    let host = &args[2];
    let config = RaftConfig::mk_config(host, hosts);
    let (cli, receiver) = run::<Cmd>(config);


    let leveldb = DB::new(cli);
    let db = Arc::new(Mutex::new(leveldb));
    let commit_db = db.clone();
    thread::spawn(move || apply_committed(receiver, commit_db));
    rocket::ignite()
        .manage(db)
        .mount("/", routes![get, put, delete])
        .launch();
}

fn apply_committed(receiver: mpsc::Receiver<Committed<Cmd>>, db: Arc<Mutex<DB>>) {
    while let Ok(commited) = receiver.recv() {
        match commited.cmd {
            Cmd::Get(_) => trace!("Get should be handled by webserver"),
            Cmd::Del(k) => {
                let k = from_u8::<i32>(k.as_bytes());
                let opts = WriteOptions::new();
                db.lock().unwrap().leveldb.delete(opts, k).unwrap()
            }
            Cmd::Put { k, v } => {
                let k = from_u8::<i32>(k.as_bytes());
                let opts = WriteOptions::new();
                db.lock().unwrap().leveldb.put(opts, k, v.as_bytes()).unwrap()
            }
        }
    }
}
