FROM rust:1.49.0 as builder

RUN rustup default nightly

RUN apt-get update && apt-get install -y cmake libleveldb-dev libsnappy-dev && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo install --example dist_leveldb --path .
