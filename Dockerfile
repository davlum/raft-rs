FROM rust:1.49.0

RUN apt-get update && apt-get install -y cmake libleveldb-dev libsnappy-dev && rm -rf /var/lib/apt/lists/*

ENV RUST_LOG info

COPY . .

RUN cargo install --example dist_leveldb --path .
