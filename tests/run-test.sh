#!/bin/bash

# Spawn 2 rust workers
cd rust
cargo run -- localhost &
sleep 1.3
cargo run -- localhost &
sleep 0.2

# Spawn 2 go workers
cd ../go
GO_BIN="$(mktemp)"
go build -o "$GO_BIN"
"$GO_BIN" localhost:6379 &
sleep 1.8
"$GO_BIN" localhost:6379 &
sleep 0.5
rm "$GO_BIN"

# Spawn 2 python workers
cd ..
python3 python-tests.py localhost &
sleep 1.45
python3 python-tests.py localhost &
sleep 0.9

# Run the script to spawn jobs and check the results
python3 job-spawner-and-cleaner.py localhost

# Kill everything at the end
pkill -P $$
