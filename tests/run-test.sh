#!/bin/bash

# Spawn 2 rust workers
cd rust
cargo run -- localhost > /tmp/attemp-rust-worker-1.txt  &
sleep 1.3
cargo run -- localhost > /tmp/attemp-rust-worker-2.txt  &
sleep 0.2

# Spawn 2 go workers
cd ../go
GO_BIN="$(mktemp)"
go build -o "$GO_BIN" 
"$GO_BIN" localhost:6379 > /tmp/attemp-go-worker-1.txt & 
sleep 1.8
"$GO_BIN" localhost:6379 > /tmp/attemp-go-worker-2.txt &
sleep 0.5
rm "$GO_BIN"

# Spawn 2 C# DotNet workers
cd ../dotnet/RedisWorkQueueTests
dotnet run -c Release localhost > /tmp/attemp-dotnet-worker-1.txt &
sleep 1.9
dotnet run -c Release localhost > /tmp/attemp-dotnet-worker-2.txt &
sleep 0.5
cd ..

# Spawn 2 ts workers
cd ../typescript
npm run test localhost > /tmp/attemp-node-worker-1.txt &
sleep 1.9
npm run test localhost > /tmp/attemp-node-worker-2.txt &
sleep 0.5

# Spawn 2 python workers
cd ../
python3 python-tests.py localhost > /tmp/attemp-py3-worker-1.txt &
sleep 1.45
python3 python-tests.py localhost > /tmp/attemp-py3-worker-2.txt &
sleep 0.9

# Run the script to spawn jobs and check the results
python3 job-spawner-and-cleaner.py localhost

# Kill everything at the end
pkill -P $$
