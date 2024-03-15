#!/bin/bash
set -e

# Default values
tests=""
host="localhost:6379"
cleaner=""


display_usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "Options:"
  echo "  -t, --tests <categories> Specify test categories (go_jobs, python_jobs, rust_jobs, node_jobs, dotnet_jobs). Example use './run-test.sh --tests "go_jobs,python_jobs"'"
  echo "  -h, --host <hostname>    Set the host (default: localhost:6379)"
  echo "  -c, --cleaner <binary>   Run <binary> <host> as the cleaner binary, see the docs in job-spawner-and-cleaner.py"
  echo "  -h, --help               Display this help message"
}


# Process command-line arguments
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -t|--tests)
      tests="${2//,/ }"
      shift
      shift
      ;;
    -h|--host)
      host="$2"
      shift
      shift
      ;;
    -c|--cleaner)
      cleaner="$2"
      shift
      shift
      ;;
    -h|--help)
      display_usage
      exit 0
      ;;
    *)
      echo "Invalid option: $key" >&2
      display_usage
      exit 1
      ;;
  esac
done


mkdir -p /tmp/redis-work-queue-test-logs


if [[ "$tests" == *"python"* ]]; then
    echo "Starting python workers..."
    python3 python-tests.py "$host" > /tmp/redis-work-queue-test-logs/py3-worker-1.txt &
    sleep 1.45
    python3 python-tests.py "$host" > /tmp/redis-work-queue-test-logs/py3-worker-2.txt &
    sleep 0.9
fi

if [[ "$tests" == *"rust"* ]]; then
    cd rust
    echo "Building rust workers..."
    cargo build
    echo "Starting rust workers..."
    cargo run -- "$host" > /tmp/redis-work-queue-test-logs/rust-worker-1.txt  &
    sleep 1.3
    cargo run -- "$host" > /tmp/redis-work-queue-test-logs/rust-worker-2.txt  &
    sleep 0.2
    cd ..
fi

if [[ "$tests" == *"go"* ]]; then
    cd go
    GO_BIN="$(mktemp)"
    echo "Building Go worker to $GO_BIN..."
    go build -o "$GO_BIN"
    echo "Starting Go workers..."
    "$GO_BIN" "$host" > /tmp/redis-work-queue-test-logs/go-worker-1.txt &
    sleep 1.8
    "$GO_BIN" "$host" > /tmp/redis-work-queue-test-logs/go-worker-2.txt &
    sleep 0.5
    rm "$GO_BIN"
    cd ..
fi

if [[ "$tests" == *"dotnet"* ]]; then
    cd dotnet/RedisWorkQueueTests
    echo "Building DotNet workers..."
    dotnet build -c Release
    echo "Running DotNet workers..."
    dotnet run -c Release "$host" > /tmp/redis-work-queue-test-logs/dotnet-worker-1.txt &
    sleep 1.9
    dotnet run -c Release "$host" > /tmp/redis-work-queue-test-logs/dotnet-worker-2.txt &
    sleep 0.5
    cd ../..
fi

if [[ "$tests" == *"node"* ]]; then
    echo "Installing Node.js dependencies"
    cd ../node
    npm install
    cd ../tests/node
    npm ci
    echo "Running Node.js workers..."
    npm run test "$host" > /tmp/redis-work-queue-test-logs/node-worker-1.txt &
    sleep 1.9
    npm run test "$host" > /tmp/redis-work-queue-test-logs/node-worker-2.txt &
    sleep 0.5
    cd ..
fi

echo "Running spawner..."
python3 job-spawner-and-cleaner.py "$host" "$tests" "$cleaner"
