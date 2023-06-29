#!/bin/bash

# Default values
tests=""
isfirst=false
host="localhost:6379"


display_usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "Options:"
  echo "  -t, --tests <categories> Specify test categories (go_jobs, python_jobs, rust_jobs, typeScript_jobs, dotnet_jobs). Example use './run-test.sh --tests "go_jobs,python_jobs"'"
  echo "  -h, --host <hostname>    Set the host (default: localhost)"
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


if [[ "$tests" == *"python"* ]]; then
  if [ "$isfirst" = false ]; then
    cd ../
    cd tests
  fi
    python3 python-tests.py "$host" > /tmp/attemp-py3-worker-1.txt &
    sleep 1.45
    python3 python-tests.py "$host" > /tmp/attemp-py3-worker-2.txt &
    sleep 0.9
    isfirst=true
  echo "Python test category is present"
fi

if [[ "$tests" == *"rust"* ]]; then
  if [ "$isfirst" = true ]; then
    cd rust 
  else
    cd ../rust 
  fi
  cargo run -- "$host" > /tmp/attemp-rust-worker-1.txt  &
  sleep 1.3
  cargo run -- "$host" > /tmp/attemp-rust-worker-2.txt  &
  sleep 0.2
  isfirst=true
  echo "Rust test category is present"
  cd ../
fi

if [[ "$tests" == *"go"* ]]; then
  if [ "$isfirst" = true ]; then
    cd go 
  else
    cd ../go 
  fi
  GO_BIN="$(mktemp)"
  go build -o "$GO_BIN" 
  "$GO_BIN" "$host" > /tmp/attemp-go-worker-1.txt & 
  sleep 1.8
  "$GO_BIN" "$host" > /tmp/attemp-go-worker-2.txt &
  sleep 0.5
  rm "$GO_BIN"
  isfirst=true
  echo "Go test category is present"
  cd ../
fi

if [[ "$tests" == *"dotnet"* ]]; then
  if [ "$isfirst" = true ]; then
    cd dotnet/RedisWorkQueueTests 

  else
    cd ../dotnet/RedisWorkQueueTests 
  fi
  dotnet run -c Release "$host" > /tmp/attemp-dotnet-worker-1.txt &
  sleep 1.9
  dotnet run -c Release "$host" > /tmp/attemp-dotnet-worker-2.txt &
  sleep 0.5
  cd ../..
  isfirst=true
  echo "Dotnet test category is present"
fi

if [[ "$tests" == *"typeScript"* ]]; then

  if [ "$isfirst" = true ]; then
    cd typescript 
  else
    cd ../typescript 
  fi
  npm run test "$host" > /tmp/attemp-node-worker-1.txt &
  sleep 1.9
  npm run test "$host" > /tmp/attemp-node-worker-2.txt &
  sleep 0.5
  cd ..
  isfirst=true
  echo "TypeScript test category is present"
fi


python3 job-spawner-and-cleaner.py "$host" "$tests"
