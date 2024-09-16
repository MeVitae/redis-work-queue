#!/bin/bash
set -e

echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
./run-test.sh -t go_jobs,python_jobs,rust_jobs,node_jobs,dotnet_jobs
kill $REDIS_PID
wait $REDIS_PID

echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
./run-test.sh -t go_jobs,python_jobs,rust_jobs,node_jobs,dotnet_jobs -c ./go-cleaner/run.sh
kill $REDIS_PID
wait $REDIS_PID

echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
./run-test.sh -t go_jobs,python_jobs,rust_jobs,node_jobs,dotnet_jobs -c ./dotnet-cleaner/run.sh
kill $REDIS_PID
wait $REDIS_PID

cd ./add-item-tests
./run.sh
