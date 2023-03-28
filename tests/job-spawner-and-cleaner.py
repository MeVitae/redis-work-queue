import sys
import json
from time import sleep

import redis

sys.path.append('../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue

if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")
db = redis.Redis(host=sys.argv[1])

if len(db.keys("*")) > 0:
    raise Exception("redis database isn't clean")

python_queue = WorkQueue(KeyPrefix("python_jobs"))
rust_queue = WorkQueue(KeyPrefix("rust_jobs"))
go_queue = WorkQueue(KeyPrefix("go_jobs"))
dotnet_queue = WorkQueue(KeyPrefix("dotnet_jobs"))
shared_queue = WorkQueue(KeyPrefix("shared_jobs"))

counter = 0
doom_counter = 0
revived = False

while doom_counter < 20:
    if counter < 256:
        # Spawn 256 initial jobs in each queue
        python_queue.add_item(db, Item(bytes([counter])))
        rust_queue.add_item(db, Item(bytes([counter])))
        go_queue.add_item(db, Item(bytes([counter])))
        dotnet_queue.add_item(db, Item(bytes([counter])))
    elif counter % 2 == 0:
        # Every other tick just log how much work is left
        print((python_queue.queue_len(db), python_queue.processing(db)))
        print((rust_queue.queue_len(db), rust_queue.processing(db)))
        print((go_queue.queue_len(db), go_queue.processing(db)))
        print((dotnet_queue.queue_len(db), dotnet_queue.processing(db)))
        print((shared_queue.queue_len(db), shared_queue.processing(db)))
        sleep(0.5)
    elif counter == 501:
        # After a little bit, add more jobs.
        print("More jobs!!")
        for n in range(0, 256):
            n = n % 256
            python_queue.add_item(db, Item(bytes([n])))
            rust_queue.add_item(db, Item(bytes([n])))
            go_queue.add_item(db, Item(bytes([n])))
            dotnet_queue.add_item(db, Item(bytes([n])))
    elif doom_counter > 10 and not revived:
        # After everything settles down, add more jobs
        print("Even more jobs!!")
        revived = True
        for n in range(0, 256):
            python_queue.add_item(db, Item(bytes([n])))
            rust_queue.add_item(db, Item(bytes([n])))
            go_queue.add_item(db, Item(bytes([n])))
            dotnet_queue.add_item(db, Item(bytes([n])))
    else:
        # Otherwise, clean!
        print("Cleaning")
        python_queue.light_clean(db)
        rust_queue.light_clean(db)
        go_queue.light_clean(db)
        dotnet_queue.light_clean(db)
        shared_queue.light_clean(db)
    # The `doom_counter` counts the number of consecutive times all the lengths are 0.
    if python_queue.queue_len(db) == 0 and python_queue.processing(db) == 0 and \
        rust_queue.queue_len(db) == 0 and rust_queue.processing(db) == 0 and \
        go_queue.queue_len(db) == 0 and go_queue.processing(db) == 0 and \
        dotnet_queue.queue_len(db) == 0 and dotnet_queue.processing(db) == 0 and \
        shared_queue.queue_len(db) == 0 and shared_queue.processing(db) == 0:
            doom_counter += 1
    else:
        doom_counter = 0
    counter += 1

# These are the results are still expecting, when a result is found, it's removed from these lists.
expecting_python = [(n * 3)%256 for n in range(0, 256*3)]
expecting_rust = [(n * 7)%256 for n in range(0, 256*3)]
expecting_go = [(n * 5)%256 for n in range(0, 256*3)]
expecting_dotnet = [(n * 11)%256 for n in range(0, 256*3)]

expecting_shared = [(a+b, a*b) for a in [3, 5] for b in expecting_rust] + \
        [(a+b, a*b) for a in [7, 11] for b in expecting_go] + \
        [(a+b, a*b) for a in [13, 17] for b in expecting_python] + \
        [(a+b, a*b) for a in [19, 23] for b in expecting_dotnet]

shared_counts = {}

for key in db.keys("*"):
    key = key.decode('utf-8')
    if key.find('results:python:') == 0:
        results = db.get(key)
        assert results is not None
        assert len(results) == 1
        expecting_python.remove(results[0])
    elif key.find('results:rust:') == 0:
        results = db.get(key)
        assert results is not None
        assert len(results) == 1
        expecting_rust.remove(results[0])
    elif key.find('results:go:') == 0:
        results = db.get(key)
        assert results is not None
        assert len(results) == 1
        expecting_go.remove(results[0])
    elif key.find('results:dotnet:') == 0:
        results = db.get(key)
        assert results is not None
        assert len(results) == 1
        expecting_dotnet.remove(results[0])
    elif key.find('results:shared:') == 0:
        result = db.get(key)
        assert result is not None
        if isinstance(result, bytes):
            result = result.decode('utf-8')
        result = json.loads(result)
        worker = result['worker']
        if worker in shared_counts:
            shared_counts[worker] += 1
        else:
            shared_counts[worker] = 1
        expecting_shared.remove((result['sum'], result['prod']))
        pass
    else:
        raise Exception('found unexpected key: ' + key)

print(shared_counts)

for key in shared_counts.keys():
    assert key in ['python', 'rust', 'go', 'dotnet']
    # Check that it's fairly well balanced
    assert shared_counts[key] < 1900

assert len(expecting_python) == 0
assert len(expecting_rust) == 0
assert len(expecting_go) == 0
assert len(expecting_dotnet) == 0
assert len(expecting_shared) == 0
