import sys
from time import sleep

import redis

sys.path.append('../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue

if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")
db = redis.Redis(host=sys.argv[1])

python_queue = WorkQueue(KeyPrefix("python_jobs"))
rust_queue = WorkQueue(KeyPrefix("rust_jobs"))
go_queue = WorkQueue(KeyPrefix("go_jobs"))
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
    elif counter % 2 == 0:
        # Every other tick just log how much work is left
        print((python_queue.queue_len(db), python_queue.processing(db)))
        print((rust_queue.queue_len(db), rust_queue.processing(db)))
        print((go_queue.queue_len(db), go_queue.processing(db)))
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
    elif doom_counter > 10 and not revived:
        # After everything settles down, add more jobs
        print("Even more jobs!!")
        revived = True
        for n in range(0, 256):
            python_queue.add_item(db, Item(bytes([n])))
            rust_queue.add_item(db, Item(bytes([n])))
            go_queue.add_item(db, Item(bytes([n])))
    else:
        # Otherwise, clean!
        print("Cleaning")
        python_queue.light_clean(db)
        rust_queue.light_clean(db)
        go_queue.light_clean(db)
        shared_queue.light_clean(db)
    # The `doom_counter` counts the number of consecutive times all the lengths are 0.
    if python_queue.queue_len(db) == 0 and python_queue.processing(db) == 0 and \
        rust_queue.queue_len(db) == 0 and rust_queue.processing(db) == 0 and \
        go_queue.queue_len(db) == 0 and go_queue.processing(db) == 0 and \
        shared_queue.queue_len(db) == 0 and shared_queue.processing(db) == 0:
            doom_counter += 1
    else:
        doom_counter = 0
    counter += 1

# These are the results are still expecting, when a result is found, it's removed from these lists.
expecting_python = [(n * 3)%256 for n in range(0, 256*3)]
expecting_rust = [(n * 7)%256 for n in range(0, 256*3)]
expecting_go = [(n * 5)%256 for n in range(0, 256*3)]

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
    elif key.find('results:shared:') == 0:
        #print('shared results')
        # TODO: check these results
        pass
    else:
        raise Exception('found unexpected key: ' + key)

assert len(expecting_python) == 0
assert len(expecting_rust) == 0
assert len(expecting_go) == 0
