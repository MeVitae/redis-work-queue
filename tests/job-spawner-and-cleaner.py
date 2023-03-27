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
dotnet_queue = WorkQueue(KeyPrefix("dotnet_jobs"))
go_queue = WorkQueue(KeyPrefix("go_jobs"))
shared_queue = WorkQueue(KeyPrefix("shared_jobs"))

counter = 0

while True:
    if counter < 256:
        python_queue.add_item(db, Item(bytes([counter])))
        rust_queue.add_item(db, Item(bytes([counter])))
        go_queue.add_item(db, Item(bytes([counter])))
        dotnet_queue.add_item(db, Item(bytes([counter])))
    elif counter % 2 == 0:
        print((python_queue.queue_len(db), python_queue.processing(db)))
        print((rust_queue.queue_len(db), rust_queue.processing(db)))
        print((go_queue.queue_len(db), go_queue.processing(db)))
        print((dotnet_queue.queue_len(db), dotnet_queue.processing(db)))
        print((shared_queue.queue_len(db), shared_queue.processing(db)))
        sleep(0.5)
    elif counter == 500:
        print("More jobs!!")
        for n in range(0, 255*6+1):
            n = n % 256
            python_queue.add_item(db, Item(bytes([n])))
            rust_queue.add_item(db, Item(bytes([n])))
            go_queue.add_item(db, Item(bytes([n])))
            dotnet_queue.add_item(db, Item(bytes([n])))
    else:
        print("Cleaning")
        python_queue.light_clean(db)
        rust_queue.light_clean(db)
        go_queue.light_clean(db)
        dotnet_queue.light_clean(db)
        shared_queue.light_clean(db)
    counter += 1

