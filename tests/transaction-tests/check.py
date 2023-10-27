import sys
import random
from time import sleep

import redis
from redis import Redis, WatchError

sys.path.insert(0, '../../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue


if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")

host = sys.argv[1].split(":")
db = redis.Redis(host=host[0], port=int(host[1]) if len(host) > 1 else 6379)

python_results_key = KeyPrefix("results:python:")
shared_results_key = KeyPrefix("results:shared:")

python_queue = WorkQueue(KeyPrefix("python_jobs"))
shared_queue = WorkQueue(KeyPrefix("shared_jobs"))

def check_queue(queue: WorkQueue):
    """Ensure a queue has exatly 200 items, with ID 0, 1, 2, ..., 199."""
    remaining = list(range(0, 200))
    for item in db.lrange(queue._main_queue_key, 0, -1):
        remaining.remove(int(item))
    assert len(remaining) == 0

check_queue(python_queue)
check_queue(shared_queue)
