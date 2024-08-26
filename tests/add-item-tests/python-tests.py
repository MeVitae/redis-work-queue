from abc import ABC, abstractmethod
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


class ItemAdder(ABC):
    """An abstract class containing the `add_item` method to add an item to a work queue.

    After running some tests, the `check` method should be called to ensure that all cases within `add_item` actually occurred.
Before running another set of tests, `reset` should be called."""
    @abstractmethod
    def add_item(self, queue: WorkQueue, db: Redis, item: Item) -> bool:
        ...

    @abstractmethod
    def check(self) -> bool:
        ...

    @abstractmethod
    def reset(self):
        ...


class AddUniqueItem(ItemAdder):
    """An ItemAdder using the `WorkQueue.add_unique_item` method, with no checks."""

    def __init__(self):
        pass

    def add_item(self, queue: WorkQueue, db: Redis, item: Item) -> bool:
        queue.add_unique_item(db, item)
        return True

    def check(self) -> bool:
        return True

    def reset(self):
        pass


class AddItem(ItemAdder):
    """An ItemAdder using the default `WorkQueue.add_item` method, which checks
    if, at some point during the test, `add_new_item` has returned both `True`
    and `False`."""

    def __init__(self):
        self.reset()

    def add_item(self, queue: WorkQueue, db: Redis, item: Item) -> bool:
        if queue.add_item(db, item):
            self.seen_true = True
            return True
        else:
            self.seen_false = True
            return False

    def check(self) -> bool:
        return self.seen_true and self.seen_false

    def reset(self):
        self.seen_true = False
        self.seen_false = False


# Decide on the adder implementation to use, from the command line arguments.
if len(sys.argv) > 2 and sys.argv[2] == "--add-item":
    adder = AddItem()
elif len(sys.argv) > 2 and sys.argv[2] == "--add-unique-item":
    adder = AddUniqueItem()
else:
    raise Exception(
        "second argument should be `--add-item` or `--add-unique-item`"
    )

python_queue = WorkQueue(KeyPrefix("python_jobs"))
shared_queue = WorkQueue(KeyPrefix("shared_jobs"))

# Add 100 unique jobs to the python queue:
for idx in range(100, 200):
    id = str(idx)
    if adder.add_item(python_queue, db, Item(str(idx), id)) and idx == 150:
        # If we're ahead at item 150, sleep so we end up behind
        sleep(10)

assert adder.check()
adder.reset()

# Add 100 unique jobs to the shared queue:
for idx in range(100, 200):
    id = str(idx)
    if adder.add_item(shared_queue, db, Item(str(idx), id)) and idx == 150:
        # If we're ahead at item 150, sleep so we end up behind (this sleep is intentionally longer
        # than the last one)
        sleep(20)

assert adder.check()
adder.reset()

# Add 100 jobs, each 10 times, to both queues:
for idx in range(0, 1000):
    id = str(idx//10)
    if adder.add_item(python_queue, db, Item(str(idx), id)) and idx == 500:
        sleep(30)
    adder.add_item(shared_queue, db, Item(str(idx), id))

assert adder.check()
adder.reset()
