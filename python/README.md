A work queue, on top of a redis database, with implementations in Python, Rust, Go, Node.js
(TypeScript) and Dotnet (C#).

This is the Python implementations. For an overview of how the work queue works, it's limitations,
and the general concepts and implementations in other languages, please read the [redis-work-queue
readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md).

## Setup

```python
from redis import Redis
from redis_work_queue import KeyPrefix, WorkQueue

db = Redis(host='your-redis-server')

work_queue = WorkQueue(KeyPrefix("example_work_queue"));
```

## Adding work

### Creating `Item`s

```python
from redis_work_queue import Item

# Create an item from bytes
bytes_item = Item(b"[1,2,3]")

# Create an item from a string
string_item = Item('[1,2,3]');

# Create an item by json serializing an object
json_item = Item.from_json_data([1, 2, 3])

# Retreive an item's data, as bytes:
assert bytes_item.data() == b"[1,2,3]"
assert bytes_item.data() == string_item.data()
assert bytes_item.data() == json_item.data()

# Parse an item's data as JSON:
assert bytes_item.data_json() == [1, 2, 3]
```

### Add an item to a work queue
```python
work_queue.add_item(db, item)
```

## Completing work

Please read [the documentation on leasing and completing
items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).

```python
from redis_work_queue import Item

while True:
    # Wait for a job with no timeout and a lease time of 5 seconds.
    # Use work_queue.lease(db, 5, timeout=10) to timeout and return `None` after 10 seconds.
    job: Item | None = work_queue.lease(db, 5)
    assert job is not None
    do_some_work(job)
    work_queue.complete(db, job)
```

### Handling errors

Please read [the documentation on handling
errors](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#handling-errors).

```python
from redis_work_queue import Item

while True:
    # Wait for a job with no timeout and a lease time of 5 seconds.
    job: Item = work_queue.lease(db, 5)
    try:
        do_some_work(job)
    except Exception as err:
        if should_retry(err):
            # Drop a job that should be retried - it will be returned to the work queue after
            # the (5 second) lease expires.
            pass
        else:
            # Errors that shouldn't cause a retry should mark the job as complete so it isn't
            # tried again.
            log_error(err)
            work_queue.complete(db, &job)
        continue
    # Mark successful jobs as complete
    work_queue.complete(db, job)
```
