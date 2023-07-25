A work queue, on top of a redis database, with implementations in Python, Rust, Go, Node.js
(TypeScript) and Dotnet (C#).

This is the Python implementations. For an overview of how the work queue works, it's limitations,
and the general concepts and implementations in other languages, please read the [redis-work-queue
readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md).

## Setup

```typescript
import Redis from 'ioredis';
const {KeyPrefix, WorkQueue} = require('@mevitae/redis-work-queue');

const db: Redis = new Redis(redisHost);

const work_queue = new WorkQueue(new KeyPrefix("example_work_queue"));
```

## Adding work

### Creating `Item`s

```typescript
const {Item} = require('@mevitae/redis-work-queue');

// Create an item from bytes
const item1Buffer: Buffer = Buffer.from('[1,2,3]', 'utf-8');
const bytesItem: Item = new Item(item1Buffer);

// Create an item from a string
const stringItem: Item = new Item('[1,2,3]');

// Create an item by JSON serializing an object
const jsonItem: Item = Item.fromJsonData([1, 2, 3]);

// Retrieve an item's data, as bytes:
console.assert(value === 42, 'Value should be 42');
console.assert(
  bytesItem.data.equals(Buffer.from('[1,2,3]', 'utf-8')),
  "'bytesItem.data' should be the same as Buffer.from('[1,2,3]', 'utf-8')."
);
console.assert(
  bytesItem.data.equals(stringItem.data),
  "'bytesItem.data' should be the same as 'stringItem.data'"
);
console.assert(
  bytesItem.data.equals(jsonItem.data),
  "'bytesItem.data' should be the same as 'jsonItem.data'"
);
```

### Add an item to a work queue

```typescript
WorkQueue.addItem(db, item)
```

## Completing work

Please read [the documentation on leasing and completing
items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).

```typescript
const Redis = require('ioredis');
const {KeyPrefix, WorkQueue} = require('@mevitae/redis-work-queue');

const db: Redis = new Redis(redisHost);
const workQueue: WorkQueue = new WorkQueue(new KeyPrefix('Queue-name'));

while (true) {
  // Wait for a job with no timeout and a lease time of 5 seconds.
  // Use WorkQueue.lease(db, 5, 10) to timeout and return `None` after 10 seconds.
  const job: Item | None = workQueue.lease(db, 5);

  await doTheJob(job);
  workQueue.complete(db, job);
}
```

### Handling errors

Please read [the documentation on handling
errors](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#handling-errors).

```typescript
const {Item, WorkQueue,KeyPrefix} = require('@mevitae/redis-work-queue');

const workQueue: WorkQueue = new WorkQueue(new KeyPrefix("Queue-name"));

while (true) {
  // Wait for a job with no timeout and a lease time of 5 seconds.
  const job: Item = workQueue.lease(db, 5);
  try {
    doSomeWOrk(job);
  } catch (err) {
    if (shouldRetry(err)) {
      // Drop a job that should be retried - it will be returned to the work queue after
      //the (5 second) lease expires.
      continue;
    } else {
      // Errors that shouldn't cause a retry should mark the job as complete so it isn't
      // tried again.
      logError(err);
      workQueue.complete(db, &job);
    }
  }
  // Mark successful jobs as complete
  workQueue.complete(db, job);
}
```

### Creating Docs

To generate the documentation, run:

```bash
npx typedoc --entryPoints ./src/WorkQueue.ts ./src/Item.ts ./src/KeyPrefix.ts
```

