# Redis Work Queue

A work queue, on top of a redis database, with implementations in Python, Rust, Go, Node.js
(TypeScript) and Dotnet (C#).

This provides no method of tracking the outcome of work items. Tracking results is fairly simple to
implement yourself (just store the result in the redis database with a key derived from the work
item id). If you want a more fully-featured system for managing jobs, see our [Collection
Manager](https://github.com/MeVitae/redis-collection-manager).

Implementations in other languages are welcome, open a PR!

## Documentation

In addition to the primary overview below, each implementation has its own examples and API
reference.

- Python: [![PyPI](https://img.shields.io/pypi/v/redis-work-queue)](https://pypi.org/project/redis-work-queue/)
  [![Docs](https://img.shields.io/badge/Docs-blue)](./python/README.md)
  [![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](./python/LICENSE)

- Rust: [![Crates.io](https://img.shields.io/crates/v/redis-work-queue)](https://crates.io/crates/redis-work-queue)
  [![docs.rs](https://img.shields.io/docsrs/redis-work-queue)](https://docs.rs/redis-work-queue)
  [![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

- Go: [![Go Report Card](https://goreportcard.com/badge/github.com/mevitae/redis-work-queue/go)](https://goreportcard.com/report/github.com/mevitae/redis-work-queue/go)
  [![GoDoc](https://pkg.go.dev/badge/github.com/mevitae-redis-work-queue/go)](https://pkg.go.dev/github.com/mevitae/redis-work-queue/go)
  [![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

- DotNet (C#): [![Nuget](https://img.shields.io/nuget/v/MeVitae.RedisWorkQueue)](https://www.nuget.org/packages/MeVitae.RedisWorkQueue/0.1.5)
  [![Docs](https://img.shields.io/badge/Docs-blue)](./dotnet/RedisWorkQueue.pdf)
  [![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

- Node.js (TypeScript): [![NPM](https://img.shields.io/badge/NPM-v0.0.4-red)](https://www.npmjs.com/package/@mevitae/redis-work-queue)
  [![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](./typescript/LICENSE)

## Overview

All the implementations share the same operations, on the same core types, these are:

### Items

Items in the work queue consist of an `id`, a string, and some `data`, arbitrary bytes.

For convenience, the IDs are often randomly generated UUIDs, however they can be customized.

### Adding an item

*Python: `WorkQueue.add_item`,
Rust: [`WorkQueue::add_item`](https://docs.rs/redis-work-queue/latest/redis_work_queue/struct.WorkQueue.html#method.add_item),
Node.js: [`WorkQueue::add_item`](WorkQueue-addItem),
Go: [`WorkQueue.AddItem`](https://pkg.go.dev/github.com/mevitae/redis-work-queue/go#WorkQueue.AddItem)*

Adding an item is exactly what it sounds like! It adds an item to the work queue. It will then
either be in the queue or being processed (before coming back to the queue if the processing fails)
until the job is completed.

#### Adding known unique items (faster)

*Python: `WorkQueue.add_unique_item`,
Rust: [`WorkQueue::add_unique_item`](https://docs.rs/redis-work-queue/latest/redis_work_queue/struct.WorkQueue.html#method.add_unique_item),
Node.js: [`WorkQueue::add_item`](WorkQueue-addItem),
Go: [`WorkQueue.AddUniqueItem`](https://pkg.go.dev/github.com/mevitae/redis-work-queue/go#WorkQueue.AddUniqueItem)*

If you know that an item ID is not already in the queue, you can instead use an optimised
`add_unique_item` method, which skips that exact check.

*If you use this incorrectly, nothing will go too badly wrong, but the reported queue length, which
may be used for autoscaling, will be inaccurate, and leasing items will take multiple iterations.*

#### Using the right add method

The default item constructors set the item ID to a randomly generated UUID (universally *unique*
ID). If this is used, then the `add_unique_item` method should be preferred.

However, if duplicate jobs are likely to be added, then the item IDs should be set such that equal
jobs have equal IDs (for example by using a hash of the job), and then the `add_item` method should
be used, to prevent jobs from being duplicated.

### Leasing an item

*Python: `WorkQueue.lease`,
Rust: [`WorkQueue::lease`](https://docs.rs/redis-work-queue/latest/redis_work_queue/struct.WorkQueue.html#method.lease),
Node.js: [`WorkQueue::lease`](WorkQueue-lease),
Go: [`WorkQueue.Lease`](https://pkg.go.dev/github.com/mevitae/redis-work-queue/go#WorkQueue.Lease)*

Workers wanting to receive a job and complete it must start by obtaining a lease.

When requesting a lease, you exchange an expiry time for an item. The worker should then complete
the item before the expiry time by calling `complete`. If `complete` isn't called in time, it's
assumed that the worker died and the item is returned to the queue for another worker to pick up.

This means that a worker can receive a job that another worker has already partially or fully
completed (and then died before calling `complete`) or even for two workers to be simultaneously
working on the same job if the lease expiry was too short (try to avoid this if possible!). It's
therefore important that workers are written in a way that won't cause problems if a worker starts
again after another worker has already fully or partially completed the task, or is working on it at
the same time. This allows a fully resilient system.

The work queue cannot loose track of a job once it's been added, so, as long as workers keep
successfully working, a job will always be run to completion (even if it is run multiple times in
that process).

If you're unhappy about jobs being run more than once, see [But I never want my job to run more than
once](#but-i-never-want-my-job-to-run-more-than-once).

#### Storing the result of a work item

The work queue provides no method of tracking the outcome of work items. This is fairly simple to
implement yourself (just store the result in the redis database with a key derived from the work
item id). If you want a more fully-featured system for managing jobs, see our [Collection
Manager](https://github.com/MeVitae/redis-collection-manager).

#### Handling errors

If an error occurs and the job should be retried, later on, by the same or different worker, then
the worker should **not** call `complete` and should obtain another lease and work on the next item,
ignoring the one it was previously processing. When the previous lease expires, it will be returned
to the work queue and will be retried. For example:

```python
while True:
    job = work_queue.lease(100)
    # ... do some work ...
    if should_try_again_later:
        # Don't call complete, just get another lease
        continue
    # ... finish the work ...
    work_queue.complete(job)
```

If an error occurs that means the job shouldn't be retried, you should send this error to the
correct place (perhaps the same place you put your results) and then call `complete`. The job then
won't be run again.

#### But I never want my job to run more than once

Before following the instructions below, you should think really hard about the title statement. If
the job can't run more than once then, and the worker dies during the work, the work will be left
incomplete, forever... *and ever...* <small>*and ever...*</small> (unless you have your own error
recovery system)

It's possible to write almost all jobs in a way which allows it to be restarted if a worker node
dies. If you can it's probably worth the effort!

##### I still think I want my job to only ever possibly run once

If this is the case, you should call `complete` (**and check the return value**) immediately
after obtaining the lease.

For example, in Python:
```python
job = queue.lease(1000)
if queue.complete(job):
    # This will only run once, per job, ever, even if the worker dies
    foo(job)
```

This works because `complete` returns `true` *iff* it is the worker that completed the job. So while
`lease` may return the same job many times, `complete(job)` will return `true` only once per job.

### Completing an item

*Python: [`WorkQueue.complete`](#), Node.js: [`WorkQueue.Complete`](#), Rust: [`WorkQueue::complete`](#), Go: [`WorkQueue.Complete`](#)*

Complete marks a job as completed and remove it from the work queue. After `complete` has been called
(and returns `true`), no workers will receive this job again.

`complete` returns a boolean indicating if *the job has been removed* **and** *this worker was the
first worker to call `complete`*. So, while lease might give the same job to multiple workers,
complete will return `true` for only one worker.

#### Storing the result

See [Storing the result of a work item](#storing-the-result-of-a-work-item)

### Cleaning

When workers fail to complete items, or if they fail in the middle of redis operations, they can
leave the queue in a state that requires cleaning to ensure items are completed. Because of this,
cleans should occur periodically.

The frequency and schedule of these is entirely up to you. Light cleans are quick, and can be
carried out regularly. Deep cleans get get very slow depending on the size of your queues, and so
should be performed less often, but should be performed to clean up cases where workers or cleaners
have unexpectedly terminated in the middle of redis operations.

#### Light cleaning

*Python: `WorkQueue.light_clean`, Rust implementation planned, no Go or C# implementation planned*

The interval *light cleaning* should be run on should be approximately equal to the shortest lease
time you use.

#### Deep cleaning

*Python and Rust implementations planned, no Go or C# implementation planned*

It's very rare that deep cleaning is needed, but it can happen if you get really unlucky, so it
should be run automatically but less frequently, depending on your requirements for guaranteed
completion times.

#### Cleaning process

When there are many workers of different types, it's simpler just to have a dedicated process
running the cleaning.

### Other operations

#### Getting the queue length

*Python: `WorkQueue.queue_len`,
Rust: [`WorkQueue::queue_len`](https://docs.rs/redis-work-queue/latest/redis_work_queue/struct.WorkQueue.html#method.queue_len),
Go: [`WorkQueue.QueueLen`](https://pkg.go.dev/github.com/mevitae/redis-work-queue/go#WorkQueue.QueueLen),
Node.js: [`WorkQueue.queueLen`](WorkQueue.queueLen-link)*

#### Getting the number of leased items

*Python: `WorkQueue.processing`,
Rust: [`WorkQueue::processing`](https://docs.rs/redis-work-queue/latest/redis_work_queue/struct.WorkQueue.html#method.processing),
Node.js: [`WorkQueue.processing`](WorkQueue.processing),
Go: [`WorkQueue.QueueLen`](https://pkg.go.dev/github.com/mevitae/redis-work-queue/go#WorkQueue.Processing)*

This includes items being worked on and abandoned items (see [Handling errors](#handling-errors)) yet to be
returned to the *main queue*.

## Testing

The client implementations each have their own (very simple) unit tests. Most of the testing is done
through the integrations tests, located in the [tests](./tests/) directory.

## Technical details

A queue is identified by its key prefix.

Each queue item has a unique ID, and has its own *data key*, which is `<prefix>:item:<item_id>`. The
item data is stored with this key. If this key exists, then the item is considered incomplete. If
the item data key does not exist, the item is considered completed.

The work queue then has a pair of lists, the *main queue* (`<prefix>:queue`) and *processing queue*
(`<prefix>:processing`), to track these items. However, if an item ends up in none of these queues
(which can happen if operations aren't properly completed), it is still considered an item. The
*deep clean* process fixes cases like this periodically.

An item in a queue list, but without a data key, isn't considered an item, so should be ignored and
removed from the queues when it's encountered.

More specifically, the *main queue* holds the list of item IDs which are yet to be processed. New
items are pushed to the left of the list, and leased items are popped from the right.

### Adding an item

To add an item, you must:

- Store the item data to the item data key (`<prefix>:item:<item_id>`), then
- Push the item to the left of the *main queue*.
  - The push should be done second, to prevent waiting workers from popping the item immediately,
    before the data key is set.

If the item's data key already exists, you shouldn't push it to the *main queue* again, since this
will cause it to be counted twice when getting the queue length, which can have negative impact on
queue-length based autoscaling. Furthermore, when the item is completed, it's copy will still be in
the *main queue*, so leasing will take more iterations.

If the ID is already known to be unique (for example UUIDs), you can safely pipeline these
operations and skip the check.

### Leasing an item

The fetch an item to work on, a worker should pop from the right of the *main queue*, and push to the
left of the *processing queue* (`rpoplpush`).

The *processing queue* is a method used to track the items currently being processed, to make the
cleaning process more efficient.

Furthermore, while an item is being processed, it has a *lease key*, which is
`<prefix>:lease:<item_id>`. The value of this is the session ID of the worker which got the lease.
Lease keys are set with an expiry, once the key has expired (or is otherwise deleted), the session
is deemed to have failed working on the item, and the item will be added, by the cleaning process,
to the end of the *main queue*.

The lease function should therefore:

- `rpoplpush` from the *main queue* to the processing queue,
- Load the data for this item
  - and, if it doesn't exist, ignore the item,
  - *(in our provided client implementations, if lease is requested to block, and there's no
    timeout, the lease method just tries to get the next item. In any other case, the method returns
    with no item)*, then
- set the *lease key*, with an expiry time longer than the expected job duration.

### Completing an item

When completing an item, you must:
- Remove the data key.

You should also:
- Remove the *lease key*, and
- Remove the item ID from the *processing queue*.

The item is considered to be removed exactly when the data key is removed, but the other steps keep
things tidy (without removing the lease, it would eventually expire, and the cleaning process would
later remove the item ID from the processing queue anyway).

The completion methods also return a boolean, for which only one remove call must return true. This
boolean can be decided by the output of the command to delete the data key. If it deletes the item,
this is the call that completed the item. Otherwise, another worker has already completed it.

### Cleaning

Items are considered items only when their data key exists.

If a lease does not exist for the item, then processing must have failed before the item was
completed, and the item should be available again for a lease.

The cleaning process finds items:

- That exist,
- Aren't in the *main queue*, and
- Don't have a lease.

And then:

- Removes them from the processing queue, and
- Pushes them to the *main queue*.

The item should be removed from the processing queue first.

#### Deep clean

The deep cleaning process is complete. It uses `keys` to enumerate all the data items for checking.

#### Light clean

Using `keys` can cause significant performance issues, so should ideally be avoided.

This is why we have the *processing queue*. So long as all operations complete fully, any item with
an expired lease will be in the processing queue, we can therefore follow the usual cleaning
algorithm, but instead only use the item IDs from the *processing queue*.

#### Cleaning schedule

This is entirely up to you. Light cleans are quick, and can be carried out regularly. Deep cleans
get get very slow depending on the size of your queues, and so should be performed less often, but
should be performed to clean up cases where workers or cleaners have unexpectedly terminated in the
middle of redis operations.

*Light cleaning* should be run on should be approximately equal to the shortest lease time you use.
