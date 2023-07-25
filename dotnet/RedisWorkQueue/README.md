# RedisWorkQueue: Dotnet Implementation

A work queue, on top of a redis database, with implementations in Python, Rust, Go, Node.js
(TypeScript) and Dotnet (C#).

This is the Dotnet implementations. For an overview of how the work queue works, it's limitations,
and the general concepts and implementations in other languages, please read the [redis-work-queue
readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md).

## Documentation

Below is a brief overview and an example. More details on the core concepts can be found in the
[readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md), and full API
documentation can be found in
[../RedisWorkQueue.pdf](https://github.com/MeVitae/redis-work-queue/blob/main/dotnet/RedisWorkQueue.pdf).

## WorkQueue

The `WorkQueue` class represents a work queue backed by a Redis database. It provides methods to add
items to the queue, lease items from the queue, and mark completed items as done.

### Properties

- `Session`: Gets or sets the unique identifier for the current session.

### Methods

- `AddItem(IRedisClient db, Item item)`: Adds an item to the work queue. The `db` parameter is the
  Redis instance and the `item` parameter is the item to be added.

- `QueueLength(IRedisClient db)`: Gets the length of the main queue. The `db` parameter is the Redis
  instance.

- `Processing(IRedisClient db)`: Gets the length of the processing queue. The `db` parameter is the
  Redis instance.

- `LeaseExists(IRedisClient db, string itemId)`: Checks if a lease exists for the specified item ID.
  The `db` parameter is the Redis instance and the `itemId` parameter is the ID of the item to
  check.

- `Lease(IRedisClient db, int leaseSeconds, bool block, int timeout = 0)`: Requests a work lease
  from the work queue. This should be called by a worker to get work to complete. The `db` parameter
  is the Redis instance, the `leaseSeconds` parameter is the number of seconds to lease the item for,
  the `block` parameter indicates whether to block and wait for an item to be available if the main
  queue is empty, and the `timeout` parameter is the maximum time to block in milliseconds.

- `Complete(IRedisClient db, Item item)`: Marks a job as completed and removes it from the work
  queue. The `db` parameter is the Redis instance and the `item` parameter is the item to be
  completed.

### Example Usage

```csharp
using FreeRedis;
using RedisWorkQueue;

var redis = new RedisClient("localhost");
var workQueue = new WorkQueue("work_queue");

var item = new Item(Encoding.UTF8.GetBytes("data"), "item_1");

workQueue.AddItem(redis, item);

var queueLength = workQueue.QueueLength(redis);
Console.WriteLine($"Queue Length: {queueLength}");

var lease = workQueue.Lease(redis, 30, true, 10000);
if (lease != null)
{
    Console.WriteLine($"Leased Item: {lease.ID}");
    // Do the work here
    workQueue.Complete(redis, lease);
}
else
{
    Console.WriteLine("No item available to lease");
}
```

In this example, we create a Redis client and a new instance of the `WorkQueue` class. We then add
an item to the work queue using the `AddItem` method and get the length of the main queue using the
`QueueLength` method.

We then try to lease an item from the work queue using the `Lease` method. If an item is available,
we do the work and mark the item as completed using the `Complete` method.

Note that in this example, we pass `true` for the `block` parameter of the `Lease` method, which
means the method will block and wait for an item to be available if the main queue is empty. We also
pass a `timeout` value of `10000` milliseconds, which means that if there are no items available
after 10 seconds, the method will return `null`.
