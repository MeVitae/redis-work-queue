# RedisWorkQueue: Dotnet Implementation

A work queue, on top of a redis database, with implementations in Python, Rust, Go, Node.js
(TypeScript) and Dotnet (C#).

This is the Dotnet implementations. For an overview of how the work queue works, it's limitations,
and the general concepts and implementations in other languages, please read the [redis-work-queue
readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md).

## Documentation

Below is a brief example. More details on the core concepts can be found in the
[readme](https://github.com/MeVitae/redis-work-queue/blob/main/README.md), and
full API documentation can be found in
[../RedisWorkQueue.pdf](https://github.com/MeVitae/redis-work-queue/blob/main/dotnet/RedisWorkQueue.pdf).

## Example Usage

```csharp
using FreeRedis;
using RedisWorkQueue;

var redis = new RedisClient("localhost");
var workQueue = new WorkQueue("work_queue");

var item = new Item(Encoding.UTF8.GetBytes("data"), "item_1");

workQueue.AddItem(redis, item);

var queueLength = workQueue.QueueLength(redis);
Console.WriteLine($"Queue Length: {queueLength}");

var lease = workQueue.Lease(redis, 30, true, 10);
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
pass a `timeout` value of `10` seconds, which means that if there are no items available after 10
seconds, the method will return `null`.
