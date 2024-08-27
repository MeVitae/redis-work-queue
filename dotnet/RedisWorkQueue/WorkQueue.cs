using System;
using System.Text;
using System.Linq;

using FreeRedis;

namespace RedisWorkQueue
{
    /// <summary>
    /// A work queue backed by a redis database.
    /// </summary>
    public class WorkQueue
    {
        /// <summary>
        /// Gets or sets the unique identifier for the current session.
        /// </summary>
        public string Session { get; set; }

        /// <summary>
        /// Gets or sets the Redis key for the main queue.
        /// </summary>
        private string MainQueueKey { get; set; }

        /// <summary>
        /// Gets or sets the Redis key for the processing queue.
        /// </summary>
        private string ProcessingKey { get; set; }

        /// <summary>
        /// Gets or sets the key prefix for lease keys.
        /// </summary>
        private KeyPrefix LeaseKey { get; set; }

        /// <summary>
        /// Gets or sets the key prefix for data keys.
        /// </summary>
        private KeyPrefix ItemDataKey { get; set; }

        /// <summary>
        /// Creates a new instance of the WorkQueue class with based on name given name.
        /// </summary>
        /// <param name="name">The key prefix for the work queue.</param>
        public WorkQueue(KeyPrefix name)
        {
            this.Session = name.Of(Guid.NewGuid().ToString());
            this.MainQueueKey = name.Of(":queue");
            this.ProcessingKey = name.Of(":processing");
            this.LeaseKey = name.Concat(":lease:");
            this.ItemDataKey = name.Concat(":item:");
        }

        /// <summary>
        /// Add an item to the work queue.
        ///
        /// If an item with the same ID already exists, this item is not added, and `false` is returned. Otherwise, if the item is added `true` is returned.
        ///
        /// If you know the item ID is unique, and not already in the queue, use the optimised WorkQueue.AddUniqueItem instead.
        /// </summary>
        /// <param name="db">Redis instance.</param>
        /// <param name="item">Item to be added.</param>
        public bool AddItem(IRedisClient db, Item item)
        {
            if (db.SetNx(ItemDataKey.Of(item.ID), item.Data))
            {
                db.LPush(MainQueueKey, item.ID);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Add an item, which is known to have an ID not already in the queue.
        /// </summary>
        /// <param name="db">Redis instance.</param>
        /// <param name="item">Item to be added.</param>
        public void AddUniqueItem(IRedisClient db, Item item)
        {
            using var pipe = db.StartPipe();
            pipe.Set(ItemDataKey.Of(item.ID), item.Data);
            pipe.LPush(MainQueueKey, item.ID);
            pipe.EndPipe();
        }

        /// <summary>
        /// Gets the length of the main queue.
        /// </summary>
        /// <param name="db">Redis instance.</param>
        /// <returns>The length of the main queue.</returns>
        public long QueueLength(IRedisClient db)
        {
            return db.LLen(MainQueueKey);
        }

        /// <summary>
        /// Gets the length of the processing queue.
        /// </summary>
        /// <param name="db">Redis instance.</param>
        /// <returns>The length of the processing queue.</returns>
        public long Processing(IRedisClient db)
        {
            return db.LLen(ProcessingKey);
        }

        /// <summary>
        /// Checks if a lease exists for the specified item ID.
        /// </summary>
        /// <param name="db">The Redis client instance.</param>
        /// <param name="itemId">The ID of the item to check.</param>
        /// <returns>True if lease exists, false otherwise.</returns>
        private bool LeaseExists(IRedisClient db, string itemId)
        {
            return db.Exists(LeaseKey.Of(itemId));
        }


        /// <summary>
        /// Request a work lease from the work queue. This should be called by a worker to get work to complete.
        ///
        /// When completed, the `complete` method should be called.
        ///
        /// If `block` is true, the function will return either when a job is leased or after `timeout` seconds if `timeout` isn't 0.
        ///
        /// If the job is not completed before the end of `leaseDuration`, another worker may pick up the same job.
        ///
        /// It is not a problem if a job is marked as `done` more than once.
        ///
        /// If you haven't already, it's worth reading the documentation on leasing items:
        /// https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item
        /// </summary>
        /// <param name="db">The Redis client instance.</param>
        /// <param name="leaseSeconds">The number of seconds to lease the item for.</param>
        /// <param name="block">Indicates whether to block and wait for an item to be available if the main queue is empty.</param>
        /// <param name="timeout">The maximum time to block in seconds. If 0, there is not timeout.</param>
        /// <returns>The leased item, or null if no item is available.</returns>
        public Item? Lease(IRedisClient db, int leaseSeconds, bool block, int timeout = 0)
        {
            for (; ; )
            {
                object maybeItemId;
                if (block)
                    maybeItemId = db.BRPopLPush(MainQueueKey, ProcessingKey, timeout);
                else
                    maybeItemId = db.RPopLPush(MainQueueKey, ProcessingKey);

                if (maybeItemId == null)
                    return null;

                string itemId;
                if (maybeItemId is byte[])
                    itemId = Encoding.UTF8.GetString((byte[])maybeItemId);
                else if (maybeItemId is string)
                    itemId = (string)maybeItemId;
                else
                    throw new Exception("item id from work queue not bytes or string");

                var data = db.Get<byte[]>(ItemDataKey.Of(itemId));
                if (data == null)
                {
                    if (block && timeout == 0)
                        continue;
                    return null;
                }

                db.SetEx(LeaseKey.Of(itemId), leaseSeconds, Encoding.UTF8.GetBytes(Session));

                return new Item(data, itemId);
            }
        }

        /// <summary>
        /// Marks a job as completed and remove it from the work queue. After `complete` has been
        /// called (and returns `true`), no workers will receive this job again.
        /// </summary>
        /// <param name="db">The Redis client instance.</param>
        /// <param name="item">The item to be completed.</param>
        /// <returns>True if the item was successfully completed and removed, otherwise false.</returns>
        public bool Complete(IRedisClient db, Item item)
        {
            using var pipe = db.StartPipe();
            pipe.Del(ItemDataKey.Of(item.ID));
            pipe.LRem(ProcessingKey, 0, item.ID);
            pipe.Del(LeaseKey.Of(item.ID));
            var results = pipe.EndPipe();
            return ((long)results[0]) != 0;
        }

        public void LightClean(IRedisClient db)
        {
            // A light clean only checks items in the processing queue
            var processing = db.LRange(ProcessingKey, 0, -1);
            foreach (string itemId in processing)
            {
                // If there's no lease for the item, then it should be reset.
                if (!LeaseExists(db, itemId))
                {
                    // We also check the item actually exists before pushing it back to the main queue
                    if (db.Exists(ItemDataKey.Of(itemId)))
                    {
                        Console.WriteLine($"{itemId} has not lease, it will be reset");
                        using var pipe = db.StartPipe();
                        pipe.LRem(ProcessingKey, 0, itemId);
                        pipe.LPush(MainQueueKey, itemId);
                        pipe.EndPipe();
                    }
                    else
                    {
                        Console.WriteLine($"{itemId} was in the processing queue but does not exist");
                        db.LRem(ProcessingKey, 0, itemId);
                    }
                }
            }
        }

        public void DeepClean(IRedisClient db)
        {
            // A deep clean checks all data keys
            string[] itemDataKeys;
            string[] mainQueue;
            using (var pipe = db.StartPipe())
            {
                pipe.Keys(ItemDataKey.Of("*"));
                pipe.LRange(MainQueueKey, 0, -1);
                var results = pipe.EndPipe();
                itemDataKeys = (string[])results[0];
                mainQueue = (string[])results[1];
            }
            foreach (string itemDataKey in itemDataKeys)
            {
                string itemId = itemDataKey.Substring(ItemDataKey.Prefix.Length);
                // If the item isn't in the queue, and there's no lease for the item, then it should
                // be reset.
                if (!mainQueue.Contains(itemId) && !LeaseExists(db, itemId))
                {
                    Console.WriteLine($"{itemId} has not lease, it will be reset");
                    using var pipe = db.StartPipe();
                    pipe.LRem(ProcessingKey, 0, itemId);
                    pipe.LPush(MainQueueKey, itemId);
                    pipe.EndPipe();
                }
            }
        }
    }
}
