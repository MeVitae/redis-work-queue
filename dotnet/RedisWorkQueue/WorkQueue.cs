using System;
using System.Text;

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

        private string CleaningKey { get; set; }

        /// <summary>
        /// Creates a new instance of the WorkQueue class with based on name given name.
        /// </summary>
        /// <param name="name">The key prefix for the work queue.</param>
        public WorkQueue(KeyPrefix name)
        {
            this.Session = name.Of(Guid.NewGuid().ToString());
            this.MainQueueKey = name.Of(":queue");
            this.ProcessingKey = name.Of(":processing");
            this.LeaseKey = name.Concat(":leased_by_session:");
            this.ItemDataKey = name.Concat(":item:");
            this.CleaningKey = name.Of(":cleaning");
        }

        /// <summary>
        /// Adds item to the work queue.
        /// </summary>
        /// <param name="db">Redis instance.</param>
        /// <param name="item">Item to be added.</param>
        public void AddItem(IRedisClient db, Item item)
        {
            using (var pipe = db.StartPipe())
            {
                pipe.Set(ItemDataKey.Of(item.ID), item.Data);
                pipe.LPush(MainQueueKey, item.ID);

                pipe.EndPipe();
            }
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
        public bool LeaseExists(IRedisClient db, string itemId)
        {
            return db.Exists(LeaseKey.Of(itemId));
        }


        /// <summary>
        /// Request a work lease from the work queue. This should be called by a worker to get work to complete.
        /// When completed, the `complete` method should be called.
        /// If `block` is true, the function will return either when a job is leased or after `timeout` seconds if `timeout` isn't 0.
        /// If the job is not completed before the end of `leaseDuration`, another worker may pick up the same job.
        /// It is not a problem if a job is marked as `done` more than once.
        ///If you haven't already, it's worth reading the documentation on leasing items:
        /// https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item
        /// </summary>
        /// <param name="db">The Redis client instance.</param>
        /// <param name="leaseSeconds">The number of seconds to lease the item for.</param>
        /// <param name="block">Indicates whether to block and wait for an item to be available if the main queue is empty.</param>
        /// <param name="timeout">The maximum time to block in seconds.</param>
        /// <returns>The leased item, or null if no item is available.</returns>
        public Item? Lease(IRedisClient db, int leaseSeconds, bool block, int timeout = 0)
        {
            object maybeItemId;
            if (block)
            {
                maybeItemId = db.BRPopLPush(MainQueueKey, ProcessingKey, timeout);
            }
            else
            {
                maybeItemId = db.RPopLPush(MainQueueKey, ProcessingKey);
            }

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
                data = new byte[0];

            db.SetEx(LeaseKey.Of(itemId), leaseSeconds, Encoding.UTF8.GetBytes(Session));

            return new Item(data, itemId);
        }

        /// <summary>
        /// Marks a job as completed and remove it from the work queue.
        /// </summary>
        /// <param name="db">The Redis client instance.</param>
        /// <param name="item">The item to be completed.</param>
        /// <returns>True if the item was successfully completed and removed, otherwise false.</returns>
        public bool Complete(IRedisClient db, Item item)
        {
            var removed = db.LRem(ProcessingKey, 0, item.ID);

            if (removed == 0)
                return false;

            string itemId = item.ID;

            using (var pipe = db.StartPipe())
            {
                pipe.Del(ItemDataKey.Of(itemId));
                pipe.Del(LeaseKey.Of(itemId));

                pipe.EndPipe();
            }

            return true;
        }


        /// <summary>
        /// Moves abandoned/stuck in processing jobs back to the main work queue
        /// First collects items in the processing queue
        /// Then re adds items without a lease (due to expiry or never created) to the main queue and cleaning queue 
        /// Next we do the same for items in cleaning queue, this handles cases when the cleaner crashes
        /// </summary>
        /// <param name="db">The Redis client instance</param>
        public void LightClean(IRedisClient db)
        {
            var processing = db.LRange(ProcessingKey, 0, -1);
            foreach (var item_id in processing)
            {
                if (!LeaseExists(db, item_id))
                {
                    Console.WriteLine("{item_id} has not lease");
                    db.LPush(CleaningKey, item_id);
                    var removed = db.LRem(ProcessingKey, 0, item_id);
                    if (removed > 0)
                    {
                        db.LPush(MainQueueKey, item_id);
                        Console.WriteLine($"{item_id} was still in the processing queue, it was reset");
                    }
                    else
                        Console.WriteLine($"{item_id} was no longer in the processing queue");
                    db.LRem(CleaningKey, 0, item_id);
                }
            }

            var forgot = db.LRange(CleaningKey, 0, -1);
            foreach (var item_id in forgot)
            {
                Console.WriteLine($"{item_id} was forgotten in clean");
                //FreeRedis LPos always returns a long
                //need to confirm if -1 is returned in place of NIL
                if (!LeaseExists(db, item_id) && db.LPos(MainQueueKey, item_id) < 0 && db.LPos(ProcessingKey, item_id) < 0)
                {
                    db.LPush(MainQueueKey, item_id);
                    Console.WriteLine($"{item_id} was not in any queue, it was reset");
                }
                db.LRem(CleaningKey, 0, item_id);
            }
        }
    }
}
