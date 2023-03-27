using System.Text;
using FreeRedis;

namespace RedisWorkQueue
{
    public class WorkQueue
    {
        public string Session { get; set; }
        public string MainQueueKey { get; set; }
        public string ProcessingKey { get; set; }
        public string CleaningKey { get; set; }
        public KeyPrefix LeaseKey { get; set; }
        public KeyPrefix ItemDataKey { get; set; }

        public WorkQueue(KeyPrefix name)
        {
            this.Session = name.Of(Guid.NewGuid().ToString());
            this.MainQueueKey = name.Of(":queue");
            this.ProcessingKey = name.Of(":processing");
            this.CleaningKey = name.Of(":cleaning");
            this.LeaseKey = KeyPrefix.Concat(name, ":leased_by_session:");
            this.ItemDataKey = KeyPrefix.Concat(name, ":item:");
        }

        // public void AddItemToPipeline(PipelineHook pipeline, Item item)
        // {
        //     pipeline.QueueCommand(p => p.Set(ItemDataKey.Of(item.ID), item.Data));
        //     pipeline.QueueCommand(p => p.Custom("LPUSH", MainQueueKey, item.ID));
        // }

        public void AddItem(IRedisClient db, Item item)
        {
            using (var pipe = db.StartPipe())
            {
                pipe.Set(ItemDataKey.Of(item.ID), item.Data);
                pipe.LPush(MainQueueKey, item.ID);

                pipe.EndPipe();
            }
        }

        public long QueueLength(IRedisClient db)
        {
            return db.LLen(MainQueueKey);
        }

        public long Processing(IRedisClient db)
        {
            return db.LLen(ProcessingKey);
        }

        public bool LeaseExists(IRedisClient db, string itemId)
        {
            return db.Exists(LeaseKey.Of(itemId));
        }

        public Item? Lease(IRedisClient db, int leaseSeconds, bool block, int timeout = 0)
        {
            object maybeItemId;
            if (block)
            {
                maybeItemId = db.BRPopLPush(MainQueueKey, ProcessingKey, timeout);
                //maybeItemId = db.Custom("BRPOPLPUSH", MainQueueKey, ProcessingKey, timeout);
            }
            else
            {
                maybeItemId = db.RPopLPush(MainQueueKey, ProcessingKey);
                //maybeItemId = db.Custom("RPOPLPUSH", MainQueueKey, ProcessingKey);
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
    }
}