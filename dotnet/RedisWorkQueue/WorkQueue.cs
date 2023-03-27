using System.Text;
using ServiceStack.Redis;
using ServiceStack.Redis.Pipeline;

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

        public void AddItemToPipeline(IRedisPipeline pipeline, Item item)
        {
            pipeline.QueueCommand(p => p.Set(ItemDataKey.Of(item.ID), item.Data));
            pipeline.QueueCommand(p => p.Custom("LPUSH", MainQueueKey, item.ID));
        }

        public void AddItem(IRedisClient db, Item item)
        {
            var pipeline = db.CreatePipeline();
            AddItemToPipeline(pipeline, item);
            pipeline.Flush();
        }

        public long QueueLength(IRedisClient db)
        {
            return db.GetListCount(MainQueueKey);
        }

        public long Processing(IRedisClient db)
        {
            return db.GetListCount(ProcessingKey);
        }

        public bool LeaseExists(IRedisClient db, string itemId)
        {
            return db.ContainsKey(LeaseKey.Of(itemId));
        }

        public Item? Lease(IRedisNativeClient db, int leaseSeconds, bool block, int timeout = 0)
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

            var data = db.Get(ItemDataKey.Of(itemId));
            if (data == null)
                data = new byte[0];
            
            db.SetEx(LeaseKey.Of(itemId), leaseSeconds, Encoding.UTF8.GetBytes(Session));

            return new Item(data, itemId);
        }

        public bool Complete(IRedisClient db, Item item){
            var removed = db.RemoveItemFromList(ProcessingKey, item.ID, 0);

            if(removed == 0)
                return false;

            string itemId = item.ID;

            var pipeline = db.CreatePipeline();
            pipeline.QueueCommand(p => p.Delete(ItemDataKey.Of(itemId)));
            pipeline.QueueCommand(p => p.Delete(LeaseKey.Of(itemId)));
            pipeline.Flush();

            return true;
        }
    }
}