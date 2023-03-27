namespace RedisWorkerQueue
{
    public class WorkQueue{
        public string Session { get; set; }
        public string MainQueueKey { get; set; }
        public string ProcessingKey { get; set; }
        public string CleaningKey { get; set; }
        public string LeaseKey { get; set; }
         public string ItemDataKey { get; set; }
    }
}