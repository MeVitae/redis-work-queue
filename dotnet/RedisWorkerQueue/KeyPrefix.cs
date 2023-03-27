namespace RedisWorkerQueue
{
    public class KeyPrefix
    {
        public string Prefix { get; set; }
        public KeyPrefix(string Prefix)
        {
            this.Prefix = Prefix;
        }

        public string Of(string name)
        {
            return Prefix + name;
        }

        public KeyPrefix Concat(string name)
        {
            return new KeyPrefix(Of(name));
        }
    }
}