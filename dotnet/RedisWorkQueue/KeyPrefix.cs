namespace RedisWorkQueue
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

        public static KeyPrefix Concat(KeyPrefix prefix, string name)
        {
            return new KeyPrefix(prefix.Of(name));
        }
    }
}