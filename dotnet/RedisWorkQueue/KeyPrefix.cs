namespace RedisWorkQueue
{
    /// <summary>
    /// KeyPrefix is a string which should be prefixed to an identifier to generate a database key.
    /// </summary>
    public class KeyPrefix
    {
        /// <summary>
        /// Gets or sets the prefix string.
        /// </summary>
        public string Prefix { get; set; }

        /// <summary>
        /// Creates a new instance of the KeyPrefix class with the specified prefix.
        /// </summary>
        /// <param name="prefix">A string specifying the prefix to use for Redis keys.</param>
        public KeyPrefix(string Prefix)
        {
            this.Prefix = Prefix;
        }


        /// <summary>
        /// This creates the Key Prefix itself.
        /// </summary>
        /// <param name="name">Name of the Redis key.</param>
        /// <returns>Namespaced Redis key.</returns>
        public string Of(string name)
        {
            return Prefix + name;
        }

        /// <summary>
        /// Concat other onto prefix and return the result as a KeyPrefix.
        /// </summary>
        /// <param name="prefix">An instance of the KeyPrefix class representing the prefix to concatenate.</param>
        /// <param name="name">Name to concatenate with the prefix.</param>
        /// <returns>A new KeyPrefix instance with the concatenated namespaced prefix.</returns>
        public static KeyPrefix Concat(KeyPrefix prefix, string name)
        {
            return new KeyPrefix(prefix.Of(name));
        }
    }
}
