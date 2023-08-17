using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Newtonsoft.Json;

namespace RedisWorkQueue
{
    /// <summary>
    /// Represents an item to be stored in the Redis work queue.
    /// </summary>
    public class Item
    {
        /// <summary>
        /// Gets or sets the data as a byte array.
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// Gets or sets the data as a (UTF-8) string.
        /// </summary>
        public string StringData
        {
            get
            {
                return Encoding.UTF8.GetString(Data);
            }
            set
            {
                Data = Encoding.UTF8.GetBytes(value);
            }
        }

        /// <summary>
        /// Gets or sets the ID of the item.
        /// </summary>
        public string ID { get; set; }

        /// <summary>
        /// Creates a new instance of the Item class with the specified data and ID.
        /// </summary>
        /// <param name="data">The data to be stored in the item.</param>
        /// <param name="id">An optional ID to uniquely identify the item. If not provided, a new GUID will be generated.</param>
        public Item(byte[] data, string? id = null)
        {
            // Generate a random ID if none was passed.
            if (string.IsNullOrEmpty(id)) id = Guid.NewGuid().ToString();
            this.ID = id;
            this.Data = data;
        }

        /// <summary>
        /// Creates a new instance of the Item class with the specified data and ID.
        /// </summary>
        /// <param name="data">The data to be stored in the item.</param>
        /// <param name="id">An optional ID to uniquely identify the item. If not provided, a new GUID will be generated.</param>
        public Item(string data, string? id = null) : this(Encoding.UTF8.GetBytes(data), id) { }

        /// <summary>
        /// Creates a new instance of the Item class from the provided data by serializing it as JSON.
        /// </summary>
        /// <param name="data">The data to be serialized and stored in the item.</param>
        /// <param name="id">An optional ID to identify the item. If not provided, a new GUID will be generated.</param>
        /// <returns>A new instance of the Item class with the serialized JSON data.</returns>
        public static Item FromJson(object data, string? id = null)
        {
            return new Item(JsonConvert.SerializeObject(data), id);
        }

        /// <summary>
        /// Deserializes the stored data into an object using JSON deserialization.
        /// </summary>
        /// <typeparam name="T">The type to deserialize the data into.</typeparam>
        /// <returns>The deserialized object of type T. Returns null if the deserialization fails.</returns>
        public T? DataJson<T>()
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(Data));
        }
    }
}
