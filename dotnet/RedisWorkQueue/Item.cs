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
        /// Gets or sets the serialized data as a byte array.
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// Gets or sets the ID of the item.
        /// </summary>
        public string ID { get; set; }

        /// <summary>
        /// Creates a new instance of the Item class with the specified data and ID.
        /// </summary>
        /// <param name="data">The data to be serialized and stored in the item.</param>
        /// <param name="id">An optional ID to uniquely identify the item. If not provided, a new GUID will be generated.</param>
        public Item(object Data, string? ID = null)
        {
            /// <summary>
            /// Gets or sets the serialized data as a byte array.
             /// </summary>
            byte[] byteData;
            if (Data is string)
                byteData = Encoding.UTF8.GetBytes((string)Data);
            else if (!(Data is byte[]))
            {
                BinaryFormatter bf = new BinaryFormatter();
                using (var ms = new MemoryStream())
                {
                    //as long as we have full control over and know what the data is then this is okay 
#pragma warning disable SYSLIB0011
                    bf.Serialize(ms, Data);
#pragma warning restore SYSLIB0011
                    byteData = ms.ToArray();
                }
            }
            else
                byteData = (byte[])Data;

            if (ID == null) ID = Guid.NewGuid().ToString();

            if (byteData == null)
                throw new Exception("item failed to serialise data to byte[]");
            this.Data = byteData;
            if (ID == null)
                throw new Exception("item failed to create ID");

            this.ID = ID;
        }

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
