using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Newtonsoft.Json;

namespace RedisWorkQueue
{
    public class Item
    {
        public byte[] Data { get; set; }
        public string ID { get; set; }
        public Item(object Data, object? ID = null)
        {
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

            if (ID == null)
                ID = Guid.NewGuid().ToString();
            else if (!(ID is string))
                ID = ID.ToString();

            if (byteData == null)
                throw new Exception("item failed to serialise data to byte[]");
            this.Data = byteData;
            if (ID == null)
                throw new Exception("item failed to create ID");

            this.ID = (string)ID;
        }

        public static Item FromJson(object data, object? id = null)
        {
            return new Item(JsonConvert.SerializeObject(data), id);
        }

        public T DataJson<T>()
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(Data));
        }
    }
}
