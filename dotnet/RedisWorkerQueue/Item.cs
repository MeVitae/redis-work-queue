using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Newtonsoft.Json;

namespace RedisWorkerQueue
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

            if (ID == null)
                ID = Guid.NewGuid().ToString();
            else if (!(ID is string))
                ID = ID.ToString();

            this.Data = (byte[])Data;
            this.ID = (string)ID;
        }

        public static Item FromJson(object data, object? id = null)
        {
            return new Item(JsonConvert.SerializeObject(data), id);
        }

        public object DataJson()
        {
            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(Data));
        }
    }
}
