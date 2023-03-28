namespace RedisWorkerQueueTests;

using Newtonsoft.Json;
using RedisWorkQueue;
using FreeRedis;

[TestClass]
public class RedisWorkerQueueTests
{
    public struct SharedJobData
    {
        public int a { get; set; }
        public int b { get; set; }
    }
    public struct SharedJobResult
    {
        public int a { get; set; }
        public int sum { get; set; }
        public int prod { get; set; }
        public string worker { get; set; }
    }

    [TestMethod]
    public void TestRedisWorkerQueue()
    {
        RedisClient db = new RedisClient("localhost:6379");

        var dotNetResultsKey = new KeyPrefix("results:dotnet:");
        var sharedResultsKey = new KeyPrefix("results:shared:");

        var dotNetQueue = new WorkQueue(new KeyPrefix("dotnet_jobs"));
        var sharedQueue = new WorkQueue(new KeyPrefix("shared_jobs"));

        int dotNetJobCounter = 0;
        int sharedJobCounter = 0;

        bool shared = false;

        while (true)
        {
            shared = !(shared);
            if (shared)
            {
                sharedJobCounter++;

                bool block = (sharedJobCounter % 5) == 0;

                Console.WriteLine($"leasing shared with block = {block}");

                var job = sharedQueue.Lease(db, 2, block, 1);

                if (job == null || sharedJobCounter % 7 == 0)
                {
                    Console.WriteLine($"Dropping job: = {job}");
                    continue;
                }

                var data = job.DataJson<SharedJobData>();


                var result = new SharedJobResult()
                {
                    a = data.a,
                    sum = data.a + data.b,
                    prod = data.a * data.b,
                    worker = "dotnet"
                };

                var jsonResult = JsonConvert.SerializeObject(result);

                Console.WriteLine("Result: ", jsonResult);

                if (sharedJobCounter % 12 == 0)
                    Thread.Sleep(sharedJobCounter % 4);

                db.Set(sharedResultsKey.Of(job.ID), jsonResult);

                if (sharedJobCounter % 29 != 0)
                {
                    Console.WriteLine("Completing");
                    sharedQueue.Complete(db, job);
                }
                else
                    Console.WriteLine("Dropping");
            }
            else
            {
                dotNetJobCounter++;

                bool block = (sharedJobCounter % 6) == 0;

                Console.WriteLine($"leasing dotnet with block = {block}");

                var job = dotNetQueue.Lease(db, 1, block, 2);

                if (job == null || dotNetJobCounter % 7 == 0)
                {
                    Console.WriteLine($"Dropping job: = {job}");
                    continue;
                }

                var data = job.Data;
                Assert.IsTrue(data.Length == 1);

                var result = data[0] * 3 % 256;

                Console.WriteLine($"Result: {result}");

                if (dotNetJobCounter % 25 == 0)
                    Thread.Sleep(sharedJobCounter % 20);

                db.Set(dotNetResultsKey.Of(job.ID), new byte[1] { (byte)result });

                if (dotNetJobCounter % 29 != 0)
                {
                    Console.WriteLine("Completing");

                    if(dotNetQueue.Complete(db, job)){
                        Console.WriteLine("Spawning shared jobs");
                        sharedQueue.AddItem(db, Item.FromJson(new SharedJobData(){
                            a = 13,
                            b = result
                        }));
                         sharedQueue.AddItem(db, Item.FromJson(new SharedJobData(){
                            a = 17,
                            b = result
                        }));
                    }
                }
            }
        }
    }
}
