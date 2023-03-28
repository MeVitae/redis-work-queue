namespace RedisWorkerQueueTests;

using Newtonsoft.Json;
using RedisWorkQueue;
using FreeRedis;
using System.Diagnostics;

class Program
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

    public static void Main(string[] args)
    {
        RedisClient db = new RedisClient(args[0]);

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

                Console.WriteLine($"Leasing shared with block = {block}");

                var job = sharedQueue.Lease(db, 2, block, 1);

                if (job == null || sharedJobCounter % 7 == 0)
                {
                    Console.WriteLine($"Dropping job: {job}");
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
                    Thread.Sleep(1000*(sharedJobCounter % 4));

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

                Console.WriteLine($"Leasing dotnet with block = {block}");

                var job = dotNetQueue.Lease(db, 1, block, 2);

                if (job == null || dotNetJobCounter % 7 == 0)
                {
                    Console.WriteLine($"Dropping job: {job}");
                    continue;
                }

                var data = job.Data;
                if (data.Length != 1) throw new Exception("data length not 1");

                var result = data[0] * 11 % 256;

                Console.WriteLine($"Result: {result}");

                if (dotNetJobCounter % 25 == 0)
                    Thread.Sleep(1000*(sharedJobCounter % 20));

                db.Set(dotNetResultsKey.Of(job.ID), new byte[1] { (byte)result });

                if (dotNetJobCounter % 29 != 0)
                {
                    Console.WriteLine("Completing");

                    if (dotNetQueue.Complete(db, job))
                    {
                        Console.WriteLine("Spawning shared jobs");
                        sharedQueue.AddItem(db, Item.FromJson(new SharedJobData()
                        {
                            a = 19,
                            b = result
                        }));
                        sharedQueue.AddItem(db, Item.FromJson(new SharedJobData()
                        {
                            a = 23,
                            b = result
                        }));
                    }
                }
            }
        }
    }
}
