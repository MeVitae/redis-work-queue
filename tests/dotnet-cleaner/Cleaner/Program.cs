using RedisWorkQueue;
using FreeRedis;

class Program
{
    public static void Main(string[] args)
    {
        RedisClient db = new RedisClient(args[0]);
        for (; ; )
        {
            string? queueName = Console.ReadLine();
            if (queueName == null) break;
            if (queueName == "") throw new Exception("input line must be non-empty");
            new WorkQueue(new KeyPrefix(queueName)).LightClean(db);
            Console.WriteLine("cleaned " + queueName);
        }
    }
}
