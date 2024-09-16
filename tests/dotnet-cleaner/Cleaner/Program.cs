using RedisWorkQueue;
using FreeRedis;

class Program
{
    public static void Main(string[] args)
    {
        RedisClient db = new RedisClient(args[0]);
        for (; ; )
        {
            string? instruction = Console.ReadLine();
            if (instruction == null) break;
            if (instruction == "") throw new Exception("input line must be non-empty");
            string[] parts = instruction.Split(':', 2);
            if (parts.Length != 2) throw new Exception("input line must be of the format light:queue-name or deep:queue-name");
            string queueName = parts[1];
            var queue = new WorkQueue(new KeyPrefix(queueName));
            switch (parts[0])
            {
                case "light":
                    queue.LightClean(db);
                    Console.WriteLine("light cleaned " + queueName);
                    break;
                case "deep":
                    queue.DeepClean(db);
                    Console.WriteLine("deep cleaned " + queueName);
                    break;
                default:
                    throw new Exception($"invalid cleaning mode: ${parts[0]}, should be \"light\" or \"deep\"");
            }
        }
    }
}
