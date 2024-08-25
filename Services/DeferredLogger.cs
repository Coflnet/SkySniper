using System;
using System.Collections.Concurrent;

namespace Coflnet.Sky.Sniper.Services
{
    public class DeferredLogger
    {
        ConcurrentQueue<(DateTime, string)> logs = new();

        public void Log(string v)
        {
            logs.Enqueue((DateTime.UtcNow, v));
        }

        public void PrintQueue()
        {
            while (logs.TryDequeue(out var log))
            {
                Console.WriteLine($"{log.Item1} {log.Item2}");
            }
        }
    }
}
