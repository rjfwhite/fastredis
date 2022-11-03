using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Core.Redis;

namespace ExampleApp
{
    public class PerformanceTest
    {
        public PerformanceTest()
        {
            var client = new NetworkedRedisClient();
            client.Open("localhost", 6379);
            var writer = new RedisStatelessStreamingWriter(client);
            var receiver = new RedisReceiver(client);

            client.SendCommand("FLUSHDB");
        
            var client2 = new NetworkedRedisClient();
            client2.Open("localhost", 6379);
            var sub2 = new RedisSubscriber(client2);
            var factory = new StreamingReaderFactory(sub2, receiver, client);

            var key = factory.Make("test-key");

            var update = new Dictionary<string, byte[]>
            {
                {"a", new byte[]{1,2,3}},
                {"b", new byte[]{4,5,6}},
                {"d", new byte[]{4,5,6}},
                {"e", new byte[]{4,5,6}},
                {"f", new byte[]{4,5,6}},
                {"g", new byte[]{4,5,6}},
                {"h", new byte[]{4,5,6}},
                {"i", new byte[]{4,5,6}},
            };
            
            Console.WriteLine("START");
            var sw = new Stopwatch();
            long count = 0;
            sw.Start();
            for (var i = 0; i < 1000; i++)
            {
                for (var j = 0; j < 1000; j++)
                {
                    writer.Send("test-key", update, new byte[][]{});
                    // client.Test();
                }
                
                // receiver.Tick();
                count += receiver.Results.Count;
                writer.Flush();
            }
            receiver.Tick();
            count += receiver.Results.Count;
            Console.WriteLine("DONE " + sw.ElapsedMilliseconds + "ms for " + count + " writes");
        }
    }
}