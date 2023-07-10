using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using FastRedis;

namespace ExampleApp
{
    public class FastRedisClientTest
    {
        static void Main()
        {
            var client = new FastRedisClient();
            var received = new List<FastRedisValue>(1000);
            client.Open("localhost", 6379);
            
            
            var command = new List<Memory<byte>>();
            command.Add(new Memory<byte>(Encoding.Default.GetBytes("HGETALL")));
            command.Add(new Memory<byte>(Encoding.Default.GetBytes("garry")));
        
            var totalMessages = 0;
            var sw = new Stopwatch();
            sw.Start();
        
            for (var i = 0; i < 500; i++)
            {
                for (var j = 0; j < 500; j++)
                {
                    client.EnqueueCommand(command);
                }
                
                received.Clear();
                client.BeginTick(received);
                totalMessages += received.Count;
                
                client.EndTick();
                // Console.WriteLine($"received {received.Count} messages");
            }
        
            Console.WriteLine($"received total of {totalMessages} messages in {sw.ElapsedMilliseconds}ms");
        }
    }
}