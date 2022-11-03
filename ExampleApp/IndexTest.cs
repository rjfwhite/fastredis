using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using BenchmarkDotNet.Engines;
using Core.Redis;
using Thor.Views;

public class TestReadClassifier : IReadIndexClassifier
{
    public string[] Classify(IReadOnlyDictionary<string, byte[]> data)
    {
        // Get X, Y and return grid coordinate
        // return data.Keys.ToArray();

        if (data.ContainsKey("x") && data.ContainsKey("y"))
        {
            return new string[]
                { "readIndex:" + (long)(BitConverter.ToDouble(data["x"], 0)) + "," + (long)(BitConverter.ToDouble(data["y"], 0)) };
        }
        else return new string[] {"readIndex-north"};
    }
}

public class TestWriteClassifier : IWriteIndexClassifier
{
    public string Classify(IReadOnlyDictionary<string, byte[]> data)
    {
        // get X, Y and return grid coordinate
        if (data.ContainsKey("x") && data.ContainsKey("y"))
        {
            return "writeIndex:" + (long)(BitConverter.ToDouble(data["x"], 0)) + "," + (long)(BitConverter.ToDouble(data["y"], 0));
        }
        return "writeIndex-1";
    }
}

public class IndexTest
{
    public IndexTest()
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

        client.SendCommand(new[]
        {
            Encoding.Default.GetBytes("HSET"),
            Encoding.Default.GetBytes("entity-1"),
            Encoding.Default.GetBytes("x"),
            BitConverter.GetBytes(0.0),
            Encoding.Default.GetBytes("y"),
            BitConverter.GetBytes(0.0),
            Encoding.Default.GetBytes("vx"),
            BitConverter.GetBytes(1.0),
            Encoding.Default.GetBytes("vy"),
            BitConverter.GetBytes(2.0),
        });
        
        client.SendCommand(new[]
        {
            Encoding.Default.GetBytes("HSET"),
            Encoding.Default.GetBytes("entity-2"),
            Encoding.Default.GetBytes("x"),
            BitConverter.GetBytes(0.0),
            Encoding.Default.GetBytes("y"),
            BitConverter.GetBytes(0.0),
            Encoding.Default.GetBytes("vx"),
            BitConverter.GetBytes(3.0),
            Encoding.Default.GetBytes("vy"),
            BitConverter.GetBytes(4.0),
        });

        client.SendCommand(new[]{
            Encoding.Default.GetBytes("HSET"),
            Encoding.Default.GetBytes("main"),
            Encoding.Default.GetBytes("writeIndex:0,0"),
            Encoding.Default.GetBytes("server-1"),
            Encoding.Default.GetBytes("writeIndex:1,0"),
            Encoding.Default.GetBytes("server-1"),
            Encoding.Default.GetBytes("writeIndex:0,1"),
            Encoding.Default.GetBytes("server-1"),
            Encoding.Default.GetBytes("writeIndex:1,1"),
            Encoding.Default.GetBytes("server-1"),
        });
        
        client.SendCommand(new[]
        {
            Encoding.Default.GetBytes("HSET"),
            Encoding.Default.GetBytes("writeIndex:0,0"),
            Encoding.Default.GetBytes("entity-1"),
            BitConverter.GetBytes(true)
        });
        
        client.SendCommand(new[]
        {
            Encoding.Default.GetBytes("HSET"),
            Encoding.Default.GetBytes("writeIndex:0,0"),
            Encoding.Default.GetBytes("entity-2"),
            BitConverter.GetBytes(true)
        });
        

        var server = new TestServer("main","server-1", sub2, receiver,
            new TestReadClassifier(), 
            new TestReadClassifier(),
            
            new TestWriteClassifier(), factory, writer);
        

        var x = 0;
        while (x < 100)
        {
            x++;
            server.Tick();
            Thread.Sleep(1);
        }
    }
}