using System.ComponentModel.Design;
using Core;
using Core.Rdis;
using StackExchange.Redis;
using Thor.Views;

namespace ExampleApp;

public class TestReadClassifier : IReadIndexClassifier
{
    public string[] Classify(IReadOnlyDictionary<string, byte[]> data)
    {
        // Get X, Y and return grid coordinate
        // return data.Keys.ToArray();

        if (data.ContainsKey("x") && data.ContainsKey("y"))
        {
            return new string[]
                { "readIndex:" + (long)(BitConverter.ToDouble(data["x"]) * 0.1) + "," + (long)(BitConverter.ToDouble(data["y"]) * 0.1) };
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
            return "writeIndex:" + (long)(BitConverter.ToDouble(data["x"]) * 0.1) + "," + (long)(BitConverter.ToDouble(data["y"]) * 0.1);

        }
        else return "writeIndex-1";
    }
}

public class IndexTest
{
    public IndexTest(IDatabase db, ISubscriber sub)
    {
        var client = new RedisClient();
        client.Open("localhost", 6379);
        var writer = new RedisStatelessStreamingWriter(client);
        var receiver = new RedisReceiver(client);
        
        var client2 = new RedisClient();
        client2.Open("localhost", 6379);
        var sub2 = new RedisSubscriber(client2);
        var factory = new StreamingReaderFactory(sub2, receiver, client);
        var index = new TestIndex("main", writer, factory);

        var writeIndex1 = new TestIndex("writeIndex-1", writer, factory);
        var entity1 = new TestIndex("entity-1", writer, factory);
        var entity2 = new TestIndex("entity-2", writer, factory);
        
        entity1.UpdateIndexValue("x", 0.0);
        entity1.UpdateIndexValue("y", 0.0);
        entity1.UpdateIndexValue("vx", 1.0);
        entity1.UpdateIndexValue("vy", 2.0);
        
        entity2.UpdateIndexValue("x", 0.0);
        entity2.UpdateIndexValue("y", 0.0);
        entity2.UpdateIndexValue("vx", 4.0);
        entity2.UpdateIndexValue("vy", 5.0);

        var readIndexNorth = new TestIndex("readIndex-north", writer, factory);
        var readIndexSouth = new TestIndex("readIndex-south", writer, factory);

        readIndexNorth.UpdateIndex("entity-1", true);
        readIndexNorth.UpdateIndex("entity-2", true);
        readIndexSouth.UpdateIndex("entity-1", false);

        index.UpdateIndexValue("writeIndex-1", "server-1");
        index.UpdateIndexValue("writeIndex:0,0", "server-1");
        index.UpdateIndexValue("writeIndex:0,1", "server-1");
        index.UpdateIndexValue("writeIndex:1,0", "server-1");
        index.UpdateIndexValue("writeIndex:2,1", "server-1");
        index.UpdateIndexValue("writeIndex:1,3", "server-1");;
        // index.UpdateIndexValue("writeIndex:0,-5", "server-1");
        // index.UpdateIndexValue("writeIndex:-2,-5", "server-1");
        writeIndex1.UpdateIndex("entity-1", true);
        writeIndex1.UpdateIndex("entity-2", true);

        var server = new TestServer("main","server-1", sub2, receiver,
            new TestReadClassifier(), 
            new TestReadClassifier(),
            
            new TestWriteClassifier(), factory, writer);

        server.Tick();
        Thread.Sleep(100);
        server.Tick();
        Thread.Sleep(100);
        server.Tick();
        Thread.Sleep(100);
        server.Tick();
        Thread.Sleep(100);
        server.Tick();
        Thread.Sleep(100);
        server.Tick();
        Thread.Sleep(100);
        server.Tick();
        Thread.Sleep(100);

        var x = 0;
        while (x < 100)
        {
            x++;
            server.Tick();
            Thread.Sleep(10);
        }

        entity1.Tick();
        entity2.Tick();
        Console.WriteLine(BitConverter.ToDouble(entity1.Data.Data["x"]));
        Console.WriteLine(BitConverter.ToDouble(entity1.Data.Data["y"]));
        Console.WriteLine(BitConverter.ToDouble(entity1.Data.Data["vx"]));
        Console.WriteLine(BitConverter.ToDouble(entity1.Data.Data["vy"]));
        
        Console.WriteLine(BitConverter.ToDouble(entity2.Data.Data["x"]));
        Console.WriteLine(BitConverter.ToDouble(entity2.Data.Data["y"]));
        Console.WriteLine(BitConverter.ToDouble(entity2.Data.Data["vx"]));
        Console.WriteLine(BitConverter.ToDouble(entity2.Data.Data["vy"]));
        
        Console.WriteLine();
    }
}