
using System.Linq;
using System.Text;
using System.Threading;
using Core.Redis;
using NUnit.Framework;


public class RedisClientTest
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void TestConnection()
    {
        var client = new NetworkedRedisClient();
        var connected = client.Open("localhost", 6379);
        Assert.AreEqual(connected, true);
        client.Close();
    }

    [Test]
    public void TestSubscriptionHandler()
    {
        var pubClient = new NetworkedRedisClient();
        pubClient.Open("localhost", 6379);
        
        var subClient = new NetworkedRedisClient();
        subClient.Open("localhost", 6379);
        var subscriber = new RedisSubscriber(subClient);
        
        Assert.That(0, Is.EqualTo(subscriber.ActiveSubscriptions.Count));
        Assert.That(0, Is.EqualTo(subscriber.PendingSubscriptions.Count));
        subscriber.Subscribe("test");
        Assert.IsTrue(subscriber.PendingSubscriptions.Contains("test"));
        Assert.IsFalse(subscriber.ActiveSubscriptions.Contains("test"));

        Thread.Sleep(100);
        subscriber.Tick();
        
        Assert.IsFalse(subscriber.PendingSubscriptions.Contains("test"));
        Assert.IsTrue(subscriber.ActiveSubscriptions.Contains("test"));

        pubClient.SendCommand(Command("PUBLISH", "test", "message"));
        Thread.Sleep(100);
        subscriber.Tick();
        
        Assert.IsTrue(subscriber.Messages.ContainsKey("test"));
        Assert.IsTrue(subscriber.Messages["test"].Count == 1);
        Assert.That("message", Is.EqualTo(Encoding.Default.GetString(subscriber.Messages["test"][0])));
    }
    
    [Test]
    public void TestTransaction()
    {
        var client = new NetworkedRedisClient();
        client.Open("localhost", 6379);

        var multi = client.SendCommand(Command("MULTI"));
        var set = client.SendCommand(Command("SET", "a", "test"));
        var get = client.SendCommand(Command("GET", "a"));
        var exec = client.SendCommand(Command("EXEC"));

        client.TryReceiveResult(out var result, out var handle);
        Assert.AreEqual("OK" ,Encoding.Default.GetString(result.stringValue));
        Assert.AreEqual(multi ,handle);
        
        client.TryReceiveResult(out var result2, out var handle2);
        Assert.AreEqual("QUEUED" ,Encoding.Default.GetString(result2.stringValue));
        
        client.TryReceiveResult(out var result3, out var handle3);
        Assert.AreEqual("QUEUED" ,Encoding.Default.GetString(result3.stringValue));
        
        client.TryReceiveResult(out var result4, out var handle4);
        Assert.AreEqual("OK" ,Encoding.Default.GetString(result4.arrayValue[0].stringValue));
        Assert.AreEqual("test" ,Encoding.Default.GetString(result4.arrayValue[1].stringValue));
        Assert.AreEqual(exec ,handle4);
    }

    byte[][] Command(params string[] command)
    {
        return command.Select(s => Encoding.Default.GetBytes(s)).ToArray();
    }
    
}