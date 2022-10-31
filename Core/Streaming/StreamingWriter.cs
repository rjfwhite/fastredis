using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Core.Rdis;

// using StackExchange.Redis;
public struct StreamingEntryUpdate
{
    public long Epoch;
    public IReadOnlyDictionary<string, byte[]> FieldUpdates;
    public byte[][] Events;
}

public interface IStatelessStreamingWriter
{
    void Send(string key, IReadOnlyDictionary<string, byte[]> fields, byte[][] events);
}


public class RedisStatelessStreamingWriter : IStatelessStreamingWriter
{
    private RedisClient _client;

    public RedisStatelessStreamingWriter(RedisClient client)
    {
        _client = client;
    }

    public void Send(string key, IReadOnlyDictionary<string, byte[]> fields, byte[][] events)
    {
        long nextEpoch = new Random().NextInt64();
        MemoryStream stream = new MemoryStream();
        BinaryWriter writer = new BinaryWriter(stream);
        Serializers.WriteUpdate(writer, new StreamingEntryUpdate{Epoch = nextEpoch, FieldUpdates = fields, Events = events});

        var updateArgs = new List<byte[]>();
        var removalArgs = new List<byte[]>();

        updateArgs.Append(Encoding.Default.GetBytes("HSET"));
        removalArgs.Append(Encoding.Default.GetBytes("HDEL"));
        
        foreach (var field in fields)
        {
            if (field.Value.Length > 0)
            {
                updateArgs.Add(Encoding.Default.GetBytes(field.Key));
                updateArgs.Add(field.Value);
            }
            else
            {
                removalArgs.Add(Encoding.Default.GetBytes(field.Key));
            }
        }
        
        updateArgs.Add(Encoding.Default.GetBytes("_epoch"));
        updateArgs.Add(BitConverter.GetBytes(nextEpoch));
        
        _client.SendCommand(new[] {  Encoding.Default.GetBytes("MULTI") });
        _client.SendCommand(updateArgs.ToArray());
        if (removalArgs.Count > 1)
        {
            _client.SendCommand(removalArgs.ToArray());
        };
        _client.SendCommand(new[] {  Encoding.Default.GetBytes("PUBLISH"), Encoding.Default.GetBytes(key), stream.ToArray() });
        _client.SendCommand(new[] {  Encoding.Default.GetBytes("EXEC") });
    }
}
