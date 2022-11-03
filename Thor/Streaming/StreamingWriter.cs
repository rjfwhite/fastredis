using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Core.Redis;
using Core.Streaming;
using Thor.Optimization;

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

    void Flush();
}


public class RedisStatelessStreamingWriter : IStatelessStreamingWriter
{
    private NetworkedRedisClient _client;

    public RedisStatelessStreamingWriter(NetworkedRedisClient client)
    {
        _client = client;
    }

    
    static MemoryStream stream = new MemoryStream();
    static BinaryWriter writer = new BinaryWriter(stream);
    
    public void Send(string key, IReadOnlyDictionary<string, byte[]> fields, byte[][] events)
    {
        long nextEpoch = new Random().Next();
        stream.SetLength(0);
        Serializers.WriteUpdate(writer, new StreamingEntryUpdate{Epoch = nextEpoch, FieldUpdates = fields, Events = events});

        var updateArgs = new List<byte[]>();
        var removalArgs = new List<byte[]>();
        var keyBytes = Memoizer.ToBytes(key);

        updateArgs.Add(Memoizer.ToBytes("HSET"));
        updateArgs.Add(keyBytes);
        removalArgs.Add(Memoizer.ToBytes("HDEL"));
        removalArgs.Add(keyBytes);
        
        foreach (var field in fields)
        {
            var fieldKey = Memoizer.ToBytes(field.Key);
            if (field.Value.Length > 0)
            {
                updateArgs.Add(fieldKey);
                updateArgs.Add(field.Value);
            }
            else
            {
                removalArgs.Add(fieldKey);
            }
        }
        
        updateArgs.Add(Memoizer.ToBytes("_epoch"));
        updateArgs.Add(BitConverter.GetBytes(nextEpoch));
        
        _client.SendCommand(new[] {  Memoizer.ToBytes("MULTI") });
        _client.SendCommand(updateArgs.ToArray());
        if (removalArgs.Count > 2)
        {
            _client.SendCommand(removalArgs.ToArray());
        };
        _client.SendCommand(new[] {  Memoizer.ToBytes("PUBLISH"), keyBytes, stream.ToArray() });
        _client.SendCommand(new[] {  Memoizer.ToBytes("EXEC") });
    }

    public void Flush()
    {
        _client.Flush();
    }
}
