using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Core.Redis;
using Core.Streaming;

public interface IStreamingReaderFactory
{
    IStreamingReader Make(string key);
}

public class StreamingReaderFactory : IStreamingReaderFactory
{
    private NetworkedRedisClient _client;
    private readonly RedisSubscriber _subscriber;
    private readonly RedisReceiver _receiver;

    public StreamingReaderFactory(RedisSubscriber subscriber, RedisReceiver receiver, NetworkedRedisClient client)
    {
        _client = client;
        _subscriber = subscriber;
        _receiver = receiver;
    }

    public IStreamingReader Make(string key)
    {
        return new RedisStreamingReader(_client, _receiver, _subscriber, key);
    }
}

public struct StreamingEntryUpdateExternal
{
    public IReadOnlyDictionary<string, byte[]> FieldUpdates;
    public byte[][] Events;
    public int Count;
}

public interface IStreamingReader
{
    void Open();

    void Close();

    bool IsValid { get; }

    bool IsClosed { get; }

    IReadOnlyDictionary<string, byte[]> Data { get; }

    bool TryReceive(out StreamingEntryUpdateExternal result);
}


public class RedisStreamingReader : IStreamingReader
{
    private readonly NetworkedRedisClient _client;
    private readonly RedisReceiver _receiver;
    private readonly RedisSubscriber _subscriber;

    private readonly string _key;
    private long _epoch = -1;
    private long _initialEpoch = -1;
    private List<StreamingEntryUpdate> _updateQueue = new();
    private StreamingEntryUpdate? _initialUpdate;
    private bool _initialized = false;
    private Dictionary<string, byte[]> _data = new();
    private long _getInitialStateHandle = -1;

    public RedisStreamingReader(NetworkedRedisClient client, RedisReceiver receiver, RedisSubscriber subscriber, string key)
    {
        _client = client;
        _receiver = receiver;
        _subscriber = subscriber;
        _key = key;
        IsValid = false;
        IsClosed = false;
    }

    public void Open()
    {
        // Console.WriteLine($"{_key} opening");
        _subscriber.Subscribe(_key);
    }

    public void Close()
    {
        _subscriber.Unsubscribe(_key);
        IsClosed = true;
    }

    public bool IsValid { get; private set; }

    public bool IsClosed { get; private set; }

    public IReadOnlyDictionary<string, byte[]> Data => _data;

    public bool TryReceive(out StreamingEntryUpdateExternal result)
    {
        if (!IsValid)
        {
            Console.WriteLine($"{_key} not valid");
            
            // If we now have a live subscription, but have yet to ask for subscription
            if (_subscriber.ActiveSubscriptions.Contains(_key) && _getInitialStateHandle == -1)
            {
                Console.WriteLine($"{_key} got subscription,  now getting initial state");
                _getInitialStateHandle = _client.SendCommand(new[]
                {
                    Encoding.Default.GetBytes("HGETALL"),
                    Encoding.Default.GetBytes(_key)
                });
            }

            // If we have a live subscription, but are still waiting on initial state
            if (_subscriber.Messages.ContainsKey(_key))
            {
                foreach (var update in _subscriber.Messages[_key])
                {
                    var stream = new MemoryStream(update);
                    var reader = new BinaryReader(stream);
                    _updateQueue.Add(Serializers.ReadUpdate(reader));
                }
            }

            // If we have a live subscription and have asked for the initial state, and now have response
            if (_receiver.Results.ContainsKey(_getInitialStateHandle))
            {
                var redisResult = _receiver.Results[_getInitialStateHandle].arrayValue;
                
                // Console.WriteLine($"{_key} got initial states {redisResult.Length}");
                
                // Read initial state
                for (var i = 0; i < redisResult.Length; i += 2)
                {
         
                    
                    var field = Encoding.Default.GetString(redisResult[i].stringValue);
                    var value = redisResult[i + 1].stringValue;
                    if (field == "_epoch")
                    {
                        _initialEpoch = BitConverter.ToInt64(value, 0);
                    }
                    else
                    {
                        _data[field] = value;
                    }
                }

                // Try and find this initial epoch with our pub/sub updates to know where to begin
                var initial = _updateQueue.FindIndex(entry => entry.Epoch == _initialEpoch);
                if (initial != -1)
                {
                    _updateQueue.RemoveRange(0, initial);
                }

                foreach (var update in _updateQueue)
                {
                    foreach (var fieldUpdate in update.FieldUpdates)
                    {
                        if (fieldUpdate.Value.Length > 0)
                        {
                            _data[fieldUpdate.Key] = fieldUpdate.Value;
                        }
                        else
                        {
                            _data.Remove(fieldUpdate.Key);
                        }
                    }
                }

                // Console.WriteLine($"{_key} got initial state {string.Join("|", _data.Keys)}");
                IsValid = true;
                result = new StreamingEntryUpdateExternal
                    { FieldUpdates = _data, Events = new byte[][] { }, Count = 1 };
                return true;
            }
        }
        else
        {
            var consolidatedEvents = new List<byte[]>();
            var consolidatedFieldUpdates = new Dictionary<string, byte[]>();
            var updateCount = 0;

            if (_subscriber.Messages.ContainsKey(_key))
            {
                updateCount = _subscriber.Messages[_key].Count;
                foreach (var updateBytes in _subscriber.Messages[_key])
                {
                    var stream = new MemoryStream(updateBytes);
                    var reader = new BinaryReader(stream);
                    var update = Serializers.ReadUpdate(reader);
                    foreach (var eventBytes in update.Events)
                    {
                        consolidatedEvents.Add(eventBytes);
                    }

                    foreach (var fieldUpdate in update.FieldUpdates)
                    {
                        consolidatedFieldUpdates[fieldUpdate.Key] = fieldUpdate.Value;
                        if (fieldUpdate.Value.Length > 0)
                        {
                            _data[fieldUpdate.Key] = fieldUpdate.Value;
                        }
                        else
                        {
                            _data.Remove(fieldUpdate.Key);
                        }
                    }
                }
            }

            result = new StreamingEntryUpdateExternal
            {
                FieldUpdates = consolidatedFieldUpdates, Events = consolidatedEvents.ToArray(),
                Count = updateCount
            };
            return true;
        }

        result = new StreamingEntryUpdateExternal
            { FieldUpdates = new Dictionary<string, byte[]>(), Events = new byte[][] { }, Count = 0 };
        return false;
    }
}