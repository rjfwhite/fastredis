using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace FastRedis;

public class FastRedisStreamingHash
{
    private Dictionary<long, ByteBuffer> _data = new();
    private Dictionary<long, Memory<byte>> _updates = new();
    private List<long> _removals = new();
    private List<Memory<byte>> _events = new();
    private int _updateCount = 0;

    private FastRedisClient _client;
    private long _streamId;

    private bool _hasSubscription = false;
    private Memory<byte> _subscribeMessage = new Memory<byte>(Encoding.Default.GetBytes("subscribe"));
    private Memory<byte> _messageMessage = new Memory<byte>(Encoding.Default.GetBytes("message"));
    private Memory<byte> _messageStreamId;
    private bool _hasInitialData = false;
    private long _initialDataId;
    private Queue<Memory<byte>> _queuedUpdated = new();
    
    public FastRedisStreamingHash(FastRedisClient client, long streamId)
    {
        _client = client;
        _streamId = streamId;
        _messageStreamId = new Memory<byte>(BitConverter.GetBytes(streamId));
        
        // subscribe to the stream
        List<Memory<byte>> command = new();
        command.Add(Encoding.Default.GetBytes("SUBSCRIBE"));
        command.Add(new Memory<byte>(BitConverter.GetBytes(streamId)));
        
        // enqueue subscribe
         _client.EnqueueCommand(command);
    }

    public IReadOnlyDictionary<long, ByteBuffer> Data => _data;
    public IReadOnlyDictionary<long, Memory<byte>> Updates => _updates;
    public IReadOnlyList<long> Removals => _removals;
    public IReadOnlyList<Memory<byte>> Events => _events;
    public int UpdateCount => _updateCount;

    public void Tick()
    {
        _updates.Clear();
        _removals.Clear();
        _events.Clear();
        _updateCount = 0;
        
        if (!_hasSubscription)
        {
            if(_client.Pushes.Any(
                item => item.ArrayValue.Any() && 
                        item.ArrayValue.Count > 3 && 
                        item.ArrayValue[0].StringValue.Span.SequenceEqual(_subscribeMessage.Span) && 
                        item.ArrayValue[1].IntValue == _streamId))
            {
                _hasSubscription = true;
                // enqueue hgetall command for the stream id now that we have our subscription
                List<Memory<byte>> command = new();
                command.Add(Encoding.Default.GetBytes("HGETALL"));
                command.Add(new Memory<byte>(BitConverter.GetBytes(_streamId)));
                _initialDataId = _client.EnqueueCommand(command);
            }
            else
            {
                return;
            }
        }
        
        foreach(var push in _client.Pushes)
        {
            if (!push.ArrayValue[0].StringValue.Span.SequenceEqual(_messageMessage.Span) || 
                !push.ArrayValue[1].StringValue.Span.SequenceEqual(_messageStreamId.Span))
            {
                continue;
            }
            _queuedUpdated.Enqueue(push.ArrayValue[3].StringValue);
        }
        
        // Set initial data if we have it
        if (!_hasInitialData) {
            if (_client.Results.ContainsKey(_initialDataId))

            {
                _hasInitialData = true;
                _updateCount++;

                var result = _client.Results[_initialDataId];
                for (var i = 0; i < result.ArrayValue.Count; i += 2)
                {
                    var key = _client.Results[i].StringValue;
                    var value = _client.Results[i + 1].StringValue;
                    var longKey = BytesToLong(key);
                    _data.Add(longKey, new ByteBuffer());
                    _data[longKey].Add(value);
                    _updates.Add(longKey, value);
                }
            }
            else
            {
                return;
            }
        }
        
        while(_queuedUpdated.TryDequeue(out var result))
        {
            ProcessUpdate(result);
        }
    }
    
    private void ProcessUpdate(Memory<byte> update)
    {
        _updateCount++;
        
        var threadLocalUpdateList = new ThreadLocal<List<FastRedisValue>>(() =>
        {
            return new();
        });
        threadLocalUpdateList.Value.Clear();
        
        var threadLocalUpdateQueue = new ThreadLocal<Queue<FastRedisValue>>(() =>
        {
            var queue = new Queue<FastRedisValue>();
            for (var i = 0; i < 30; i++)
            {
                queue.Enqueue(new FastRedisValue());
            }

            return queue;
        });

        var bytesRead =
            FastRedisValue.TryReadValueList(update, threadLocalUpdateList.Value, threadLocalUpdateQueue.Value);

        if (bytesRead > 0 && threadLocalUpdateList.Value.Count >=4)
        {
            var epoch = threadLocalUpdateList.Value[0].IntValue.Value;
            var updateLen = threadLocalUpdateList.Value[1].IntValue.Value;

            for (var i = 2; i < updateLen + 2; i += 2)
            {
                var key = threadLocalUpdateList.Value[i].StringValue;
                var value = threadLocalUpdateList.Value[i + 1].StringValue;
                var longKey = BytesToLong(key);
            }
            
            var removalsLen = threadLocalUpdateList.Value[updateLen + 2].IntValue.Value;
            for (var i = updateLen + 3; i < updateLen + removalsLen + 3 + removalsLen; i++)
            {
                var key = threadLocalUpdateList.Value[i].StringValue;
                var longKey = BytesToLong(key);
                _removals.Add(longKey);
            }
            
            var eventsLen = threadLocalUpdateList.Value[updateLen + removalsLen + 3].IntValue.Value;
            for (var i = updateLen + removalsLen + 4; i < updateLen + removalsLen + eventsLen + 4; i++)
            {
                var eventBytes = threadLocalUpdateList.Value[i].StringValue;
                _events.Add(eventBytes);
            }
        }
    }

    private static long BytesToLong(Memory<byte> bytes)
    {
        // new thread local bytes
        var threadLocalBytes = new ThreadLocal<byte[]>(() =>
        {
            var bytes = new byte[8];
            return bytes;
        });

        bytes.CopyTo(threadLocalBytes.Value);
        return BitConverter.ToInt64(threadLocalBytes.Value);
    }
    
}