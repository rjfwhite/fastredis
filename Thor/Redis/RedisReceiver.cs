using System;
using System.Collections.Generic;

namespace Core.Redis
{
    public class RedisReceiver
    {
        private NetworkedRedisClient _client;

        private Dictionary<long, RedisValue> _results = new();

        public IReadOnlyDictionary<long, RedisValue> Results => _results;

        public RedisReceiver(NetworkedRedisClient client)
        {
            _client = client;
        }
    
        public void Tick()
        {
            _results.Clear();
            while (_client.TryReceiveResult(out var result, out var handle))
            {
                _results.Add(handle, result);
            }   
        }
    }
}