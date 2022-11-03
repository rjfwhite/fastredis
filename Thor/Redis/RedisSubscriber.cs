using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Core.Redis
{
    public class RedisSubscriber
    {
        private HashSet<string> _activeSubscriptions = new();
        private HashSet<string> _pendingSubscriptions = new();
        private Dictionary<string, List<byte[]>> _messagesThisTick = new();
        private Dictionary<string, int> _numberOfSubscriptions = new();

        public HashSet<string> ActiveSubscriptions => _activeSubscriptions;
        public HashSet<string> PendingSubscriptions => _pendingSubscriptions;
        public IReadOnlyDictionary<string, List<byte[]>> Messages => _messagesThisTick;
    
        public RedisSubscriber(NetworkedRedisClient client)
        {
            _client = client;
        }

        public void Subscribe(string channel)
        {
            if (_numberOfSubscriptions.ContainsKey(channel))
            {
                _numberOfSubscriptions[channel]++;
            }
            else
            {
                _numberOfSubscriptions.Add(channel, 1);
                _pendingSubscriptions.Add(channel);
                _client.SendCommand(new []
                {
                    Encoding.Default.GetBytes("SUBSCRIBE"),
                    Encoding.Default.GetBytes(channel)
                });
            }
        }

        public void Unsubscribe(string channel)
        {
            if(_numberOfSubscriptions.TryGetValue(channel, out var count))
            {
                if (count == 1)
                {
                    _numberOfSubscriptions.Remove(channel);
                    _pendingSubscriptions.Remove(channel);
                    _activeSubscriptions.Remove(channel);
                    _client.SendCommand(new []
                    {
                        Encoding.Default.GetBytes("UNSUBSCRIBE"),
                        Encoding.Default.GetBytes(channel)
                    });
                }
                else
                {
                    _numberOfSubscriptions[channel]--;
                }
            }
        }
    
        private NetworkedRedisClient _client;

        public void Tick()
        {
            _messagesThisTick.Clear();
            while (_client.TryReceiveResult(out var result, out var handle))
            {
                if (result.arrayValue != null && result.arrayValue.Length == 3)
                {
                    var command = Encoding.Default.GetString(result.arrayValue[0].stringValue);
                    var channel = Encoding.Default.GetString(result.arrayValue[1].stringValue);
                    var data = result.arrayValue[2].stringValue;
                
                    switch (command)
                    {
                        case "message":
                            if (!_messagesThisTick.ContainsKey(channel))
                            {
                                _messagesThisTick.Add(channel, new List<byte[]>());
                            }
                            _messagesThisTick[channel].Add(data);
                            break;
                    
                        case "subscribe":
                            _activeSubscriptions.Add(channel);
                            _pendingSubscriptions.Remove(channel);
                            break;
                    }
                }
                else
                {
                    // throw new Exception("Unknown result received");
                }
            }
        }

        public void Flush()
        {
            _client.Flush();
        }
    }
}