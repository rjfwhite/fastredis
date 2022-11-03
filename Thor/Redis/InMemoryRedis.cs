using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Policy;
using System.Text;

namespace Core.Redis
{
    public class InMemoryRedis
    {
        private bool Foo = true;

        private Dictionary<string, Dictionary<string, byte[]>> _data = new();
        private Dictionary<string, HashSet<InMemoryRedisClient>> _subscriptions = new();

        public RedisValue ExecuteCommand(InMemoryRedisClient client, byte[][] command)
        {
            var op = Encoding.Default.GetString(command[0]);
            var key = Encoding.Default.GetString(command[1]);
            var result = new RedisValue();
            
            switch (op)
            {
                case "PUBLISH":
                    var message = command[2];
                    if (_subscriptions.ContainsKey(key))
                    {
                        foreach (var subscriber in _subscriptions[key])
                        {
                            subscriber.ReceiveMessage(key, message);
                        }
                    }

                    break;
                
                case "SUBSCRIBE":
                    var numReceived = 0;
                    if (!_subscriptions.ContainsKey(key))
                    {
                        numReceived++;
                        _subscriptions.Add(key, new HashSet<InMemoryRedisClient>());
                    }

                    result.intValue = numReceived;
                    _subscriptions[key].Add(client);
                    break;
                
                case "UNSUBSCRIBE":
                    if (_subscriptions.ContainsKey(key))
                    {
                        _subscriptions[key].Remove(client);
                    }
                    break;
                
                case "HSET":
                    break;
                
                case "HDEL":
                    break;
                
                case "HGETALL":
                    break;
            }

            return result;
        }
        
    }
}