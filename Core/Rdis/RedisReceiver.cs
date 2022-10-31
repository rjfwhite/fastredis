using System.Text;
using Iced.Intel;

namespace Core.Rdis;

public class RedisReceiver
{
    private RedisClient _client;

    private Dictionary<long, RedisClient.RedisValue> _results = new();

    public IReadOnlyDictionary<long, RedisClient.RedisValue> Results => _results;

    public RedisReceiver(RedisClient client)
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