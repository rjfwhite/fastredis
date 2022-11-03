using System.Linq;
using System.Text;

namespace Core.Redis
{
    public class InMemoryRedisClient : IRedisClient
    {
        private InMemoryRedis _redis;

        internal void ReceiveMessage(string key, byte[] message)
        {
            
        }
        
        public long SendCommand(params string[] command)
        {
            return SendCommand(command.Select(c => Encoding.Default.GetBytes(c)).ToArray());
        }

        public long SendCommand(byte[][] command)
        {
            throw new System.NotImplementedException();
        }

        public bool TryReceiveResult(out RedisValue result, out long handle)
        {
            throw new System.NotImplementedException();
        }
    }
}