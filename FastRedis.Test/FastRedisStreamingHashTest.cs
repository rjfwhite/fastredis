using System.Text;
using NUnit.Framework;

namespace FastRedis.Test
{
    public class FastRedisStreamingHashTest
    {
        public class RedisClientTest
        {
            [Test]
            public void GetsPreviouslySetMap()
            {
                long streamId = 1;
                
                var key = 1;
                var value = "Gaz";
                
                var sourceClientResults = new List<FastRedisValue>();
                var sourceClient = new FastRedisClient();
                sourceClient.Open("localhost", 6379);
                sourceClient.EnqueueCommand(new List<Memory<byte>>()
                {
                    new Memory<byte>(Encoding.Default.GetBytes("HSET")),
                    new Memory<byte>(BitConverter.GetBytes(streamId)),
                    new Memory<byte>(BitConverter.GetBytes(key)),
                    new Memory<byte>(Encoding.Default.GetBytes(value))
                });
                sourceClient.BeginTick(sourceClientResults);
                sourceClient.EndTick();
                
                var streamingHashClientResults = new List<FastRedisValue>();
                var streamingHashClient = new FastRedisClient();
                streamingHashClient.Open("localhost", 6379);
                var streamingHash = new FastRedisStreamingHash(streamingHashClient, streamId);
                
                while (!streamingHash.IsReady())
                {
                    streamingHashClientResults.Clear();
                    streamingHashClient.BeginTick(streamingHashClientResults);
                    streamingHash.Tick();
                    streamingHashClient.EndTick();
                }
                
                Assert.That(streamingHash.Data.Count, Is.EqualTo(1));
                var byteArray = streamingHash.Data[key].Data.ToArray();
                int lastIndex = Array.FindLastIndex(byteArray, b => b != 0);
                Assert.That(Encoding.Default.GetString(byteArray, 0, lastIndex + 1), Is.EqualTo(value));
            }
        }
    }
}