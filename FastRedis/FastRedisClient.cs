using System;
using System.Collections.Generic;
using System.Net.Sockets;

namespace FastRedis
{
    public class FastRedisClient
    {
        private TcpClient _client;
        
        private static ByteBuffer currentBuffer = new();
        private static ByteBuffer nextFrameBuffer = new();
        private static ByteBuffer writeBuffer = new();

        private Queue<FastRedisValue> _redisValuePool = new();
        
        public bool Open(string host, int port) {
            _client = new TcpClient(host, port);
            for (int i = 0; i < 50000; i++)
            {
                var redisValue = new FastRedisValue();
                redisValue.Reset();
                _redisValuePool.Enqueue(redisValue);
            }
            return _client.Connected;
        }
        
        // private Dictionary<long, RedisValue>

        public void BeginTick(List<FastRedisValue> outResults)
        {
            if (!_client.GetStream().DataAvailable)
            {
                return;
            }

            currentBuffer.Head += _client.GetStream().Read(currentBuffer.Data, currentBuffer.Head, currentBuffer.Data.Length - currentBuffer.Head);

            var totalBytesRead = FastRedisValue.TryReadValueList(new Memory<byte>(currentBuffer.Data, 0, currentBuffer.Head), outResults, _redisValuePool);
            
            // write the remainder of the buffer to the next frame buffer
            nextFrameBuffer.Reset();
            
            nextFrameBuffer.Add(currentBuffer.Data, totalBytesRead, currentBuffer.Head - totalBytesRead);
            
            currentBuffer.Reset();
            
            
            // Console.WriteLine($"ROLLING OVER {currentBuffer.Head - totalBytesRead} bytes to next frame, head is now {currentBuffer.Head}");
            
            // Console.WriteLine($"READ {totalBytesRead} bytes from socket {Encoding.Default.GetString(currentBuffer.Data, 0, currentBuffer.Head)}");
        }

        public void EqueueCommand(List<Memory<byte>> command)
        {
            var bytesWritten = FastRedisValue.WriteBulkStringArray(new Memory<byte>(writeBuffer.Data, writeBuffer.Head, writeBuffer.Data.Length - writeBuffer.Head), command);
            writeBuffer.Head += bytesWritten;
        }

        public void EndTick()
        {
            // flush out writes
            _client.GetStream().Write(writeBuffer.Data, 0, writeBuffer.Head);
            // Console.WriteLine($"WRITE {writeBuffer.Head} bytes to socket {Encoding.Default.GetString(writeBuffer.Data, 0, writeBuffer.Head)}");
            
            // clear write buffer
            writeBuffer.Reset();

            // swap the buffers around
            (currentBuffer, nextFrameBuffer) = (nextFrameBuffer, currentBuffer);

            nextFrameBuffer.Reset();
        }
    }
}