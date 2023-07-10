using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace FastRedis
{
    public class FastRedisClient
    {
        private TcpClient _client;
        
        private ByteBuffer currentBuffer = new();
        private ByteBuffer nextFrameBuffer = new();
        private ByteBuffer writeBuffer = new();

        private Queue<FastRedisValue> _redisValuePool = new();
        private List<FastRedisValue> _received = new(1000);

        private Dictionary<long, FastRedisValue> _results = new();
        private List<FastRedisValue> _pushes = new();

        private long nextSendId = 0L;
        private long nextReceiveId = 0L;

        public IReadOnlyDictionary<long, FastRedisValue> Results => _results;
        public IReadOnlyList<FastRedisValue> Pushes => _pushes;

        public bool Open(string host, int port) {
            _client = new TcpClient(host, port);
            
            // create a pool of redis values
            for (int i = 0; i < 50000; i++)
            {
                var redisValue = new FastRedisValue();
                redisValue.Reset();
                _redisValuePool.Enqueue(redisValue);
            }
            
            var command = new List<Memory<byte>>();
            command.Add(new Memory<byte>(Encoding.Default.GetBytes("HELLO")));
            command.Add(new Memory<byte>(Encoding.Default.GetBytes("3")));
            EnqueueCommandWithoutId(command);
            
            return _client.Connected;
        }
        
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

            // add enumerated results
            for (var i = 0; i < outResults.Count; i++)
            {
                if (outResults[i].IsPush)
                {
                    _pushes.Add(outResults[i]);
                    continue;
                }
                _results.Add(nextReceiveId++, outResults[i]);
            }
        }

        public void EnqueueCommandWithoutId(List<Memory<byte>> command)
        {
            var bytesWritten = FastRedisValue.WriteBulkStringArray(new Memory<byte>(writeBuffer.Data, writeBuffer.Head, writeBuffer.Data.Length - writeBuffer.Head), command);
            writeBuffer.Head += bytesWritten;
        }

        public long EnqueueCommand(List<Memory<byte>> command)
        {
            EnqueueCommandWithoutId(command);
            return ++nextSendId;
        }

        public void EndTick()
        {
            // flush out writes
            _client.GetStream().Write(writeBuffer.Data, 0, writeBuffer.Head);
            // Console.WriteLine($"WRITE {writeBuffer.Head} bytes to socket {Encoding.Default.GetString(writeBuffer.Data, 0, writeBuffer.Head)}");
            
            // clear write buffer
            writeBuffer.Reset();
            
            // clear results this tick
            _results.Clear();

            // swap the buffers around
            (currentBuffer, nextFrameBuffer) = (nextFrameBuffer, currentBuffer);

            nextFrameBuffer.Reset();
        }
    }
}