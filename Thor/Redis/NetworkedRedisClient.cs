using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Thor.Optimization;

namespace Core.Redis
{
    public class NetworkedRedisClient : IRedisClient
    {
        private TcpClient _client;
        private BinaryWriter _writer;
        private BinaryReader _reader;

        // Used for correlating requests to responses 
        // This ceases to be valid if the connection enters subscribe mode
        // where the server will send messages without a correlated client message
        private long _sendHandle = 0;
        private long _receiveHandle = 0;

        struct QueueItem
        {
            public RedisValue Value;
            public long Handle;
        }

        private ConcurrentQueue<QueueItem> _queue = new();

        public bool Open(string hostname, int port)
        {
            _client = new TcpClient();
            _client.Connect(hostname, port);

            if (_client.Connected)
            {
                var stream = _client.GetStream();
                _reader = new BinaryReader(new BufferedStream(stream, 100000));
                _writer = new BinaryWriter(new BufferedStream(stream, 100000));
            }

            Thread thread = new Thread(() =>
            {
                while (true)
                {
                    var result = RedisValue.ReceiveResult(_reader);
                    var handle = _receiveHandle++;
                    _queue.Enqueue(new QueueItem{Value = result, Handle = handle});
                }
            });
            thread.Start();

            SendCommand(new[]
            {
                Encoding.Default.GetBytes("SELECT"),
                Encoding.Default.GetBytes("1")
            });

            return _client.Connected;
        }

        public void Close()
        {
            _client.Dispose();
        }

        public long SendCommand(params string[] command)
        {
            return SendCommand(command.Select(c => Encoding.Default.GetBytes(c)).ToArray());
        }

        public long SendCommand(byte[][] command)
        {
            RedisValue.WriteBulkStringArray(command, _writer);
            return _sendHandle++;
        }

        public void Flush()
        {
            _writer.Flush();
        }

        public bool TryReceiveResult(out RedisValue result, out long handle)
        {
            while (_queue.TryDequeue(out var item))
            {
                result = item.Value;
                handle = item.Handle;
                return true;
            }

            result = new RedisValue();
            handle = -1;
            return false;
        }
    }
}