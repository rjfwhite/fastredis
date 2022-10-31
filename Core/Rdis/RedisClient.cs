using System.Net.Sockets;
using System.Text;

namespace Core.Rdis;

public class RedisClient
{
    private TcpClient _client;
    private BinaryWriter _writer;
    private BinaryReader _reader;
    
    // Used for correlating requests to responses 
    // This ceases to be valid if the connection enters subscribe mode
    // where the server will send messages without a correlated client message
    private long _sendHandle = 0;
    private long _receiveHandle = 0;

    public struct RedisValue
    {
        public byte[] stringValue;
        public byte[] errorValue;
        public int? intValue;
        public RedisValue[] arrayValue;
    }
    
    public bool Open(string hostname, int port)
    {
        _client = new TcpClient();
        _client.Connect(hostname, port);
        if (_client.Connected)
        {
            var stream = _client.GetStream();
            _reader = new BinaryReader(stream);
            _writer = new BinaryWriter(stream);
        }

        return _client.Connected;
    }

    public void Close()
    {
        _client.Dispose();
    }
    
    public long SendCommand(byte[][] command)
    {
        WriteBulkStringArray(command);
        return _sendHandle++;
    }

    public void Flush()
    {
        _writer.Flush();
    }

    public bool IsResultAvailable => _client.GetStream().DataAvailable;

    public bool TryReceiveResult(out RedisValue result, out long handle)
    {
        result = new RedisValue();
        handle = -1;
        if (IsResultAvailable)
        {
            result = ReceiveResult();
            handle = _receiveHandle++;
            return true;
        }
        return false;
    }
    
    private RedisValue ReceiveResult()
    {
        RedisValue result = new RedisValue();
        var b = _reader.ReadByte();
        switch (b)
        {
            // string
            case (byte)'+':
                result.stringValue = ReadSimpleStringResult();
                break;

            // error
            case (byte)'-':
                result.errorValue = ReadSimpleStringResult();
                break;

            // bulk string
            case (byte)'$':
                result.stringValue = ReadBulkStringResult();
                break;

            // integer
            case (byte)':':
                result.intValue = ReadIntegerValue();
                break;

            // array
            case (byte)'*':
                var length = ReadIntegerValue();
                var array = new List<RedisValue>();
                for (var i = 0; i < length; i++)
                {
                    array.Add(ReceiveResult());
                }
                result.arrayValue = array.ToArray();
                break;
            
            default:
                throw new Exception("COULD NOT PARSE STARTING " + (char)b + (char)_reader.ReadByte() + (char)_reader.ReadByte() + (char)_reader.ReadByte() + (char)_reader.ReadByte()+ (char)_reader.ReadByte() + (char)_reader.ReadByte()+ (char)_reader.ReadByte()+ (char)_reader.ReadByte()+ (char)_reader.ReadByte()+ (char)_reader.ReadByte()+ (char)_reader.ReadByte()+ (char)_reader.ReadByte());
        }

        return result;
    }
    private void WriteBulkStringArray(byte[][] data)
    {
        _writer.Write('*');
        _writer.Write(Encoding.Default.GetBytes(data.Length.ToString()));
        WriteCRLF();
        foreach (var bulkString in data)
        {
            WriteBulkString(bulkString);
        }
    }

    private byte[] ReadSimpleStringResult()
    {
        var result = new List<byte>();

        byte b = _reader.ReadByte();
        while (b != '\r')
        {
            result.Add(b);
            b = _reader.ReadByte();
        }

        // Read the '\n'
        _reader.ReadByte();
        return result.ToArray();
    }

    private byte[] ReadBulkStringResult()
    {
        var length = ReadIntegerValue();
        var result = _reader.ReadBytes(length);
        _reader.ReadByte();
        _reader.ReadByte();
        return result;
    }
    private int ReadIntegerValue()
    {
        var result = -1;
        var length = new List<byte>();

        var b = _reader.ReadByte();
        while (b != '\r')
        {
            length.Add(b);
            b = _reader.ReadByte();
        }

        int.TryParse(Encoding.Default.GetString(length.ToArray()), out result);
        // parse the '\n'
        _reader.ReadByte();
        return result;
    }

    private void WriteBulkString(byte[] data)
    {
        _writer.Write('$');
        _writer.Write(Encoding.ASCII.GetBytes(data.Length.ToString()));
        WriteCRLF();
        _writer.Write(data);
        WriteCRLF();
    }

    private void WriteCRLF()
    {
        _writer.Write('\r');
        _writer.Write('\n');
    }
}