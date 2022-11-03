using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Thor.Optimization;

namespace Core.Redis
{
    public struct RedisValue
    {
        public byte[] stringValue;
        public byte[] errorValue;
        public int? intValue;
        public RedisValue[] arrayValue;
        
        static byte[] CRLF = { (byte)'\r', (byte)'\n'};
        private static ByteBuffer buffer = new();

        public static RedisValue ReceiveResult(BinaryReader reader)
        {
            RedisValue result = new RedisValue();
            var b = reader.ReadByte();
            switch (b)
            {
                // string
                case (byte)'+':
                    result.stringValue = ReadSimpleStringResult(reader);
                    break;

                // error
                case (byte)'-':
                    result.errorValue = ReadSimpleStringResult(reader);
                    break;

                // bulk string
                case (byte)'$':
                    result.stringValue = ReadBulkStringResult(reader);
                    break;

                // integer
                case (byte)':':
                    result.intValue = ReadIntegerValue(reader);
                    break;

                // array
                case (byte)'*':
                    var length = ReadIntegerValue(reader);
                    var array = new List<RedisValue>();
                    for (var i = 0; i < length; i++)
                    {
                        array.Add(ReceiveResult(reader));
                    }
                    result.arrayValue = array.ToArray();
                    break;
            
                default:
                    throw new Exception("COULD NOT PARSE STARTING " + (char)b + (char)reader.ReadByte() + (char)reader.ReadByte() + (char)reader.ReadByte() + (char)reader.ReadByte()+ (char)reader.ReadByte() + (char)reader.ReadByte()+ (char)reader.ReadByte()+ (char)reader.ReadByte()+ (char)reader.ReadByte()+ (char)reader.ReadByte()+ (char)reader.ReadByte()+ (char)reader.ReadByte());
            }

            return result;
        }
        
        public static void WriteBulkStringArray(byte[][] data, BinaryWriter writer)
        {
            buffer.Reset();
            buffer.Add((byte)'*');
            buffer.AddIntString(data.Length);
            buffer.Add(CRLF);
            foreach (var bulkString in data)
            {
                buffer.Add((byte)'$');
                buffer.AddIntString(bulkString.Length);
                buffer.Add(CRLF);
                buffer.Add(bulkString);
                buffer.Add(CRLF);
            }
            writer.Write(buffer.Data, 0, buffer.Head);
        }

        private static byte[] ReadSimpleStringResult(BinaryReader reader)
        {
            var result = new List<byte>();

            byte b = reader.ReadByte();
            while (b != '\r')
            {
                result.Add(b);
                b = reader.ReadByte();
            }

            // Read the '\n'
            reader.ReadByte();
            return result.ToArray();
        }

        private static byte[] ReadBulkStringResult(BinaryReader reader)
        {
            var length = ReadIntegerValue(reader);
            var result = reader.ReadBytes(length);
            reader.ReadByte();
            reader.ReadByte();
            return result;
        }
        
        private static int ReadIntegerValue(BinaryReader reader)
        {
            var result = -1;
            var length = new List<byte>();

            var b = reader.ReadByte();
            while (b != '\r')
            {
                length.Add(b);
                b = reader.ReadByte();
            }

            int.TryParse(Encoding.Default.GetString(length.ToArray()), out result);
            // parse the '\n'
            reader.ReadByte();
            return result;
        }
    }
    
    
}