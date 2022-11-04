using System;
using System.Collections.Generic;

namespace FastRedis
{
    public struct FastRedisValue
    {
        public Memory<byte> stringValue;
        public Memory<byte> errorValue;
        public bool nullValue;
        public int? intValue;
        public List<FastRedisValue> arrayValue;

        public void Reset()
        {
            stringValue = null;
            errorValue = null;
            nullValue = false;
            intValue = null;
            if (arrayValue == null)
            {
                arrayValue = new List<FastRedisValue>(100);
            }
            arrayValue.Clear();
        }

        public static int TryReadValue(Memory<byte> reader, ref FastRedisValue result)
        {
            try
            {
                var identifier = reader.Span[0];
             
                // simple string
                if (identifier == '+')
                {
                    var bytesRead = TryReadSimpleString(reader, out var simpleString);
                    if (bytesRead > 0)
                    {
                        result.stringValue = simpleString;
                        return bytesRead;
                    }

                    return -1;
                }

                // error string
                if (identifier == '-')
                {
                    var bytesRead = TryReadSimpleString(reader, out var simpleString);
                    if (bytesRead > 0)
                    {
                        result.errorValue = simpleString;
                        return bytesRead;
                    }

                    return -1;
                }

                // bulk string
                if (identifier == '$')
                {
                    var bytesRead = TryReadBulkString(reader, out var bulkString);
                    if (bytesRead > 0)
                    {
                        if (bulkString != null)
                        {
                            result.stringValue = bulkString.Value;
                        }
                        else
                        {
                            result.nullValue = true;
                        }

                        return bytesRead;
                    }

                    return -1;
                }

                // integer
                if (identifier == ':')
                {
                    var bytesRead = TryReadIntegerValue(reader.Slice(1), out var integer) + 1;
                    if (bytesRead > 0)
                    {
                        result.intValue = integer;
                        return bytesRead;
                    }

                    return -1;
                }

                // array
                if (identifier == '*')
                {
                    var bytesRead = TryReadArray(reader, result.arrayValue);
                    if (bytesRead > 0)
                    {
                        return bytesRead;
                    }

                    return -1;
                }
            }
            catch (IndexOutOfRangeException e)
            {
                return -1;
            }
            catch (ArgumentOutOfRangeException e)
            {
                return -1;
            }
            return -1;
        }

        public static int TryReadSimpleString(Memory<byte> reader, out Memory<byte> result)
        {
            try
            {
                var totalBytesRead = 0;
                var availableBytes = reader.Length;

                // read a + or -
                if (reader.Span[0] != '+' && reader.Span[0] != '-')
                {
                    result = new Memory<byte>();
                    return -1;
                }

                // consume the identifier
                reader = reader.Slice(1);
                totalBytesRead++;
                var ptr = 0;

                // seek the CLRF
                while (ptr < reader.Length && reader.Span[ptr] != '\r')
                {
                    ptr++;
                }

                // if overran, return false
                if (ptr == reader.Length)
                {
                    result = new Memory<byte>();
                    return -1;
                }

                result = reader.Slice(0, ptr);

                totalBytesRead += ptr;

                totalBytesRead += 2;

                if (totalBytesRead > availableBytes)
                {
                    result = new Memory<byte>();
                    return -1;
                }

                // header + string + CLRF
                return totalBytesRead;
            }
            catch (IndexOutOfRangeException e)
            {
                result = new Memory<byte>();
                return -1;
            }
            catch (ArgumentOutOfRangeException e)
            {
                result = new Memory<byte>();
                return -1;
            }
        }

        public static int TryReadBulkString(Memory<byte> reader, out Memory<byte>? result)
        {
            try
            {
                var totalBytesRead = 0;
                var availableBytes = reader.Length;

                // if read a $
                if (reader.Span[0] != '$')
                {
                    result = null;
                    return -1;
                }

                // consume the identifier
                reader = reader.Slice(1);
                totalBytesRead++;

                int read = TryReadIntegerValue(reader, out var intValue);
                if (read == -1)
                {
                    result = null;
                    return -1;
                }

                // advance read to start of string
                reader = reader.Slice(read);
                totalBytesRead += read;

                if (intValue >= 0)
                {
                    result = reader.Slice(0, intValue);
                    totalBytesRead += intValue;
                }
                else
                {
                    result = null;
                }

                // CLRF
                totalBytesRead += 2;

                if (totalBytesRead > availableBytes)
                {
                    result = null;
                    return -1;
                }

                return totalBytesRead;
            }
            catch (IndexOutOfRangeException e)
            {
                result = null;
                return -1;
            }
            catch (ArgumentOutOfRangeException e)
            {
                result = null;
                return -1;
            }
        }

        public static int TryReadIntegerValue(Memory<byte> reader, out int result)
        {
            try
            {
                var totalBytesRead = 0;
                var availableBytes = reader.Length;

                bool isNegative = false;
                if (reader.Span[0] == '-')
                {
                    isNegative = true;
                    reader = reader.Slice(1);
                    totalBytesRead++;
                }

                var ptr = 0;
                // seek the CLRF
                while (ptr < reader.Length && reader.Span[ptr] != '\r')
                {
                    ptr++;
                }

                // if overran, return false
                if (ptr == reader.Length)
                {
                    result = -1;
                    return -1;
                }

                int value = 0;
                for (var i = 0; i < ptr; i++)
                {
                    value *= 10;
                    int dec = reader.Span[i] - '0';
                    value += dec;
                }

                if (isNegative)
                {
                    value *= -1;
                }

                result = value;

                // Integer size
                totalBytesRead += ptr;

                // CLRF
                totalBytesRead += 2;

                if (totalBytesRead > availableBytes)
                {
                    result = -1;
                    return -1;
                }

                return totalBytesRead;
            }
            catch (IndexOutOfRangeException e)
            {
                result = -1;
                return -1;
            }
        }

        public static int TryReadArray(Memory<byte> reader, List<FastRedisValue> result)
        {
            try
            {
                var totalBytesRead = 0;
                var availableBytes = reader.Length;

                // if read a *
                if (reader.Span[0] != '*')
                {
                    return -1;
                }

                // consume the identifier
                reader = reader.Slice(1);
                totalBytesRead++;

                int read = TryReadIntegerValue(reader, out var arrayCount);
                if (read == -1)
                {
                    return -1;
                }

                // advance read to start of array
                reader = reader.Slice(read);
                totalBytesRead += read;

                for (var i = 0; i < arrayCount; i++)
                {
                    var redisValue = new FastRedisValue();
                    
                    var bytesRead = TryReadValue(reader, ref redisValue);
                    if (bytesRead == -1)
                    {
                        return -1;
                    }

                    reader = reader.Slice(bytesRead);
                    totalBytesRead += bytesRead;
                    result.Add(redisValue);
                }

                if (totalBytesRead > availableBytes)
                {
                    return -1;
                }

                return totalBytesRead;
            }
            catch (IndexOutOfRangeException e)
            {
                return -1;
            }
        }

        public static int TryReadValueList(Memory<byte> reader, List<FastRedisValue> outValueList, Queue<FastRedisValue> poolIn)
        {
            var totalBytesRead = 0;
            var availableBytes = reader.Length;
            var bytesRead = 0;

            while (bytesRead != -1 && totalBytesRead < availableBytes)
            {
                var redisValue = poolIn.Dequeue();
                poolIn.Enqueue(redisValue);
                redisValue.Reset();
                bytesRead = TryReadValue(reader,  ref redisValue);
                if (bytesRead > 0)
                {
                    totalBytesRead += bytesRead;
                    reader = reader.Slice(bytesRead);
                    outValueList.Add(redisValue);
                }
            }

            return totalBytesRead;
        }

        public static int WriteBulkStringArray(Memory<byte> writer, List<Memory<byte>> bulkStringArray)
        {
            var totalBytesWritten = 0;

            writer.Span[0] = (byte)'*';
            writer = writer.Slice(1);
            totalBytesWritten++;

            var lengthBytes = WriteIntValue(writer, bulkStringArray.Count);
            totalBytesWritten += lengthBytes;
            writer = writer.Slice(lengthBytes);

            for (var i = 0; i < bulkStringArray.Count; i++)
            {
                var bytesWritten = WriteBulkString(writer, bulkStringArray[i]);
                writer = writer.Slice(bytesWritten);
                totalBytesWritten += bytesWritten;
            }

            return totalBytesWritten;
        }

        public static int WriteBulkString(Memory<byte> writer, Memory<byte> bulkString)
        {
            var totalBytesWritten = 0;

            writer.Span[0] = (byte)'$';
            writer = writer.Slice(1);
            totalBytesWritten++;

            var lengthBytes = WriteIntValue(writer, bulkString.Length);
            writer = writer.Slice(lengthBytes);
            totalBytesWritten += lengthBytes;

            bulkString.CopyTo(writer);
            writer = writer.Slice(bulkString.Length);
            totalBytesWritten += bulkString.Length;

            writer.Span[0] = (byte)'\r';
            writer.Span[1] = (byte)'\n';
            totalBytesWritten += 2;

            return totalBytesWritten;
        }

        public static int WriteIntValue(Memory<byte> writer, int value)
        {
            var totalBytesWritten = 0;

            if (value < 0)
            {
                value *= -1;
                writer.Span[0] = (byte)'-';
                writer = writer.Slice(1);
                totalBytesWritten++;
            }

            if (value >= 10000)
            {
                throw new NotImplementedException("can only write arrays of up to length 9999 in this basic implementation");
            }

            var divideBy = 1000;
            for (var i = 0; i < 4; i++)
            {
                var divisor = (value / divideBy) % 10;
                if (divisor > 0 || i == 3)
                {
                    writer.Span[0] = (byte)(divisor + '0');
                    writer = writer.Slice(1);
                    totalBytesWritten++;
                }

                divideBy /= 10;
            }

            writer.Span[0] = (byte)'\r';
            writer.Span[1] = (byte)'\n';
            totalBytesWritten += 2;

            return totalBytesWritten;
        }
    }
}