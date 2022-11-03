using System;
using System.Collections.Generic;
using System.Text;
using Core.Redis;
using NUnit.Framework;

namespace Test
{
    public class FastRedisValueTest
    {
        public class RedisClientTest
        {
            [Test]
            public void ReadInteger()
            {
                byte[] input = Encoding.ASCII.GetBytes("32\r\n");
                var bytesRead = FastRedisValue.TryReadIntegerValue(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(result, Is.EqualTo(32));
            }
            
            [Test]
            public void ReadNegativeInteger()
            {
                byte[] input = Encoding.ASCII.GetBytes("-24\r\n");
                var bytesRead = FastRedisValue.TryReadIntegerValue(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(result, Is.EqualTo(-24));
            }
            
            [Test]
            public void ReadTruncatedInteger()
            {
                byte[] input = Encoding.ASCII.GetBytes("-24\r");
                var bytesRead = FastRedisValue.TryReadIntegerValue(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(-1));
            }
            
            [Test]
            public void ReadSimpleString()
            {
                byte[] input = Encoding.ASCII.GetBytes("+hello\r\nfas;lkf;la'sf");
                var bytesRead = FastRedisValue.TryReadSimpleString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo("+hello\r\n".Length));
                Assert.That(Encoding.Default.GetString(result.ToArray()), Is.EqualTo("hello"));
            }
            
            [Test]
            public void ReadIncompleteString()
            {
                byte[] input = Encoding.ASCII.GetBytes("+hel");
                var bytesRead = FastRedisValue.TryReadSimpleString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(-1));
            }
            
            [Test]
            public void ReadSimpleErrorString()
            {
                byte[] input = Encoding.ASCII.GetBytes("-hello error times\r\n");
                var bytesRead = FastRedisValue.TryReadSimpleString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(Encoding.Default.GetString(result.ToArray()), Is.EqualTo("hello error times"));
            }
            
            [Test] public void ReadArray()
            {
                byte[] input = Encoding.ASCII.GetBytes("*4\r\n+hello\r\n+world\r\n+t\r\n+values\r\n");
                var bytesRead = FastRedisValue.TryReadArray(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(Encoding.Default.GetString(result[0].stringValue.ToArray()), Is.EqualTo("hello"));
                Assert.That(Encoding.Default.GetString(result[1].stringValue.ToArray()), Is.EqualTo("world"));
                Assert.That(Encoding.Default.GetString(result[2].stringValue.ToArray()), Is.EqualTo("t"));
                Assert.That(Encoding.Default.GetString(result[3].stringValue.ToArray()), Is.EqualTo("values"));
            }

            [Test] public void ReadValue()
            {
                byte[] input = Encoding.ASCII.GetBytes("+hello\r\nfas;lkf;la'sf");
                var bytesRead = FastRedisValue.TryReadValue(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo("+hello\r\n".Length));
                Assert.That(Encoding.Default.GetString(result.stringValue.ToArray()), Is.EqualTo("hello"));
            }
            
            [Test] public void ReadValueWithInt()
            {
                byte[] input = Encoding.ASCII.GetBytes(":32\r\n");
                var bytesRead = FastRedisValue.TryReadValue(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(":32\r\n".Length));
                Assert.That(result.intValue, Is.EqualTo(32));
            }
            
            [Test] public void ReadValueWithArray()
            {
                byte[] input = Encoding.ASCII.GetBytes("*4\r\n+hello\r\n+world\r\n$5\r\nhello\r\n+values\r\n");
                var bytesRead = FastRedisValue.TryReadValue(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(Encoding.Default.GetString(result.arrayValue[0].stringValue.ToArray()), Is.EqualTo("hello"));
                Assert.That(Encoding.Default.GetString(result.arrayValue[1].stringValue.ToArray()), Is.EqualTo("world"));
                Assert.That(Encoding.Default.GetString(result.arrayValue[2].stringValue.ToArray()), Is.EqualTo("hello"));
                Assert.That(Encoding.Default.GetString(result.arrayValue[3].stringValue.ToArray()), Is.EqualTo("values"));
            }

            [Test] public void ReadBulkString()
            {
                byte[] input = Encoding.ASCII.GetBytes("$5\r\nhello\r\n");
                var bytesRead = FastRedisValue.TryReadBulkString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(Encoding.Default.GetString(result.Value.ToArray()), Is.EqualTo("hello"));
            }
            
            [Test] public void ReadEmptyBulkString()
            {
                byte[] input = Encoding.ASCII.GetBytes("$0\r\n\r\n");
                var bytesRead = FastRedisValue.TryReadBulkString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(Encoding.Default.GetString(result.Value.ToArray()), Is.EqualTo(""));
            }
            
            [Test] public void ReadNullBulkString()
            {
                byte[] input = Encoding.ASCII.GetBytes("$-1\r\n\r\n");
                var bytesRead = FastRedisValue.TryReadBulkString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(result, Is.Null);
            }
            
            [Test] public void ReadPartialNullBulkString()
            {
                byte[] input = Encoding.ASCII.GetBytes("$-1\r\n\r");
                var bytesRead = FastRedisValue.TryReadBulkString(new Memory<byte>(input), out var result);
                Assert.That(bytesRead, Is.EqualTo(-1));
            }
            
            [Test] public void ReadValueList()
            {
                byte[] input = Encoding.ASCII.GetBytes("$-1\r\n\r\n$5\r\nhello\r\n");
                var resultList = new List<FastRedisValue>();
                var bytesRead = FastRedisValue.TryReadValueList(new Memory<byte>(input), resultList);
                Assert.That(bytesRead, Is.EqualTo(input.Length));
                Assert.That(resultList.Count, Is.EqualTo(2));
                Assert.That(resultList[0].nullValue, Is.True);
                Assert.That(Encoding.Default.GetString(resultList[1].stringValue.ToArray()), Is.EqualTo("hello"));
            }

            [Test] public void ReadTruncatedValueList()
            {
                byte[] input = Encoding.ASCII.GetBytes("$-1\r\n\r\n$5\r\nhello\r");
                var resultList = new List<FastRedisValue>();
                var bytesRead = FastRedisValue.TryReadValueList(new Memory<byte>(input), resultList);
                Assert.That(bytesRead, Is.EqualTo("$-1\r\n\r\n".Length));
                Assert.That(resultList.Count, Is.EqualTo(1));
                Assert.That(resultList[0].nullValue, Is.True);
            }
            
            [Test] public void WriteInt()
            {
                byte[] output = new byte[1024];
                var bytesWritten = FastRedisValue.WriteIntValue(new Memory<byte>(output), 1234);
                Assert.That(bytesWritten, Is.EqualTo(6));
                Assert.That(Encoding.Default.GetString(output, 0, bytesWritten), Is.EqualTo("1234\r\n"));
            }
            
            [Test] public void WriteNegativeInt()
            {
                byte[] output = new byte[1024];
                var bytesWritten = FastRedisValue.WriteIntValue(new Memory<byte>(output), -532);
                Assert.That(bytesWritten, Is.EqualTo(6));
                Assert.That(Encoding.Default.GetString(output, 0, bytesWritten), Is.EqualTo("-532\r\n"));
            }
            
            [Test] public void WriteBulkString()
            {
                byte[] output = new byte[1024];
                var result = "$5\r\nhello\r\n";
                var bytesWritten = FastRedisValue.WriteBulkString(new Memory<byte>(output), new Memory<byte>(Encoding.Default.GetBytes("hello")));
                Assert.That(bytesWritten, Is.EqualTo(result.Length));
                Assert.That(Encoding.Default.GetString(output, 0, bytesWritten), Is.EqualTo(result));
            }
            
            [Test] public void WriteBulkStringArray()
            {
                byte[] output = new byte[1024];
                var strings = new List<Memory<byte>>();
                strings.Add(new Memory<byte>(Encoding.Default.GetBytes("hello")));
                strings.Add(new Memory<byte>(Encoding.Default.GetBytes("test")));
                strings.Add(new Memory<byte>(Encoding.Default.GetBytes("val")));
                var result = "*3\r\n$5\r\nhello\r\n$4\r\ntest\r\n$3\r\nval\r\n";
                var bytesWritten = FastRedisValue.WriteBulkStringArray(new Memory<byte>(output), strings);
                Assert.That(bytesWritten, Is.EqualTo(result.Length));
                Assert.That(Encoding.Default.GetString(output, 0, bytesWritten), Is.EqualTo(result));
            }
        }
    }
}