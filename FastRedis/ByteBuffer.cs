﻿using System;
using System.Collections.Generic;

namespace FastRedis
{
    public class ByteBuffer
    {
        // TODO: do something smarter than a big fixed chunk of memory here
        public byte[] Data = new byte[500000];
        public int Head = 0;

        public void Add(byte[] value)
        {
            Buffer.BlockCopy(value, 0, Data, Head, value.Length);
            Head += value.Length;
        }
        
        public void Add(byte[] value, int offset, int length)
        {
            Buffer.BlockCopy(value, offset, Data, Head, length);
            Head += length;
        }
        
        public void Add(Memory<byte> buffer)
        {
            //TODO: assert this doesn't overflow
            buffer.CopyTo(new Memory<byte>(Data, Head, buffer.Length));
            Head += buffer.Length;
        }
        
        public void Add(byte value)
        {
            Data[Head++] = value;
        }

        public void Reset()
        {
            Head = 0;
        }
    }

    public static class ByteBufferPool
    {
        private static Queue<ByteBuffer> _freeBuffers = new();
        
        public static ByteBuffer Get()
        {
           return _freeBuffers.Dequeue();
        }

        public static void Return(ByteBuffer buffer)
        {
            buffer.Reset();
            _freeBuffers.Enqueue(buffer);
        }
    }
}