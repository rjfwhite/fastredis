using System;

namespace Thor.Optimization
{
    public ref struct ByteSpan
    {
        public Span<byte> Span;
        public long Tick;
    }
}