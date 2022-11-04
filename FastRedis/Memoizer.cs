using System.Collections.Generic;
using System.Text;

namespace FastRedis
{
    public static class Memoizer
    {
        private static Dictionary<string, byte[]> _stringToByte = new();
        private static Dictionary<int, byte[]> _intToStringByte = new();

        public static byte[] ToBytes(string value)
        {
            if (!_stringToByte.ContainsKey(value))
            {
                _stringToByte.Add(value, Encoding.Default.GetBytes(value));
            }

            return _stringToByte[value];
        }
        
        public static byte[] ToStringBytes(int value)
        {
            if (!_intToStringByte.ContainsKey(value))
            {
                _intToStringByte.Add(value, Encoding.Default.GetBytes(value.ToString()));
            }

            return _intToStringByte[value];
        }
    }
}