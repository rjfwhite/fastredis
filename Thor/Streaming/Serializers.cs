using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Thor.Optimization;

namespace Core.Streaming
{
    public static class Serializers
    {
        public static byte[][] ReadByteList(BinaryReader reader)
        {
            var length  = reader.ReadInt32();
            byte[][] result = new byte[length][];
            for (var i = 0; i < length; i++)
            {
                var entryLength = reader.ReadInt32();
                result[i] = reader.ReadBytes(entryLength);
            }
            return result;
        }

        public static void WriteByteList(BinaryWriter writer, byte[][] list)
        {
            writer.Write(list.Length);
            for (var i = 0; i < list.Length; i++)
            {
                writer.Write(list[i].Length);
                writer.Write(list[i]);
            }
        }
    
        public static IReadOnlyDictionary<string, byte[]> ReadDictionary(BinaryReader reader)
        {
            var keys = ReadByteList(reader).Select(key => Encoding.Default.GetString(key)).ToArray();
            var values = ReadByteList(reader);

            Dictionary<string, byte[]> result = new Dictionary<string, byte[]>();
            for (var i = 0; i < keys.Length; i++)
            {
                result.Add(keys[i], values[i]);
            }
            return result;
        }

        public static void WriteDictionary(BinaryWriter writer, IReadOnlyDictionary<string, byte[]> dictionary)
        {
            writer.Write(dictionary.Count);
            foreach (var key in dictionary.Keys)
            {
                var keyBytes = Memoizer.ToBytes(key);
                writer.Write(keyBytes.Length);
                writer.Write(keyBytes);
            }
            
            writer.Write(dictionary.Count);
            foreach (var value in dictionary.Values)
            {
                writer.Write(value.Length);
                writer.Write(value);
            }
        }

        public static StreamingEntryUpdate ReadUpdate(BinaryReader reader)
        {
            var epoch = reader.ReadInt64();
            var fieldUpdates = ReadDictionary(reader);
            var events = ReadByteList(reader);
            return new StreamingEntryUpdate{Epoch = epoch, FieldUpdates = fieldUpdates, Events = events};
        }

        public static void WriteUpdate(BinaryWriter writer, StreamingEntryUpdate update)
        {
            writer.Write(update.Epoch);
            WriteDictionary(writer, update.FieldUpdates);
            WriteByteList(writer, update.Events);
        }
    }
}