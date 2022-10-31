using System.Text;

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
        var keys = dictionary.Keys.Select(key => Encoding.Default.GetBytes(key)).ToArray();
        var values = dictionary.Values.ToArray();
        WriteByteList(writer, keys);
        WriteByteList(writer, values);
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