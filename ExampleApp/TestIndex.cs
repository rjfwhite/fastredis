using System.Text;
using Dia2Lib;
using Thor;

namespace Core;

public class TestIndex
{
    private string _key;
    private IStreamingReader _reader;
    private IStatelessStreamingWriter _writer;

    public IStreamingReader Data => _reader;
    
    public TestIndex(string key, IStatelessStreamingWriter writer, IStreamingReaderFactory factory)
    {
        _key = key;
        _reader = factory.Make(key);
        _reader.Open();
        _writer = writer;
        Console.WriteLine($"New Test Index {key}");
    }

    public void UpdateIndex(string entry, bool exists)
    {
        Utils.WriteToIndex(_writer, _key, entry, exists);
    }
    
    public void UpdateIndexValue(string entry, string value)
    {
        _writer.Send(_key, new Dictionary<string, byte[]>
            {
                { entry, Encoding.Default.GetBytes(value) }
            },
            new byte[][] { });
    }
    
    public void UpdateIndexValue(string entry, double value)
    {
        _writer.Send(_key, new Dictionary<string, byte[]>
            {
                { entry, BitConverter.GetBytes(value)}
            },
            new byte[][] { });
    }

    public void Tick()
    {
        if (_reader.TryReceive(out var result))
        {
            if (result.FieldUpdates.Count > 0)
            {
                Console.Write($"{_key} [{_reader.Data.Count}] Updated: ");
                foreach (var key in result.FieldUpdates.Keys)
                {
                    if (Utils.ReadIndex(result.FieldUpdates, key))
                    {
                        Console.Write($" +{key}");
                    }
                    else
                    {
                        Console.Write($" -{key}");
                    }
                }

                Console.Write(" | new state: ");
                
                Console.WriteLine(string.Join(",", _reader.Data.Keys.Where(key => Utils.ReadIndex(_reader.Data, key))));
            }
        }
    }
}