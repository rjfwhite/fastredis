using System.Collections.Generic;
using Thor.Views;

public class EntityView
{
    public class EntityViewData
    {
        private IStreamingReader _reader;
        internal string[] _readDependencies;
        internal string[] _readIndexes;
        
        public EntityViewData(IStreamingReader reader)
        {
            _reader = reader;
        }
        public StreamingEntryUpdateExternal Update { get; internal set; }
        public string WriteIndex { get; internal set; }
        public IReadOnlyList<string> ReadDependencies => _readDependencies;
        public IReadOnlyList<string> ReadIndexes => _readIndexes;
        public IReadOnlyDictionary<string, byte[]> Fields => _reader.Data;
    }

    public string Name { get; private set; }
    public EntityViewData Data { get; private set; }
    public bool IsClosed => _reader.IsClosed;
    public bool IsValid => _reader.IsValid;

    private string _name;
    private readonly IReadIndexClassifier _readIndexClassifier;
    private readonly IReadIndexClassifier _readDependencyClassifier;
    private readonly IWriteIndexClassifier _writeIndexClassifier;
    private IStreamingReader _reader;

    public EntityView(string name, 
        IStreamingReaderFactory factory, 
        IReadIndexClassifier readIndexClassifier,
        IReadIndexClassifier readDependencyClassifier,
        IWriteIndexClassifier writeIndexClassifier)
    {
        Name = name;
        _name = name;
        _readIndexClassifier = readIndexClassifier;
        _readDependencyClassifier = readDependencyClassifier;
        _writeIndexClassifier = writeIndexClassifier;
        _reader = factory.Make(name);
        _reader.Open();
    }

    public void Close()
    {
        _reader.Close();
    }
    
    public void Tick()
    {
        if (_reader.TryReceive(out var results))
        {
            if (Data == null)
            {
                Data = new EntityViewData(_reader);
            }
            Data.Update = results;
            Data._readDependencies = _readDependencyClassifier.Classify(_reader.Data);
            Data._readIndexes = _readIndexClassifier.Classify(_reader.Data);
            Data.WriteIndex = _writeIndexClassifier.Classify(_reader.Data);
        }
        else
        {
            Data = null;
        }
    }
}