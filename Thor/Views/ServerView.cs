
using System.Linq;
using System.Text;
using Thor.Views;

public class ServerView
{
    private IStreamingReader _writeIndexAssignments;
    private IStreamingReaderFactory _factory;
    
    public readonly string Name;
    public readonly MultiIndexView WriteView;
    public readonly MultiIndexView ReadView;

    public ServerView(string writeIndexAssignmentsKey,
        string name,
        IReadIndexClassifier readIndexParticipationClassifier,
        IReadIndexClassifier readIndexDependencyClassifier,
        IWriteIndexClassifier writeIndexClassifier,
        IStreamingReaderFactory factory)
    {
        Name = name;
        _factory = factory;
        _writeIndexAssignments = _factory.Make(writeIndexAssignmentsKey);
        _writeIndexAssignments.Open();
        WriteView = new MultiIndexView(_factory, readIndexDependencyClassifier,
            readIndexParticipationClassifier, writeIndexClassifier);
        ReadView = new MultiIndexView(_factory, readIndexDependencyClassifier,
            readIndexParticipationClassifier, writeIndexClassifier);
    }

    public void Tick()
    {
        // Fetch latest assignments of write indexes to servers
        _writeIndexAssignments.TryReceive(out var result);
        
        // Materialize a view of all the entities of all of the write indexes 
        // that this server is authoritative for
        var writeIndexes = _writeIndexAssignments.Data.Where(entry => Encoding.Default.GetString(entry.Value) == Name).Select(entry => entry.Key);
        WriteView.Tick(writeIndexes);

        // Materialize a view of all the entities of all the read index dependencies
        // of the entities that the server is authoritative for 
        var readIndexes = WriteView.Entities.Values.Where(e => e.IsValid).SelectMany(e => e.Data.ReadDependencies).Distinct();
        ReadView.Tick(readIndexes);
    }
}