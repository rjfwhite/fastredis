using System.Diagnostics;
using Core.Rdis;
using Thor;
using Thor.Views;

namespace Core;

public class TestServer
{
    private readonly string _mainIndex;
    private readonly string _name;
    private readonly RedisSubscriber _subscriber;
    private readonly RedisReceiver _receiver;
    private readonly ServerView _view;

    public ServerView View => _view;

    private Stopwatch _timer;
    private IStatelessStreamingWriter _writer;
    public TestServer(
        string mainIndex,
        string name,
        RedisSubscriber subscriber,
        RedisReceiver receiver,
        IReadIndexClassifier readIndexParticipationClassifier,
        IReadIndexClassifier readIndexDependencyClassifier,
        IWriteIndexClassifier writeIndexClassifier,
        IStreamingReaderFactory factory, IStatelessStreamingWriter writer)
    {
        _mainIndex = mainIndex;
        _name = name;
        _subscriber = subscriber;
        _receiver = receiver;
        _writer = writer;
        _timer = new Stopwatch();
        _view = new ServerView(mainIndex,
            name,
            readIndexParticipationClassifier,
            readIndexDependencyClassifier,
            writeIndexClassifier,
            factory);
    }



    public void Tick()
    {
        _subscriber.Tick();
        _receiver.Tick();
        
        _view.Tick();

        double dt = (long)_timer.ElapsedMilliseconds / 1000.0;
        
        // do flocking
        foreach (var entity in _view.WriteView.Entities.Where(e => e.Value.IsValid))
        {
            Console.WriteLine($"TICKING {entity.Key}");
            
            var fields = entity.Value.Data.Fields;
            double x = BitConverter.ToDouble(fields["x"]);
            double y = BitConverter.ToDouble(fields["y"]);
            double vx = BitConverter.ToDouble(fields["vx"]);
            double vy = BitConverter.ToDouble(fields["vy"]);

            double newX = x + vx * dt;
            double newY = y + vy * dt;
            
            _writer.Send(entity.Key, new Dictionary<string, byte[]>
            {
                {"x", BitConverter.GetBytes(newX)},
                {"y", BitConverter.GetBytes(newY)}
            }, new byte[][]{});
            
            Console.WriteLine($"moved {entity} to x:{newX} y:{newY} {_view.WriteView.Entities[entity.Key].Data.WriteIndex}");
        }
        

        // Handle read index and write index updates
        foreach (var entity in _view.WriteView.Entities.Keys)
        {
            if (_view.WriteView.Entities[entity].IsValid)
            {
                if (_view.ReadView.EntityIndexes.ContainsKey(entity))
                {
                    Utils.DiffStringList(_view.ReadView.EntityIndexes[entity], _view.WriteView.Entities[entity].Data.ReadIndexes, out var readIndexesToAdd, out var readIndexesToRemove);
                    foreach (var readIndex in readIndexesToAdd)
                    {
                        Console.WriteLine($"Adding Entity {entity} to read index {readIndex}");
                        Utils.WriteToIndex(_writer, readIndex, entity, true);
                    }
                    foreach (var readIndex in readIndexesToRemove)
                    {
                        Console.WriteLine($"Removing Entity {entity} from read index {readIndex}");
                        Utils.WriteToIndex(_writer, readIndex, entity, false);
                    }
                }

                var currentAuthority = _view.WriteView.EntityIndexes[entity][0];
                var desiredAuthority = _view.WriteView.Entities[entity].Data.WriteIndex;
                if (currentAuthority != desiredAuthority)
                {
                    Console.WriteLine($"Migrating Entity {entity} from {currentAuthority} to {desiredAuthority}");
                    Utils.WriteToIndex(_writer, currentAuthority, entity, false);
                    Utils.WriteToIndex(_writer,desiredAuthority, entity, true);
                }   
            }
        }
        
        _timer.Restart();
    }
}