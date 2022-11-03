using System.Collections.Generic;
using System.Linq;
using Thor;

namespace Thor.Views
{
    public interface IReadIndexClassifier
    {
        string[] Classify(IReadOnlyDictionary<string, byte[]> data);
    }

    public interface IWriteIndexClassifier
    {
        string Classify(IReadOnlyDictionary<string, byte[]> data);
    }

// Takes a list of indexes and manifests them into a list of entities
// Takes as a tick the added/removed indexes, and returns added/removed entities
    public class MultiIndexView
    {
        private Dictionary<string, IStreamingReader> _indexes = new();
        private Dictionary<string, StreamingEntryUpdateExternal> _indexUpdates = new();
        private List<string> _indexesAdded = new();
        private List<string> _indexesRemoved = new();

        private Dictionary<string, EntityView> _entities = new();
        private Dictionary<string, List<string>> _entityIndexes = new();

        private List<string> _entitiesAdded = new();
        private List<string> _entitiesRemoved = new();

        private IStreamingReaderFactory _factory;
        private IReadIndexClassifier _dependencyClassifier;
        private IReadIndexClassifier _readIndexClassifier;
        private IWriteIndexClassifier _writeIndexClassifier;

        public IReadOnlyDictionary<string, IStreamingReader> Indexes => _indexes;
        public IReadOnlyDictionary<string, StreamingEntryUpdateExternal> IndexUpdates => _indexUpdates;
        public IReadOnlyList<string> IndexesAdded => _indexesAdded;
        public IReadOnlyList<string> IndexesRemoved => _indexesRemoved;
    
        public IReadOnlyDictionary<string, EntityView> Entities => _entities;
        public IReadOnlyDictionary<string, List<string>> EntityIndexes => _entityIndexes;
        public IReadOnlyList<string> EntitiesAdded => _entitiesAdded;
        public IReadOnlyList<string> EntitiesRemoved => _entitiesRemoved;
        public MultiIndexView(
            IStreamingReaderFactory factory, 
            IReadIndexClassifier dependencyClassifier, 
            IReadIndexClassifier readIndexClassifier, 
            IWriteIndexClassifier writeIndexClassifier)
        {
            _factory = factory;
            _dependencyClassifier = dependencyClassifier;
            _readIndexClassifier = readIndexClassifier;
            _writeIndexClassifier = writeIndexClassifier;
        }

        public void Tick(IEnumerable<string> indexes)
        {   
            _indexUpdates.Clear();
            _indexesAdded.Clear();
            _indexesRemoved.Clear();
        
            _entitiesAdded.Clear();
            _entitiesRemoved.Clear();

            Utils.DiffStringList(_indexes.Keys, indexes, out var indexesAdded, out var indexesRemoved);
        
            foreach (var index in indexesAdded)
            {
                _indexesAdded.Add(index);
                _indexes[index] = _factory.Make(index);
                _indexes[index].Open();
            }
        
            foreach (var index in indexesRemoved)
            {
                _indexesRemoved.Add(index);
                _indexes[index].Close();
                _indexes.Remove(index);
            }
        
            foreach (var index in _indexes)
            {
                if (index.Value.TryReceive(out var result))
                {
                    _indexUpdates.Add(index.Key, result);
                }
            }

            // show which indexes each entity is in
            _entityIndexes.Clear();
            foreach (var index in _indexes)
            {
                foreach (var entity in index.Value.Data.Keys)
                {
                    if (!_entityIndexes.ContainsKey(entity))
                    {
                        _entityIndexes.Add(entity, new List<string>());
                    }
                    _entityIndexes[entity].Add(index.Key);
                }
            }

            var entities = _indexes.Values
                .Where(index => index.IsValid)
                .SelectMany(index => index.Data.Keys)
                .Distinct();
        
            Utils.DiffStringList(_entities.Keys, entities, out var entitiesAdded, out var entitiesRemoved);

            foreach (var entity in entitiesAdded)
            {
                _entities[entity] = new EntityView(entity, 
                    _factory, 
                    _readIndexClassifier, 
                    _dependencyClassifier,
                    _writeIndexClassifier);
            }

            foreach (var entity in entitiesRemoved)
            {
                _entities[entity].Close();
                _entities.Remove(entity);
            }

            foreach (var entityView in _entities.Values)
            {
                entityView.Tick();
            }
        }
    }
}