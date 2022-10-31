using System.Collections.Generic;
using System.Text;

namespace Thor
{
    public static class Utils
    {
        public static void DiffStringList(
            IEnumerable<string> oldList, 
            IEnumerable<string> newList, 
            out IEnumerable<string> added, 
            out IEnumerable<string> removed)
        {
            var oldSet = new HashSet<string>(oldList);
            var newSet = new HashSet<string>(newList);

            var addedStuff = new List<string>();
            var removedStuff = new List<string>();

            foreach (var entry in oldList)
            {
                if (!newSet.Contains(entry))
                {
                    removedStuff.Add(entry);
                }
            }
        
            foreach (var entry in newList)
            {
                if (!oldSet.Contains(entry))
                {
                    addedStuff.Add(entry);
                }
            }

            added = addedStuff;
            removed = removedStuff;
        }
    
        public static void WriteToIndex(IStatelessStreamingWriter writer, string index, string key, bool exists)
        {
            var no = new byte[] { };
            var yes = new byte[] { 1 };

            writer.Send(index, new Dictionary<string, byte[]>
                {
                    { key, exists ? yes : no}
                },
                new byte[][] { });
        }

        public static bool ReadIndex(IReadOnlyDictionary<string, byte[]> index, string key)
        {
            return (index.ContainsKey(key) && index[key].Length > 0);
        }
    }
}