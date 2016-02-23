using System;
using System.Collections.Generic;
using System.IO;

namespace BulkCsvInsert
{
    public class CsvQueueReader
    {
        private Queue<string> _csvQueue;

        public int LoadFile(string filename, bool removeHeader)
        {
            _csvQueue = new Queue<string>();
            foreach (var line in File.ReadLines(filename))
            {
                _csvQueue.Enqueue(line);
            }
            if (removeHeader) RemoveHeader(_csvQueue);
            return _csvQueue.Count;
        }

        public IEnumerable<string> Read(int maxCount = 1000)
        {
            if (_csvQueue.Count == 0) return null;
            var list  = new List<string>();
            for (var i = 0; i < maxCount; i++)
            {
                if (_csvQueue.Count == 0) break;
                list.Add(_csvQueue.Dequeue());
            }
            return list;
        }

        private static void RemoveHeader(Queue<string> lineQueue)
        {
            lineQueue.Dequeue();
        }
    }
}