using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BulkCsvInsert
{
    public class ErrorQueueHandler
    {
        private readonly Queue<string> _errorQueue;

        public ErrorQueueHandler()
        {
            _errorQueue = new Queue<string>();
        }

        public void AddError(IEnumerable<string> errorLines)
        {
            foreach (var errorLine in errorLines)
            {
                _errorQueue.Enqueue(errorLine);
            }
        }

        public bool WriteErrorFile()
        {
            if (_errorQueue.Count == 0) return false;

            var file = File.CreateText($"ErrorProcessedFile");
            foreach (var line in _errorQueue)
            {
                file.WriteLine(line);
            }
            Console.WriteLine("Archivo de errores fue creado.");
            file.Flush();
            file.Close();

            return true;
        }
    }
}