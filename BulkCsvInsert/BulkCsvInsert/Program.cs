using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace BulkCsvInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Iniciando la carga");
            var startTime = DateTime.Now;
            var csvQueueReader = new CsvQueueReader();
            var count = csvQueueReader.LoadFile(ConfigurationManager.AppSettings["filePath"], true);

            Console.WriteLine($"Se cargaron {count} en {(DateTime.Now - startTime).Milliseconds} milisegundos");

            var csvData = csvQueueReader.Read(100);
            var processor = new MySqlProcessor();
            var errorQueue = new ErrorQueueHandler();
            var index = 0;
            while (csvData != null)
            {
                Console.WriteLine($"Insertando {index}");
                var response = processor.Insert(CsvQueryBuilder.GenerateInsert(csvData));

                if (response == -1)
                {
                    break;
                    errorQueue.AddError(csvData);   
                }
                
                index += csvData.Count();

                Console.WriteLine($"Procesado el {(index*100/count)}% {(response == -1 ? "Error":"Exito!")}");

                csvData = csvQueueReader.Read(100);
            }

            errorQueue.WriteErrorFile();
            processor.Dispose();

            Console.ReadLine();

        }
    }
}
