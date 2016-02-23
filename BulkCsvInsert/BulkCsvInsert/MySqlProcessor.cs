using System;
using System.Configuration;
using System.Data;
using System.IO;
using MySql.Data.MySqlClient;

namespace BulkCsvInsert
{
    public class MySqlProcessor: IDisposable
    {
        private string _connectionString = ConfigurationManager.AppSettings["stringConnection"];
        private MySqlConnection _connection;

        public MySqlProcessor()
        {
            _connection = new MySqlConnection(_connectionString);
            _connection.Open();
        }


        public int Insert(string query)
        {
            try
            {
                if (_connection.State != ConnectionState.Open) OpenConnection();
                var cmd = _connection.CreateCommand();
                cmd.CommandText = query;
                return cmd.ExecuteNonQuery();
            }
            catch (Exception exception)
            {
                Console.Write($"Error: {exception.Message}");
                DumpQuery(query);

                return -1;
            }
        }

        private void DumpQuery(string query)
        {
            var file = File.AppendText("ErrorQuery");
            file.WriteLine(query);
            file.Flush();
            file.Close();
        }


        private void OpenConnection()
        {
            _connection.Open();
        }

        private void CloseConnection()
        {
            if (_connection != null && _connection.State == ConnectionState.Open)
                _connection.Close();
        }

        public void Dispose()
        {
            CloseConnection();
            _connection = null;
        }
    }
}