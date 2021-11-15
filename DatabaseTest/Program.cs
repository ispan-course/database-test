using System;
using MySql.Data.MySqlClient;

namespace DatabaseTest
{
  internal class Program
  {
    private static MySqlConnection m_connection;

    public static void Main(string[] args)
    {
      // set up connection info and connect to db
      const string DBHost = "127.0.0.1";
      const int DBPort = 3306;
      const string DBAccount = "root";
      const string DBPasswd = "";
      const string DBName = "db_test";

      if (ConnectDB(DBHost, DBPort, DBAccount, DBPasswd, DBName) == false)
      {
        return;
      }

      m_connection.Close();
      m_connection.Dispose();
    }

    private static  bool ConnectDB(string hostAddress, int port, string account, string password, string dbName)
    {
      var connectionString = $"server={hostAddress};user={account};database={dbName};port={port};password={password};allow zero datetime=true;";

      try
      {
        m_connection = new MySqlConnection(connectionString);
        m_connection.Open();

        Console.WriteLine("DB connected...");
        return true;
      }
      catch (MySqlException ex)
      {
        var currentFile = new System.Diagnostics.StackTrace(true).GetFrame(0).GetFileName();
        var currentLine = new System.Diagnostics.StackTrace(true).GetFrame(0).GetFileLineNumber();
        
        Console.WriteLine("# ERR: SQLException in " + currentFile + ":" + currentLine);
        Console.WriteLine("# ERR: " + ex.Message);
        Console.WriteLine("# ERR: MySQL error code: " + ex.Number);

        return false;
      }
    }
  }
}
