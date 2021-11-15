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

      if (TestSQL() == false)
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

    private static bool TestSQL()
    {
      Console.WriteLine("Starting Testing SQL...");

      // first query
      var reader = ExecuteQuery("select * from `member`");
      if (reader != null)
      {
        do
        {
          if (reader.HasRows == false)
          {
            break;
          }

          var fieldCount = reader.FieldCount;

          while (reader.Read())
          {
            for (var i = 0; i < fieldCount; i++)
            {
              var name = reader.GetName(i);
              var obj = reader.GetValue(i);
              Console.Write("{0}: {1}, ", name, obj.ToString());
            }

            Console.WriteLine("");
          }
        } while (reader.NextResult());

        reader.Dispose();
      }
      else
      {
        Console.Write("Query Failed!");
        return false;
      }

      // first insert
      var id1 = (int)( System.DateTime.Now.Ticks & 0x00FFFFFF );
      var date1 = System.DateTime.Now.ToString( "yyyy/MM/dd" );
      var rows = ExecuteNonQuery($"INSERT INTO member(`id`, `name`, `dob`) VALUES({id1}, {id1}, \"{date1}\")");
      if( rows != 1 )
      {
        Console.Write( "Insert Failed!" );
        return false;
      }

      // first delete
      rows = ExecuteNonQuery($"DELETE FROM member WHERE `id`={id1}");
      if( rows != 1 )
      {
        Console.Write( "delete Failed!" );
        return false;
      }

      // second insert
      var id2 = (int)( System.DateTime.Now.Ticks & 0x00FFFFFF );
      var date2 = System.DateTime.Now.ToString( "yyyy/MM/dd hh:mm:ss" );
      rows = ExecuteNonQuery($"INSERT INTO member(`id`, `name`, `dob`) VALUES({id2}, {id2}, \"{date2}\")");
      if( rows != 1 )
      {
        Console.Write( "Insert Failed!" );
        return false;
      }

      // first update
      var id3 = (int)( System.DateTime.Now.Ticks & 0x00FFFFFF );
      var date3 = System.DateTime.Now.ToString( "yyyy/MM/dd" );
      rows = ExecuteNonQuery($"UPDATE member set `name`='{id3}', `dob`='{date3}' WHERE `id`='{id2}'");
      if( rows != 1 )
      {
        Console.Write( "Update Failed!" );
        return false;
      }

      return true;
    }

    private static MySqlDataReader ExecuteQuery(string command)
    {
      var sqlCommand = new MySqlCommand("", m_connection);

      try
      {
        sqlCommand.CommandText = command;

        var dataReader = sqlCommand.ExecuteReader();

        return dataReader;
      }
      catch (MySqlException ex)
      {
        var currentFile = new System.Diagnostics.StackTrace(true).GetFrame(0).GetFileName();
        var currentLine = new System.Diagnostics.StackTrace(true).GetFrame(0).GetFileLineNumber();

        Console.WriteLine("# ERR: SQLException in " + currentFile + ":" + currentLine);
        Console.WriteLine("# ERR: " + ex.Message);
        Console.WriteLine("# ERR: MySQL error code: " + ex.Number);
        return null;
      }
      finally
      {
        sqlCommand.Dispose();
      }
    }

    private static int ExecuteNonQuery(string command)
    {
      var sqlCommand = new MySqlCommand("", m_connection);

      try
      {
        sqlCommand.CommandText = command;

        var numRows = sqlCommand.ExecuteNonQuery();

        return numRows;
      }
      catch (MySqlException ex)
      {
        var currentFile = new System.Diagnostics.StackTrace(true).GetFrame(0).GetFileName();
        var currentLine = new System.Diagnostics.StackTrace(true).GetFrame(0).GetFileLineNumber();

        Console.WriteLine("# ERR: SQLException in " + currentFile + ":" + currentLine);
        Console.WriteLine("# ERR: " + ex.Message);
        Console.WriteLine("# ERR: MySQL error code: " + ex.Number);

        return 0;
      }
      finally
      {
        sqlCommand.Dispose();
      }
    }
  }
}
