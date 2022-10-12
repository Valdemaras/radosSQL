using System.Diagnostics;
using Dapper;
using Microsoft.Data.Sqlite;

class Program
{
    static void loadCephSqlite()
    {
        using var test = new SqliteConnection();
        test.Open();
        test.LoadExtension("libcephsqlite.so");
        test.Close();
    }

    static void Main(string[] args)
    {
        bool isReadOnly = Boolean.Parse(args[0]);
        string dbName = args[1];
        int count = int.Parse(args[2]);
        string mode = "ReadWriteCreate";
        if (isReadOnly)
        {
            mode = "ReadOnly";
        }

        Stopwatch sw = new Stopwatch();
        loadCephSqlite();
        var connection = new SqliteConnection($"Data Source=file:///test_metadata:/{dbName}?vfs=ceph;mode={mode}");
        sw.Start();
        connection.Open();
        connection.Execute("PRAGMA page_size = 65536");
        connection.Execute("PRAGMA cache_size = 4096");
        connection.Execute("PRAGMA journal_mode = PERSIST");
        //connection.Execute("PRAGMA locking_mode = EXCLUSIVE");
        connection.Execute("PRAGMA temp_store=memory");
        sw.Stop();
        Console.WriteLine("SQL connection open time ={0}", sw.Elapsed);
        
        if (isReadOnly == false)
        {
            using (var trans = connection.BeginTransaction())
            {
                try
                {
                    connection.Execute($"CREATE TABLE Metadata (SID INT, Timestamp REAL, Data BLOB);");
                    connection.Execute($"CREATE INDEX idx_metadatadb ON Metadata (SID, Timestamp)");
                    connection.Execute($"CREATE INDEX idx_metadatatimestampdb ON Metadata (Timestamp)");
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    //Console.WriteLine(ex.ToString());
                    trans.Rollback();
                }
            }


            Console.WriteLine("Tables created successfully");
            sw.Start();
            using (var trans = connection.BeginTransaction())
            {
                Console.WriteLine("Start Inserting");
                var command = connection.CreateCommand();
                command.CommandText = @"INSERT INTO Metadata VALUES ($sid, $Timestamp, $Data)";

                var parameter_sid = command.CreateParameter();
                parameter_sid.ParameterName = "$sid";
                command.Parameters.Add(parameter_sid);

                var parameter_ts = command.CreateParameter();
                parameter_ts.ParameterName = "$Timestamp";
                command.Parameters.Add(parameter_ts);

                var parameter_val = command.CreateParameter();
                parameter_val.ParameterName = "$Data";
                command.Parameters.Add(parameter_val);

                try
                {
                    byte[] data = new byte[1024];
                    for (int i = 0; i < 1000; i++)
                    {
                        parameter_sid.Value = i + 1;
                        parameter_ts.Value = 100;
                        parameter_val.Value = data;
                        command.ExecuteNonQuery();
                        Thread.Sleep(10);
                    }

                    trans.Commit();
                }
                catch (Exception e)
                {
                    trans.Rollback();
                    Console.WriteLine(e.ToString());
                }
            }

            sw.Stop();
            Console.WriteLine("Insert Elapsed={0}", sw.Elapsed);
            
            Console.WriteLine("Many insert has finished!");
            Thread.Sleep(5000);
        }

        Console.WriteLine("Try select");
        sw.Restart();
        IList<string> ids = (IList<string>) connection.Query<string>($"Select Timestamp from Metadata limit {count}");
        sw.Stop();
        Console.WriteLine("Select Elapsed={0}", sw.Elapsed);
        Console.WriteLine($"Result {ids.Last()}");
        connection.Close();
        connection.Dispose();
        Thread.Sleep(5000);
    }
}

