using System.Diagnostics;
using Dapper;
using Microsoft.Data.Sqlite;

void loadCephSqlite()
{
    using var test = new SqliteConnection();
    test.Open();
    test.LoadExtension("libcephsqlite.so");
    test.Close();
}

loadCephSqlite();
var connection = new SqliteConnection("Data Source=file:///test_metadata:hellodc2/metadata.db?vfs=ceph");
using (connection)
{
    connection.Open();
    connection.Execute("PRAGMA page_size = 65536");
    connection.Execute("PRAGMA cache_size = 4096");
    connection.Execute("PRAGMA journal_mode = PERSIST");
    connection.Execute("PRAGMA locking_mode = EXCLUSIVE");
    connection.Execute("PRAGMA temp_store=memory");

    using (var trans = connection.BeginTransaction())
    {
        try
        {
            connection.Execute("CREATE TABLE MdMessage (UniqueId TEXT, Class TINYINT, DID INT, SID INT, Timestamp REAL, Data TEXT)");
            connection.Execute("CREATE TABLE MdMessageDb (UniqueId TEXT, Key TEXT, Value TEXT)");
            connection.Execute("CREATE INDEX idx_metadatadb ON MdMessage (DID, SID, Timestamp)");
            connection.Execute("CREATE INDEX idx_metadatatimestampdb ON MdMessage (Timestamp)");
            trans.Commit();
        }
        catch (Exception ex)
        {
            //Console.WriteLine(ex.ToString());
            trans.Rollback();
        }
    }
    
    Console.WriteLine("Tables created successfully");

    string uid = Guid.NewGuid().ToString();
    int classId = 0;
    int did = 1;
    int sid = 1;
    double ts = 100;
    string val = "Properties\":{\"Method\":\"SET_PARAMETER\"";
    string sql = $"insert into MdMessage(UniqueId, Class, DID, SID, Timestamp, Data) values('{uid}', {classId}, {did}, {sid}, {ts}, '{val}')";
    Console.WriteLine(sql);
    
    
    Stopwatch sw = new Stopwatch();
    sw.Start();
    using (var trans = connection.BeginTransaction())
    {
        try
        {
            for (int i = 0; i < 1000; i++)
            {
                connection.Execute(sql);
            }
            
            trans.Commit();
        } catch (Exception e)
        {
            trans.Rollback();
            Console.WriteLine(e.ToString());
        }
    }
    sw.Stop();
    Console.WriteLine("Insert Elapsed={0}",sw.Elapsed);

    
    Console.WriteLine("Try select");
    
    sw.Start();
    IList<string> ids = (IList<string>) connection.Query<string>("Select UniqueId from MdMessage");
    sw.Stop();
    Console.WriteLine("Select Elapsed={0}",sw.Elapsed);
    /*
    foreach (var id in ids)
    {
        Console.WriteLine($"Result {id}");
    }
    */
}

