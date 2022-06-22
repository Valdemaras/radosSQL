using System.Data.SQLite;
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
SQLiteConnectionStringBuilder connBuilder = new SQLiteConnectionStringBuilder();

connBuilder.DataSource = "file:///test_metadata:hellodc2/metadata.db?vfs=ceph";
connBuilder.DateTimeKind = DateTimeKind.Utc;
connBuilder.PageSize = 65536;
connBuilder.CacheSize = 4096;
connBuilder.JournalMode = SQLiteJournalModeEnum.Persist;
var connection = new SqliteConnection(connBuilder.ToString());
using (connection)
{

    connection.Open();
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
            Console.WriteLine(ex.ToString());
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
    connection.Execute(sql);


    Console.WriteLine("Try select");
    IList<string> ids = (IList<string>) connection.Query<string>("Select UniqueId from MdMessage");
    foreach (var id in ids)
    {
        Console.WriteLine($"Result {id}");
    }
}

