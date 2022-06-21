using Dapper;
using Microsoft.Data.Sqlite;

void loadCephSqlite()
{
    var test = new SqliteConnection();
    test.Open();
    test.LoadExtension("libcephsqlite.so");
}

loadCephSqlite();
var connection = new SqliteConnection("Data Source=file:///test_metadata:hellodc1/metadata.db?vfs=ceph");
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
}

