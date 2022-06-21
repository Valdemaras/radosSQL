using Dapper;
using Microsoft.Data.Sqlite;

var connection = new SqliteConnection("Data Source=file:///test_metadata:hellodc1/main.db?vfs=ceph");
using (connection)
{
    connection.LoadExtension("libcephsqlite.so");
    connection.Open();
    using (var trans = connection.BeginTransaction())
    {
        try
        {
            connection.Execute("DROP TABLE MdMessage");
            connection.Execute("DROP TABLE MdMessageDb");
            connection.Execute("CREATE TABLE MdMessage (UniqueId TEXT, Class TINYINT, DID TINYINT, SID TINYINT, Timestamp REAL, Data TEXT)");
            connection.Execute("CREATE TABLE MdMessageDb (UniqueId TEXT, Key TEXT, Value TEXT)");
            connection.Execute("CREATE INDEX idx_metadatadb ON MdMessage (DID, SID, Timestamp)");
            connection.Execute("CREATE INDEX idx_metadatatimestampdb ON MdMessage (Timestamp)");
            trans.Commit();
        }
        catch (Exception)
        {
            trans.Rollback();
        }
    }
}

