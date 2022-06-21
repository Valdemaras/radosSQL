using System.Data.SQLite;
using Dapper;

SQLiteConnectionStringBuilder connBuilder = new SQLiteConnectionStringBuilder
{
    DataSource = "file:///test_metadata:hellodc1/main.db?vfs=ceph",
    DateTimeKind = DateTimeKind.Utc,
    PageSize = 65536,
    JournalMode = SQLiteJournalModeEnum.Wal
};


var connection = new SQLiteConnection(connBuilder.ToString(), true);
using (connection)
{
    connection.LoadExtension("libcephsqlite.so");
    connection.Open();
    connection.SetChunkSize(100);
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

