using Dapper;

using Microsoft.Data.Sqlite;

namespace VideoDedupe.Infrastructure
{
    public sealed class AppDb
    {
        private readonly string _dbPath;

        public AppDb(string dbPath)
        {
            _dbPath = dbPath;
        }

        private SqliteConnection Open()
        {
            var cn = new SqliteConnection($"Data Source={_dbPath}");
            cn.Open();
            return cn;
        }

        public async Task ExecuteAsync(string sql, object? args = null)
        {
            using var cn = Open();
            await cn.ExecuteAsync(sql, args);
        }
    }
}
