using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class ScanRootTests
{
    [Theory]
    [InlineData("MiXeD/Videos")]
    [InlineData("MiXeD/Videos/")]
    [InlineData("MiXeD/../Videos")]
    [InlineData(null)]
    public async Task ReturnsInsertedIdAndStoresAbsolutePath(string? input)
    {
        var database = Path.Combine(Path.GetTempPath(), $"videodedupe-test-{Guid.NewGuid():N}.db");
        try
        {
            var db = new AppDb(database);
            await DbInitializer.EnsureCreatedAsync(db);
            var path = input ?? Path.GetPathRoot(Path.GetFullPath("."))!;

            var id = await db.AddScanRootAsync(path);
            var root = Assert.Single(await db.ListScanRootsAsync());

            Assert.True(id > 0);
            Assert.Equal(root.Id, id);
            Assert.Equal(Path.TrimEndingDirectorySeparator(Path.GetFullPath(path)), root.Path);
            await db.SetScanRootEnabledAsync(id, false);
            Assert.Equal(0, Assert.Single(await db.ListScanRootsAsync()).IsEnabled);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(database);
            File.Delete(database + "-wal");
            File.Delete(database + "-shm");
        }
    }
}
