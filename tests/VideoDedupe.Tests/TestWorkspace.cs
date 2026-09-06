using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

internal sealed class TestWorkspace : IDisposable
{
    public string Root { get; } = Path.Combine(Path.GetTempPath(), $"videodedupe-{Guid.NewGuid():N}");
    public AppDb Db { get; }

    public TestWorkspace()
    {
        Directory.CreateDirectory(Root);
        Db = new AppDb(Path.Combine(Root, "test.db"));
    }

    public string CreateFile(string relativePath)
    {
        var path = Path.GetFullPath(Path.Combine(Root, relativePath));
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllBytes(path, new byte[] { 1, 2, 3 });
        return path;
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        Directory.Delete(Root, recursive: true);
    }
}
