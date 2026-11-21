using Dapper;
using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class ReferentialIntegrityTests
{
    [Fact]
    public async Task ClearingGroupsAlsoRemovesTheirMembers()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await db.ExecuteAsync(@"
INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc)
VALUES (1, 'a', 0, '', ''), (2, 'b', 0, '', '');");
        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10,
            [new() { MediaFileId = 1 }, new() { MediaFileId = 2 }]);

        await db.ClearDuplicateGroupsAsync();

        Assert.Equal(0, await CountAsync(workspace, "DuplicateMember"));
    }

    private static async Task<int> CountAsync(TestWorkspace workspace, string table)
    {
        using var cn = new SqliteConnection($"Data Source={Path.Combine(workspace.Root, "test.db")}");
        await cn.OpenAsync();
        return await cn.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {table}");
    }
}
