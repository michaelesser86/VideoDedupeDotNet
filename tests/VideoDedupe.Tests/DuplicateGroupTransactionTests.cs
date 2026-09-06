using Dapper;
using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class DuplicateGroupTransactionTests
{
    [Fact]
    public async Task ListsAllGroupsButExcludesDeletedMembersAndIncompleteGroups()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        for (var id = 1; id <= 6; id++)
            await db.ExecuteAsync(@"
INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc, IsDeleted)
VALUES (@Id, @Path, 0, '', '', 0);", new { Id = id, Path = $"{id}.mp4" });

        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10,
            [new() { MediaFileId = 1 }, new() { MediaFileId = 2 }]);
        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10,
            [new() { MediaFileId = 3 }, new() { MediaFileId = 4 }]);
        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10,
            [new() { MediaFileId = 5 }, new() { MediaFileId = 6 }]);
        await db.MarkMediaFileDeletedAsync(4);

        var groups = await db.ListDuplicateGroupsWithMembersAsync();
        Assert.Equal(2, groups.Count);
        Assert.DoesNotContain(groups, group => group.Members.Any(member => member.File.Id == 4));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task SavesAllOrRollsBackGroupAndMembers(bool failSecondMember, bool cancel)
    {
        var database = Path.Combine(Path.GetTempPath(), $"videodedupe-tx-{Guid.NewGuid():N}.db");
        try
        {
            var db = new AppDb(database);
            await DbInitializer.EnsureCreatedAsync(db);
            await db.ExecuteAsync(@"
INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc)
VALUES (1, 'a', 0, '', ''), (2, 'b', 0, '', '');");
            if (failSecondMember)
                await db.ExecuteAsync(@"
CREATE TRIGGER FailSecondMember BEFORE INSERT ON DuplicateMember
WHEN NEW.MediaFileId = 2
BEGIN SELECT RAISE(ABORT, 'Simulated member failure'); END;");

            using var cts = new CancellationTokenSource();
            IEnumerable<AppDb.DuplicateMemberRow> Members()
            {
                yield return new() { MediaFileId = 1, AvgDist = 0 };
                // Enumeration resumes after the first member has been inserted.
                if (cancel) cts.Cancel();
                yield return new() { MediaFileId = 2, AvgDist = 3 };
            }

            var save = db.InsertDuplicateGroupWithMembersAsync("dhash", "50", 0.25, 10, Members(), cts.Token);
            if (failSecondMember) await Assert.ThrowsAsync<SqliteException>(() => save);
            else if (cancel) await Assert.ThrowsAnyAsync<OperationCanceledException>(() => save);
            else Assert.True(await save > 0);

            using var cn = new SqliteConnection($"Data Source={database}");
            await cn.OpenAsync();
            var rollback = failSecondMember || cancel;
            Assert.Equal(rollback ? 0 : 1, await cn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM DuplicateGroup"));
            Assert.Equal(rollback ? 0 : 2, await cn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM DuplicateMember"));
            if (!rollback)
            {
                var group = Assert.Single(await db.ListDuplicateGroupsWithMembersAsync());
                Assert.Equal(await save, group.Group.Id);
                Assert.Equal(2, group.Members.Count);
                Assert.Equal(3.0, group.Members.Single(m => m.File.Id == 2).AvgDist);
            }
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
