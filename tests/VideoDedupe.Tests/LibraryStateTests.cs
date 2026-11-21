using Dapper;
using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class LibraryStateTests
{
    [Fact]
    public async Task QuarantineRemovesFileFromGroupsAndKeepsTheDecision()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await db.ExecuteAsync(@"
INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc)
VALUES (1, 'a', 0, '', ''), (2, 'b', 0, '', ''), (3, 'c', 0, '', '');");
        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10,
            [new() { MediaFileId = 1 }, new() { MediaFileId = 2 }, new() { MediaFileId = 3 }]);
        await db.UpsertDecisionAsync(1, "keep");

        await db.MarkMediaFileQuarantinedAsync(3, "q/c");

        var group = Assert.Single(await db.ListDuplicateGroupsWithMembersAsync());
        Assert.DoesNotContain(group.Members, m => m.File.Id == 3);
        var decisions = await db.GetDecisionsAsync([1, 2, 3]);
        Assert.Equal("keep", decisions[1]);
        Assert.Equal("quarantine", decisions[3]);
        Assert.False(decisions.ContainsKey(2));
    }

    [Fact]
    public async Task IndexMarksVanishedFilesDeletedAndRestoresReturningFiles()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        var kept = workspace.CreateFile("lib/kept.mp4");
        var vanished = workspace.CreateFile("lib/vanished.mp4");
        await db.AddScanRootAsync(Path.Combine(workspace.Root, "lib"));
        // A file outside every scan root must not be touched by the index.
        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = Path.Combine(workspace.Root, "elsewhere.mp4") });
        var service = new IndexService(db,
            new FfprobeService(new FfmpegTools(ffprobePath: Path.Combine(workspace.Root, "missing-ffprobe"))));

        await service.RunAsync(null, CancellationToken.None);
        File.Delete(vanished);
        await service.RunAsync(null, CancellationToken.None);

        Assert.Equal(0, await IsDeletedAsync(workspace, kept));
        Assert.Equal(1, await IsDeletedAsync(workspace, vanished));
        Assert.Equal(0, await IsDeletedAsync(workspace, Path.Combine(workspace.Root, "elsewhere.mp4")));

        workspace.CreateFile("lib/vanished.mp4");
        await service.RunAsync(null, CancellationToken.None);
        Assert.Equal(0, await IsDeletedAsync(workspace, vanished));
    }

    private static async Task<long> IsDeletedAsync(TestWorkspace workspace, string path)
    {
        using var cn = new SqliteConnection($"Data Source={Path.Combine(workspace.Root, "test.db")}");
        await cn.OpenAsync();
        return await cn.ExecuteScalarAsync<long>("SELECT IsDeleted FROM MediaFile WHERE Path=@Path", new { Path = path });
    }
}
