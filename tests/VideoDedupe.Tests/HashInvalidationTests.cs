using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class HashInvalidationTests
{
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, true)]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(true, false, true)]
    public async Task InvalidatesOnlyChangedFileAndPreservesUnchangedCache(bool stub, bool sizeChanged, bool timeChanged)
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        var modified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var row = new AppDb.MediaFileRow
        {
            Path = workspace.CreateFile("video.mp4"), SizeBytes = 3,
            ModifiedUtc = modified.ToString("O"), LastScannedUtc = modified.ToString("O")
        };
        await db.UpsertMediaFileAsync(row);
        var id = Assert.Single(await db.ListMediaFilesAsync()).Id;
        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = workspace.CreateFile("other.mp4") });
        var otherId = (await db.ListMediaFilesAsync()).Single(f => f.Id != id).Id;
        foreach (var position in new[] { 20, 50, 80 })
            await db.UpsertFrameHashAsync(id, position, ulong.MaxValue);
        await db.UpsertVisualHashAsync(id, "scene-phash-v1", 0, 77);
        await db.UpsertFrameHashAsync(otherId, 50, 42);

        if (sizeChanged) row.SizeBytes++;
        if (timeChanged) modified = modified.AddSeconds(1);
        row.ModifiedUtc = modified.ToString("O");
        row.LastScannedUtc = modified.AddHours(1).ToString("O");
        if (stub) await db.UpsertMediaFileStubAsync(row.Path, row.SizeBytes, modified);
        else await db.UpsertMediaFileAsync(row);

        var updated = (await db.ListMediaFilesAsync()).Single(f => f.Path == row.Path);
        Assert.Equal(id, updated.Id);
        Assert.Equal(row.SizeBytes, updated.SizeBytes);
        Assert.Equal(row.ModifiedUtc, updated.ModifiedUtc);
        foreach (var position in new[] { 20, 50, 80 })
        {
            var hash = await db.GetFrameHashAsync(id, position);
            if (sizeChanged || timeChanged) Assert.Null(hash);
            else Assert.Equal(ulong.MaxValue, hash);
        }
        var visualHash = await db.GetVisualHashesAsync(id, "scene-phash-v1");
        if (sizeChanged || timeChanged) Assert.Empty(visualHash);
        else Assert.Equal(77UL, visualHash[0]);
        Assert.Equal(42UL, await db.GetFrameHashAsync(otherId, 50));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedMetadataUpdateRollsBackHashDeletion(bool stub)
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        var modified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var row = new AppDb.MediaFileRow
        {
            Path = workspace.CreateFile("video.mp4"), SizeBytes = 3, ModifiedUtc = modified.ToString("O")
        };
        await db.UpsertMediaFileAsync(row);
        var id = Assert.Single(await db.ListMediaFilesAsync()).Id;
        await db.UpsertFrameHashAsync(id, 50, 42);
        await db.ExecuteAsync(@"
CREATE TRIGGER FailUpdate BEFORE UPDATE ON MediaFile
BEGIN SELECT RAISE(ABORT, 'Simulated update failure'); END;");
        row.SizeBytes++;
        await Assert.ThrowsAsync<SqliteException>(() => stub
            ? db.UpsertMediaFileStubAsync(row.Path, row.SizeBytes, modified)
            : db.UpsertMediaFileAsync(row));
        Assert.Equal(3, Assert.Single(await db.ListMediaFilesAsync()).SizeBytes);
        Assert.Equal(42UL, await db.GetFrameHashAsync(id, 50));
    }
}
