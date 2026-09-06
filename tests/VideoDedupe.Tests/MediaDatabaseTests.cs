using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class MediaDatabaseTests
{
    [Fact]
    public async Task UpsertPreservesIdentityAndUpdatesMetadataAndCandidateEligibility()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await DbInitializer.EnsureCreatedAsync(db);
        var row = new AppDb.MediaFileRow { Path = workspace.CreateFile("video.mp4"), DurationSec = 10 };
        await db.UpsertMediaFileAsync(row);
        var id = Assert.Single(await db.ListMediaFilesForCandidatesAsync()).Id;
        row.DurationSec = 20;
        row.Width = 1920;
        row.Height = 1080;
        row.SizeBytes = 123;
        await db.UpsertMediaFileAsync(row);
        var updated = Assert.Single(await db.ListMediaFilesForCandidatesAsync());
        Assert.Equal(id, updated.Id);
        Assert.Equal(20.0, updated.DurationSec);
        Assert.Equal(1920, updated.Width);
        Assert.Equal(1080, updated.Height);
        Assert.Equal(123, updated.SizeBytes);
        await db.MarkMediaFileDeletedAsync(id);
        Assert.Empty(await db.ListMediaFilesForCandidatesAsync());
    }

    [Fact]
    public async Task HashCachePreservesAll64BitsAndSeparatesFramePositions()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = workspace.CreateFile("video.mp4") });
        var id = Assert.Single(await db.ListMediaFilesAsync()).Id;
        Assert.Null(await db.GetFrameHashAsync(id, 50));
        await db.UpsertFrameHashAsync(id, 50, ulong.MaxValue);
        await db.UpsertFrameHashAsync(id, 80, 0x8000000000000000UL);
        Assert.Equal(ulong.MaxValue, await db.GetFrameHashAsync(id, 50));
        Assert.Equal(0x8000000000000000UL, await db.GetFrameHashAsync(id, 80));
        await db.UpsertFrameHashAsync(id, 50, 0);
        Assert.Equal(0UL, await db.GetFrameHashAsync(id, 50));
        Assert.Equal(0x8000000000000000UL, await db.GetFrameHashAsync(id, 80));
    }
}
