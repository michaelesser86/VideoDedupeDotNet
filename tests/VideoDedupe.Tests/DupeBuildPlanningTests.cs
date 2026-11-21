using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class DupeBuildPlanningTests
{
    [Theory]
    // Every file shares a window with a neighbour: all of them must be hashed.
    [InlineData(new[] { 10.0, 10.2, 10.4 }, 0.25, 2, new[] { 0, 1, 2 })]
    // 10.4 is too far from 10.0 for a three-file window, but 10.2–10.4 form one with 10.3.
    [InlineData(new[] { 10.0, 10.2, 10.3, 10.4 }, 0.25, 3, new[] { 1, 2, 3 })]
    // No window reaches three files.
    [InlineData(new[] { 10.0, 10.2, 10.5 }, 0.25, 3, new int[0])]
    public void HashesOnlyFilesThatCanJoinAGroup(double[] durations, double tolerance, int minItems, int[] expected)
    {
        var files = durations.Select((d, i) => new AppDb.MediaFileRow { Id = i, DurationSec = d }).ToList();

        var needed = DupeBuildService.FilesInQualifyingWindows(files, tolerance, minItems);

        Assert.Equal(expected.Select(i => (long)i), needed.Select(f => f.Id));
    }

    [Fact]
    public void ParsesAllOptionsAndKeepsDefaultsForMalformedValues()
    {
        var opt = DupeBuildService.ParseArgs([
            "--tol=0.5", "--min=3", "--maxdist=12", "--frames=80,x,20,20,150", "--clear=false",
            "--refine-frames=4", "--scene-threshold=0.4", "--max-phash=14", "--margin=2", "--parallel=3",
            "--maxdist=oops", "positional"
        ]);

        Assert.Equal(new DupeBuildOptions(ClearExisting: false, TolSec: 0.5, MinItems: 3, MaxAvgDist: 12,
            FramePercents: opt.FramePercents, RefinementFrames: 4, SceneThreshold: 0.4, MaxPHashDist: 14,
            BorderlineMargin: 2, MaxParallelism: 3), opt);
        Assert.Equal([20, 80], opt.FramePercents!);
        Assert.Equal(3, opt.EffectiveParallelism);
        Assert.InRange(new DupeBuildOptions().EffectiveParallelism, 2, 8);
    }

    [Fact]
    public async Task HashCacheFromAnotherAlgorithmVersionIsDiscardedOnce()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = workspace.CreateFile("v.mp4") });
        var id = Assert.Single(await db.ListMediaFilesAsync()).Id;
        await db.UpsertFrameHashAsync(id, 50, 1);
        await db.UpsertVisualHashAsync(id, "scene", 0, 2);

        // Same version: the cache survives re-initialisation.
        await DbInitializer.EnsureCreatedAsync(db);
        Assert.Equal(1UL, await db.GetFrameHashAsync(id, 50));

        await db.ExecuteAsync("UPDATE Meta SET Value='old' WHERE Key='HashVersion';");
        await DbInitializer.EnsureCreatedAsync(db);

        Assert.Null(await db.GetFrameHashAsync(id, 50));
        Assert.Empty(await db.GetVisualHashesAsync(id, "scene"));
        Assert.Equal(DHashService.Version, (await db.GetMetaAsync("HashVersion")));
    }
}
