using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class DetectionTests
{
    [Theory]
    [InlineData(0.25, 0, 2, 1)]
    [InlineData(0.01, 0, 2, 0)]
    [InlineData(0.25, 0, 3, 0)]
    [InlineData(0.25, 64, 2, 1)]
    public async Task FindsDuplicatesAfterUnrelatedSeedAndHonorsThresholds(double tolerance,
        double distance, int minItems, int expectedGroups)
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        for (var i = 0; i < 3; i++)
        {
            await db.UpsertMediaFileAsync(new AppDb.MediaFileRow
            {
                Path = workspace.CreateFile($"{i}.mp4"), DurationSec = 10.04 + i * 0.1,
                Width = i == 0 ? null : i * 640, Height = i == 0 ? null : i * 360
            });
        }
        var files = (await db.ListMediaFilesForCandidatesAsync()).OrderBy(f => f.Path).ToList();
        Assert.Equal(3, files.Count);
        foreach (var file in files)
            foreach (var position in new[] { 20, 50, 80 })
                await db.UpsertFrameHashAsync(file.Id, position, file.Id == files[0].Id ? 0UL : ulong.MaxValue);

        // Cached hashes must suffice; invoking this missing executable would prevent detection.
        var service = new DupeBuildService(db,
            new FfmpegFrameExtractor(new FfmpegTools(ffmpegPath: Path.Combine(workspace.Root, "missing-ffmpeg"))),
            new DHashService());
        var reports = new List<DupeBuildProgress>();
        var count = await service.RunAsync(new DupeBuildOptions(TolSec: tolerance,
            MaxAvgDist: distance, MinItems: minItems), new InlineProgress(reports), CancellationToken.None);

        Assert.Equal(expectedGroups, count);
        var done = Assert.Single(reports, p => p.Phase == "Done");
        Assert.Equal(files.Count, done.OverallTotal);
        Assert.Equal(files.Count, done.OverallDone);
        var groups = await db.ListDuplicateGroupsWithMembersAsync();
        Assert.Equal(expectedGroups, groups.Count);
        if (count > 0)
        {
            var group = Assert.Single(groups);
            var expected = distance == 64 ? files : files.Skip(1).ToList();
            Assert.Equal(expected.Select(f => f.Id).Order(), group.Members.Select(m => m.File.Id).Order());
            Assert.Equal(tolerance, group.Group.TolSec);
            Assert.Equal(distance, group.Group.MaxAvgDist);
        }
    }

    private sealed class InlineProgress(List<DupeBuildProgress> reports) : IProgress<DupeBuildProgress>
    {
        public void Report(DupeBuildProgress value) => reports.Add(value);
    }
}
