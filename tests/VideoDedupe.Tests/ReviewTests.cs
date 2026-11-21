using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class ReviewTests
{
    private static AppDb.MediaFileRow File(long id, int width = 1920, int height = 1080, long size = 1000,
        string modified = "2024-01-01T00:00:00.0000000Z") =>
        new() { Id = id, Path = $"f{id}.mp4", Width = width, Height = height, SizeBytes = size, ModifiedUtc = modified, DurationSec = 60 };

    [Fact]
    public void BestFilePrefersResolutionThenSizeThenAgeAndExplainsWhy()
    {
        var choice = BestFileSelector.Choose([File(1, 1280, 720, 5000), File(2, 1920, 1080, 1000)]);
        Assert.Equal(2, choice.File.Id);
        Assert.Contains("resolution", choice.Reason);

        choice = BestFileSelector.Choose([File(1, size: 1000), File(2, size: 3000)]);
        Assert.Equal(2, choice.File.Id);
        Assert.Contains("largest", choice.Reason);

        choice = BestFileSelector.Choose([File(1, modified: "2025-01-01T00:00:00Z"), File(2)]);
        Assert.Equal(1, choice.File.Id);
        Assert.Contains("newest", choice.Reason);
    }

    [Fact]
    public void FileMarkedAsKeepWinsOverHigherQuality()
    {
        var decisions = new Dictionary<long, string> { [1] = "keep" };
        var choice = BestFileSelector.Choose([File(1, 640, 360), File(2)], decisions);
        Assert.Equal(1, choice.File.Id);
        Assert.Equal("marked as keep", choice.Reason);
    }

    [Fact]
    public void BitrateComesFromSizeAndDuration() =>
        Assert.Equal(8.0, BestFileSelector.BitrateMbps(File(1, size: 60_000_000))!.Value, 3);

    [Fact]
    public void CleanupKeepsBestPerGroupAndNeverMovesAFileAnotherGroupKeeps()
    {
        // Group 1: file 2 is best. Group 2: file 1 is best there, but file 1 must go in group 1?
        // No: group 2 keeps file 3 (highest resolution), so file 2 would be dropped there,
        // yet group 1 keeps it, so it stays.
        var groups = new[]
        {
            new CleanupPlanner.Group(1, [File(1, 1280, 720), File(2)]),
            new CleanupPlanner.Group(2, [File(2), File(3, 3840, 2160), File(4, 640, 360)]),
            new CleanupPlanner.Group(3, [File(5), File(6, size: 99)]),
        };
        var plan = CleanupPlanner.Create(groups, new Dictionary<long, string>(), path => path != "f6.mp4");

        Assert.Equal([1L, 4L], plan.Items.Select(i => i.File.Id).Order());
        Assert.Equal(2, plan.Items.Single(i => i.File.Id == 1).KeptFile.Id);
        Assert.Equal(3, plan.Items.Single(i => i.File.Id == 4).KeptFile.Id);
        Assert.Equal(2, plan.GroupCount); // group 3 has only one existing file left
        Assert.Equal(2000, plan.Bytes);
    }

    [Fact]
    public void CleanupHonorsKeepDecisions()
    {
        var groups = new[] { new CleanupPlanner.Group(1, [File(1, 640, 360), File(2), File(3)]) };
        var plan = CleanupPlanner.Create(groups, new Dictionary<long, string> { [1] = "keep" }, _ => true);
        Assert.Equal([2L, 3L], plan.Items.Select(i => i.File.Id).Order());
    }

    private static DupeBuildService.Match Match(long id, double avg) => new(File(id), avg, false);

    [Fact]
    public void NotDuplicatePairsAreLeftOutOfAGroup()
    {
        var matches = new[] { Match(1, 0), Match(2, 1), Match(3, 2), Match(4, 3) };
        var pairs = new HashSet<(long, long)> { (1, 2), (3, 4) };

        var kept = DupeBuildService.ApplyNotDuplicate(1, matches, pairs);

        // 2 differs from the seed; 4 differs from 3, which is closer to the seed.
        Assert.Equal([1L, 3L], kept.Select(m => m.File.Id));
    }

    [Fact]
    public async Task BuildSkipsPairsMarkedAsNotDuplicateAndStoresEvidence()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        for (var i = 0; i < 3; i++)
            await db.UpsertMediaFileAsync(new AppDb.MediaFileRow
            {
                Path = workspace.CreateFile($"{i}.mp4"), DurationSec = 10, SizeBytes = 3,
                ModifiedUtc = "2024-01-01T00:00:00.0000000Z"
            });
        var files = (await db.ListMediaFilesForCandidatesAsync()).OrderBy(f => f.Path).ToList();
        foreach (var position in new[] { 20, 50, 80 })
        {
            await db.UpsertFrameHashAsync(files[0].Id, position, 0x0F0F, 0xF0F0);
            await db.UpsertFrameHashAsync(files[1].Id, position, 0x0F0F, 0xF0F0);
            // A mirror image of file 0: its mirror hash equals file 0's hash.
            await db.UpsertFrameHashAsync(files[2].Id, position, 0xF0F0, 0x0F0F);
        }
        var service = new DupeBuildService(db,
            new FfmpegFrameExtractor(new FfmpegTools(ffmpegPath: Path.Combine(workspace.Root, "missing-ffmpeg"))),
            new DHashService());
        var options = new DupeBuildOptions(MaxAvgDist: 2, BorderlineMargin: 0);

        Assert.Equal(1, await service.RunAsync(options, null, CancellationToken.None));
        var members = Assert.Single(await db.ListDuplicateGroupsWithMembersAsync()).Members;
        Assert.Equal(3, members.Count);
        Assert.True(members.Single(m => m.File.Id == files[0].Id).IsReference);
        Assert.True(members.Single(m => m.File.Id == files[2].Id).Mirrored);
        Assert.False(members.Single(m => m.File.Id == files[1].Id).Mirrored);

        await db.MarkNotDuplicateOfAsync(files[1].Id, [files[0].Id, files[2].Id]);
        Assert.Equal(1, await service.RunAsync(options, null, CancellationToken.None));
        members = Assert.Single(await db.ListDuplicateGroupsWithMembersAsync()).Members;
        Assert.Equal([files[0].Id, files[2].Id], members.Select(m => m.File.Id).Order());

        await db.MarkNotDuplicateAsync(files.Select(f => f.Id));
        Assert.Equal(0, await service.RunAsync(options, null, CancellationToken.None));
    }

    [Fact]
    public async Task RemovingMembersAndGroupsUpdatesTheList()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        for (var i = 0; i < 3; i++)
            await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = workspace.CreateFile($"{i}.mp4"), DurationSec = 10 });
        var ids = (await db.ListMediaFilesForCandidatesAsync()).Select(f => f.Id).ToList();
        var groupId = await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10,
            ids.Select(id => new AppDb.DuplicateMemberRow { MediaFileId = id }));

        await db.RemoveGroupMemberAsync(groupId, ids[0]);
        Assert.Equal(2, Assert.Single(await db.ListDuplicateGroupsWithMembersAsync()).Members.Count);

        await db.DeleteGroupAsync(groupId);
        Assert.Empty(await db.ListDuplicateGroupsWithMembersAsync());
    }
}
