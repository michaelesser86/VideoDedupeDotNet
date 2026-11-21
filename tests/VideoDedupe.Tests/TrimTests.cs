using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class TrimTests
{
    private static readonly Random Rng = new(42);

    // About half the bits set, like a real picture.
    private static ulong Picture() => (ulong)Rng.NextInt64() ^ ((ulong)Rng.Next() << 33);

    private static FrameHash Hash(ulong h) => new(h, ~h);

    private static (AppDb.MediaFileRow, Dictionary<int, FrameHash>) File(long id, double duration, ulong start, ulong end) =>
        (new AppDb.MediaFileRow { Id = id, Path = $"{id}.mp4", DurationSec = duration },
         new Dictionary<int, FrameHash>
         {
             [TrimMatcher.StartAnchorKey] = Hash(start),
             [TrimMatcher.EndAnchorKey] = Hash(end)
         });

    [Fact]
    public void FindsCopiesSharingStartOrEnd()
    {
        ulong shared = Picture(), shared2 = Picture();
        var files = new[]
        {
            File(1, 600, shared, Picture()),   // original
            File(2, 540, shared, Picture()),   // credits cut: same start
            File(3, 900, Picture(), shared2),  // original
            File(4, 800, Picture(), shared2),  // intro cut: same end
            File(5, 600, shared, Picture()),   // same length as 1: the regular search covers it
            File(6, 300, shared, Picture()),   // only half as long: an excerpt, not a copy
            File(7, 700, 0, 0),                // black anchors match every black frame
            File(8, 650, 0, 0),
        };

        var candidates = TrimMatcher.FindCandidates(files, tolSec: 0.25, maxDist: 10, matchMirrored: true);

        Assert.Equal(3, candidates.Count);
        Assert.Contains(candidates, c => c.Longer.Id == 1 && c.Shorter.Id == 2 && c.Kind == AppDb.TrimKinds.SameStart);
        Assert.Contains(candidates, c => c.Longer.Id == 5 && c.Shorter.Id == 2 && c.Kind == AppDb.TrimKinds.SameStart);
        Assert.Contains(candidates, c => c.Longer.Id == 3 && c.Shorter.Id == 4 && c.Kind == AppDb.TrimKinds.SameEnd);
    }

    [Fact]
    public void SharedOpeningOfManyFilesIsIgnored()
    {
        var opening = Picture();
        var episodes = Enumerable.Range(1, TrimMatcher.MaxAnchorMatches + 2)
            .Select(i => File(i, 1200 + i * 30, opening, Picture()))
            .ToList();

        Assert.Empty(TrimMatcher.FindCandidates(episodes, 0.25, 10, matchMirrored: true));
    }

    [Fact]
    public void FindsMirroredShortenedCopy()
    {
        var start = Picture();
        var files = new[] { File(1, 600, start, Picture()), File(2, 540, ~start, Picture()) };

        var candidate = Assert.Single(TrimMatcher.FindCandidates(files, 0.25, 10, matchMirrored: true));
        Assert.True(candidate.Mirrored);
        Assert.Empty(TrimMatcher.FindCandidates(files, 0.25, 10, matchMirrored: false));
    }

    [Theory]
    [InlineData(true, 1)]   // the shorter file's frames reappear in the longer file: a copy
    [InlineData(false, 0)]  // only the opening matches: a different video
    public async Task BuildGroupsVerifiedShortenedCopiesOnly(bool sameFootage, int expectedGroups)
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = workspace.CreateFile("long.mp4"), DurationSec = 600 });
        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow { Path = workspace.CreateFile("short.mp4"), DurationSec = 540 });
        var files = (await db.ListMediaFilesForCandidatesAsync()).ToDictionary(f => Path.GetFileName(f.Path));
        var (longer, shorter) = (files["long.mp4"], files["short.mp4"]);

        var opening = Picture();
        foreach (var file in new[] { longer, shorter })
        {
            await db.UpsertFrameHashAsync(file.Id, TrimMatcher.StartAnchorKey, opening, ~opening);
            await db.UpsertFrameHashAsync(file.Id, TrimMatcher.EndAnchorKey, Picture(), Picture());
            foreach (var p in new[] { 20, 50, 80 })
                await db.UpsertFrameHashAsync(file.Id, p, Picture(), Picture());
        }
        // The longer file's frames at the moments the shorter file was hashed (cut at the end,
        // so the times are the same): equal for a copy, unrelated for a different video.
        var shorterHashes = (await db.GetAllFrameHashesAsync())[shorter.Id];
        await db.UpsertTimedFrameHashesAsync(longer.Id, new[] { 20, 50, 80 }.ToDictionary(
            p => (long)Math.Round(540 * p / 100.0 * 1000),
            p => sameFootage ? shorterHashes[p] : new FrameHash(Picture(), Picture())));

        var service = new DupeBuildService(db,
            new FfmpegFrameExtractor(new FfmpegTools(ffmpegPath: Path.Combine(workspace.Root, "missing-ffmpeg"))),
            new DHashService());

        Assert.Equal(0, await service.RunAsync(new DupeBuildOptions(), null, CancellationToken.None));
        Assert.Equal(expectedGroups,
            await service.RunAsync(new DupeBuildOptions(FindTrimmed: true), null, CancellationToken.None));

        if (expectedGroups == 1)
        {
            var members = Assert.Single(await db.ListDuplicateGroupsWithMembersAsync()).Members;
            var copy = members.Single(m => m.File.Id == shorter.Id);
            Assert.Equal(AppDb.TrimKinds.SameStart, copy.TrimKind);
            Assert.Equal(-60, copy.TrimDiffSec);
            Assert.True(members.Single(m => m.File.Id == longer.Id).IsReference);

            // Marked as different: the pair is not grouped again.
            await db.MarkNotDuplicateAsync([longer.Id, shorter.Id]);
            Assert.Equal(0, await service.RunAsync(new DupeBuildOptions(FindTrimmed: true), null, CancellationToken.None));
        }
    }

    [Fact]
    public async Task MissingToolsAreReported()
    {
        var missing = await new FfmpegTools(ffprobePath: "no-such-ffprobe-tool", ffmpegPath: "no-such-ffmpeg-tool")
            .FindMissingToolsAsync();
        Assert.Equal(["no-such-ffmpeg-tool", "no-such-ffprobe-tool"], missing);
    }

    [Fact]
    public void AnchorTimesAreFixedFromStartAndEnd()
    {
        Assert.Equal(60, DupeBuildService.TimeOf(TrimMatcher.StartAnchorKey, 600));
        Assert.Equal(540, DupeBuildService.TimeOf(TrimMatcher.EndAnchorKey, 600));
        Assert.Equal(300, DupeBuildService.TimeOf(50, 600));
    }
}
