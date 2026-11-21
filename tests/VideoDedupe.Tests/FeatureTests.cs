using VideoDedupe.App.Services;
using VideoDedupe.App.ViewModels;
using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class FeatureTests
{
    private static readonly Random Rng = new(7);
    private static ulong Picture() => (ulong)Rng.NextInt64() ^ ((ulong)Rng.Next() << 33);

    private static AppDb.MediaFileRow File(long id, string path, string codec = "h264", long size = 1000, double? duration = null) =>
        new()
        {
            Id = id, Path = path, Width = 1920, Height = 1080, SizeBytes = size, VideoCodec = codec,
            DurationSec = duration, ModifiedUtc = "2024-01-01T00:00:00Z"
        };

    [Fact]
    public void RulesDecideBetweenCopiesOfTheSameResolution()
    {
        AppDb.MediaFileRow[] files = [File(1, "a.mkv", "h264", 3000), File(2, "b.mp4", "hevc", 1000)];

        // Default: the larger file has more detail.
        Assert.Equal(1, BestFileSelector.Choose(files).File.Id);

        var modern = BestFileSelector.Choose(files, rules: new BestFileRules(Codec: BestFileRules.ModernCodec));
        Assert.Equal((2L, BestFileSelector.ReasonKind.PreferredCodec), (modern.File.Id, modern.Kind));

        var mp4 = BestFileSelector.Choose(files, rules: new BestFileRules(Container: BestFileRules.Mp4));
        Assert.Equal((2L, BestFileSelector.ReasonKind.PreferredContainer), (mp4.File.Id, mp4.Kind));

        var smaller = BestFileSelector.Choose(files, rules: new BestFileRules(PreferSmaller: true));
        Assert.Equal((2L, BestFileSelector.ReasonKind.SmallestFile), (smaller.File.Id, smaller.Kind));

        // Resolution still comes first.
        var sd = File(3, "c.mp4", "hevc", 10);
        sd.Width = 640;
        sd.Height = 360;
        Assert.Equal(1, BestFileSelector.Choose([files[0], sd], rules: new BestFileRules(BestFileRules.ModernCodec, BestFileRules.Mp4, true)).File.Id);
    }

    [Fact]
    public async Task HistoryRecordsEveryUserAction()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        await db.ExecuteAsync("INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc) VALUES (1, 'a.mp4', 10, '', ''), (2, 'b.mp4', 20, '', '');");

        await db.MarkMediaFileDeletedByUserAsync(1);
        await db.MarkMediaFileQuarantinedAsync(2, "q\\b.mp4");
        await db.MarkQuarantineRestoredAsync(2);
        await db.MarkMediaFileQuarantinedAsync(2, "q\\b.mp4");
        await db.MarkQuarantineDiscardedAsync(2, "q\\b.mp4");
        // Files that vanished on their own (index scan) are not the user's doing.
        await db.MarkMediaFileDeletedAsync(2);

        var history = await db.ListHistoryAsync();
        Assert.Equal(
            ["discarded", "quarantined", "restored", "quarantined", "deleted"],
            history.Select(h => h.Action));
        Assert.Equal(("a.mp4", 10L), (history[^1].Path, history[^1].SizeBytes));
        Assert.Equal("q\\b.mp4", history[0].Target);

        var vm = new HistoryViewModel(history);
        Assert.Equal(5, vm.Items.Count);
        Assert.False(vm.IsEmpty);
    }

    [Fact]
    public void ExcerptIsFoundAtItsOffsetAndUnrelatedClipsAreNot()
    {
        // A 10-minute source with a keyframe every 8 s, and a 60 s excerpt from 123.4 s sampled
        // 5 times per second: wherever a source keyframe falls into the excerpt, the excerpt has
        // a sample within 0.1 s showing (nearly) the same picture.
        var source = File(1, "long.mp4", duration: 600);
        var keyframes = Enumerable.Range(0, 75).Select(i => ((long)(i * 8000), Picture())).ToList();
        const double offset = 123.4;
        var excerpt = File(2, "clip.mp4", duration: 60);
        var samples = new List<(long, ulong)>();
        for (var t = 0; t < 300; t++)
        {
            var time = t * 200L;
            var match = keyframes.FirstOrDefault(k => Math.Abs(k.Item1 - (time + offset * 1000)) <= 100);
            samples.Add((time, match.Item1 > 0 ? match.Item2 ^ 0b101 : Picture()));
        }
        var unrelated = File(3, "other.mp4", duration: 60);
        var unrelatedSamples = Enumerable.Range(0, 300).Select(t => (t * 200L, Picture())).ToList();

        var candidate = Assert.Single(ExcerptMatcher.FindCandidates(
            [(source, keyframes)],
            [(excerpt, samples), (unrelated, unrelatedSamples)]));
        Assert.Equal((1L, 2L), (candidate.Source.Id, candidate.Excerpt.Id));
        Assert.InRange(candidate.OffsetSec, offset - 0.2, offset + 0.2);
        Assert.True(candidate.Votes >= ExcerptMatcher.MinVotes);

        // Copies of similar length are not excerpts.
        Assert.Empty(ExcerptMatcher.FindCandidates([(File(4, "x.mp4", duration: 70), keyframes)], [(excerpt, samples)]));
    }

    [Fact]
    public void MostlyBlackMatchesNeedConfirmation()
    {
        var content = Picture();
        Dictionary<int, FrameHash> a = new() { [20] = new(0, 0), [50] = new(content, ~content), [80] = new(0, 0) };
        Dictionary<int, FrameHash> b = new() { [20] = new(0, 0), [50] = new(content ^ 0xFFFF, ~content), [80] = new(0, 0) };
        Assert.True(DupeBuildService.MostlyFlat(a, b));
        Assert.True(DupeBuildService.Compare(a, b, File(2, "b.mp4"), matchMirrored: false).NeedsConfirmation);

        Dictionary<int, FrameHash> c = new() { [20] = new(content, 0), [50] = new(content, 0), [80] = new(0, 0) };
        Assert.False(DupeBuildService.MostlyFlat(c, c));
    }

    [Fact]
    public void UpdateCheckComparesReleaseVersions()
    {
        Assert.True(UpdateChecker.IsNewer("v1.1.0", "1.0.0"));
        Assert.True(UpdateChecker.IsNewer("v1.0.10", "1.0.9"));
        Assert.False(UpdateChecker.IsNewer("v1.0.0", "1.0.0"));
        Assert.False(UpdateChecker.IsNewer("v0.9.0", "1.0.0"));
        Assert.False(UpdateChecker.IsNewer("nightly", "1.0.0"));
        Assert.True(UpdateChecker.IsReleaseBuild("1.2.0"));
        Assert.False(UpdateChecker.IsReleaseBuild("0.0.0-dev.12"));
    }

    [Fact]
    public void ComparisonShowsTheSameMomentInBothFiles()
    {
        // Same length: proportional.
        Assert.Equal(50, CompareViewModel.RightTime(50, 100, 100.5, alignEnd: false), 0);
        // Cut at the end: same start.
        Assert.Equal(30, CompareViewModel.RightTime(30, 300, 240, alignEnd: false));
        // Cut at the start: same end.
        Assert.Equal(30, CompareViewModel.RightTime(90, 300, 240, alignEnd: true));
        // Never outside the file.
        Assert.Equal(0.1, CompareViewModel.RightTime(10, 300, 240, alignEnd: true));
    }

    [Fact]
    public void KeyframeStreamSplitsIntoTimedFrames()
    {
        const int size = FfmpegFrameExtractor.KeyframeWidth * FfmpegFrameExtractor.KeyframeHeight;
        var raw = new byte[size * 2 + 10];
        raw[size] = 255;
        const string log = "[Parsed_showinfo_3] n:0 pts:0 pts_time:0 ...\n[Parsed_showinfo_3] n:1 pts:900 pts_time:8.341667 ...";
        var frames = FfmpegFrameExtractor.SplitKeyframes(raw, log);
        Assert.Equal([0L, 8342L], frames.Select(f => f.TimeMs));
        Assert.Equal(255, frames[1].Frame.Pixels[0]);
    }
}
