using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class IdenticalFileTests
{
    private static byte[] Content(int size, int seed)
    {
        var bytes = new byte[size];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    [Fact]
    public async Task ByteCopiesAreGroupedEvenWithoutReadableVideoData()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);

        // 5 MB: larger than the sampled part, so sampling is used.
        var original = Content(5 << 20, 1);
        var sameSizeOther = Content(5 << 20, 2);
        var paths = new[] { "a.mp4", "copy of a.mp4", "b.mp4", "c.mp4" }
            .Select(n => Path.Combine(workspace.Root, n)).ToArray();
        File.WriteAllBytes(paths[0], original);
        File.WriteAllBytes(paths[1], original);
        File.WriteAllBytes(paths[2], sameSizeOther);
        File.WriteAllBytes(paths[3], Content(1000, 3));
        for (var i = 0; i < paths.Length; i++)
            await db.ExecuteAsync(
                "INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc) VALUES (@Id, @Path, @Size, '', '');",
                new { Id = i + 1, Path = paths[i], Size = new FileInfo(paths[i]).Length });

        var service = new DupeBuildService(db, new FfmpegFrameExtractor(new FfmpegTools()), new DHashService());
        Assert.Equal(1, await service.RunAsync(new DupeBuildOptions(UseAudio: false), null, CancellationToken.None));

        var (_, members) = Assert.Single(await db.ListDuplicateGroupsWithMembersAsync());
        Assert.Equal([1L, 2L], members.Select(m => m.File.Id).Order());
        Assert.All(members, m => Assert.True(m.Identical));
    }

    [Fact]
    public void SamplesBothEndsAndReadsSmallFilesCompletely()
    {
        var small = ContentSampler.Ranges(1000).ToList();
        Assert.Equal([(0L, 1000)], small);

        var size = 10L << 30;
        var ranges = ContentSampler.Ranges(size).ToList();
        Assert.Equal(18, ranges.Count);
        Assert.Equal(0, ranges[0].Offset);
        Assert.Equal(size, ranges[^1].Offset + ranges[^1].Count);
        Assert.All(ranges.Zip(ranges.Skip(1)), p => Assert.True(p.First.Offset + p.First.Count <= p.Second.Offset));
    }
}
