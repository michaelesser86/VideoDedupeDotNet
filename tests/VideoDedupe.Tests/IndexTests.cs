using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class IndexTests
{
    private sealed class ProgressRecorder : IProgress<IndexProgress>
    {
        public IndexProgress? Last { get; private set; }
        public int Errors { get; private set; }
        public void Report(IndexProgress value)
        {
            lock (this)
            {
                Last = value;
                if (value.Error is not null) Errors++;
            }
        }
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    public async Task HonorsRootOptionsAndRetainsFilesWhenProbeFails(bool recursive, int expected)
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        var top = workspace.CreateFile("enabled/top.mp4");
        var nested = workspace.CreateFile("enabled/sub/nested.mp4");
        workspace.CreateFile("enabled/SKIP.mp4");
        workspace.CreateFile("enabled/ignore.txt");
        workspace.CreateFile("disabled/hidden.mp4");
        var root = await db.AddScanRootAsync(Path.Combine(workspace.Root, "enabled"));
        await db.UpdateScanRootOptionsAsync(root, recursive, "*skip*;unused");
        var disabled = await db.AddScanRootAsync(Path.Combine(workspace.Root, "disabled"));
        await db.SetScanRootEnabledAsync(disabled, false);
        var probePath = Path.Combine(workspace.Root, "missing-ffprobe");
        var probe = new FfprobeService(new FfmpegTools(ffprobePath: probePath));
        var error = await Assert.ThrowsAsync<FileNotFoundException>(() => probe.ProbeAsync(top, CancellationToken.None));
        Assert.Contains(probePath, error.Message);
        var service = new IndexService(db, probe);
        var progress = new ProgressRecorder();

        // Reindexing must update the same rows, not duplicate them.
        for (var pass = 0; pass < 2; pass++)
            await service.RunAsync(progress, CancellationToken.None);

        var files = await db.ListMediaFilesAsync();
        Assert.Equal(expected, files.Count);
        Assert.Contains(files, f => f.Path == top);
        Assert.Equal(recursive, files.Any(f => f.Path == nested));
        Assert.All(files, f => Assert.Equal(3, f.SizeBytes));
        Assert.Empty(await db.ListMediaFilesForCandidatesAsync());
        Assert.NotNull(progress.Last);
        Assert.Equal(expected, progress.Last.Processed);
        Assert.Equal(expected, progress.Last.ProbedFail);
        Assert.Equal(0, progress.Last.ProbedOk);
        Assert.Equal(expected * 2, progress.Errors);
    }

    [Fact]
    public async Task RejectsMissingEnabledRoots()
    {
        using var workspace = new TestWorkspace();
        var service = new IndexService(workspace.Db, new FfprobeService(new FfmpegTools()));
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.RunAsync(null, CancellationToken.None));
    }
}
