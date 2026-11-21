using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class FrameExtractionTests
{
    [Fact]
    public void SplitsStackedFramesInOrder()
    {
        var stack = new GrayFrame(2, 4, [1, 1, 1, 1, 2, 2, 2, 2]);

        var frames = FfmpegFrameExtractor.SplitStack(stack, 2);

        Assert.Equal([1, 1, 1, 1], frames[0]!.Pixels);
        Assert.Equal([2, 2, 2, 2], frames[1]!.Pixels);
        Assert.Throws<InvalidDataException>(() => FfmpegFrameExtractor.SplitStack(stack, 3));
    }

    // Needs a real ffmpeg on PATH; without one the test has nothing to compare and passes.
    [Fact]
    public async Task StackedExtractionReturnsExactlyTheSingleFrames()
    {
        if (!await FfmpegAvailableAsync()) return;
        using var workspace = new TestWorkspace();
        var video = Path.Combine(workspace.Root, "clip with spaces.mp4");
        var tools = new FfmpegTools();
        var (exit, _, err) = await tools.RunAsync(tools.FfmpegPath,
            ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=4",
             "-pix_fmt", "yuv420p", "-c:v", "libx264", "-g", "10", video], CancellationToken.None);
        Assert.True(exit == 0, err);
        var extractor = new FfmpegFrameExtractor(tools);
        double[] timestamps = [0.5, 1.7, 3.2];

        var stacked = await extractor.ExtractGrayFramesAsync(video, timestamps, CancellationToken.None);

        for (var i = 0; i < timestamps.Length; i++)
        {
            var single = await extractor.ExtractGrayFrameAsync(video, timestamps[i], CancellationToken.None);
            Assert.Equal(single.Pixels, stacked[i]!.Pixels);
        }
        // A timestamp ffmpeg cannot deliver only loses that frame.
        var partial = await extractor.ExtractGrayFramesAsync(video, [1.0, 999], CancellationToken.None);
        Assert.NotNull(partial[0]);
        Assert.Null(partial[1]);
    }

    private static async Task<bool> FfmpegAvailableAsync()
    {
        try
        {
            var tools = new FfmpegTools();
            return (await tools.RunAsync(tools.FfmpegPath, ["-version"], CancellationToken.None)).ExitCode == 0;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return false;
        }
    }
}
