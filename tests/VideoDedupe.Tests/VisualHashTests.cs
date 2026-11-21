using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class VisualHashTests
{
    [Fact]
    public void LetterboxCropKeepsTheSamePictureClose()
    {
        var plain = CreateImage(0);
        var letterboxed = CreateImage(20);

        var hasher = new DHashService();
        var distance = DHashService.HammingDistance(hasher.ComputeDHash64(plain), hasher.ComputeDHash64(letterboxed));

        Assert.InRange(distance, 0, 4);
    }

    [Fact]
    public void PHashSeparatesDifferentStructures()
    {
        var dark = CreateSolidImage(20);
        var stripes = CreateStripedImage();
        var hasher = new PHashService();

        Assert.NotEqual(hasher.ComputePHash64(dark), hasher.ComputePHash64(stripes));
    }

    [Fact]
    public void SceneHashDistanceToleratesShiftedSceneSlots()
    {
        // The second encode found one extra cut, so every scene moved by one slot.
        ulong[] original = [0x0FUL, 0xF0UL, 0xFF00UL];
        ulong[] reencoded = [0xAAAAUL, 0x0FUL, 0xF0UL, 0xFF00UL];

        Assert.Equal(0, PHashService.SceneSetDistance(original, reencoded));
        Assert.True(PHashService.SceneSetDistance([0UL], [ulong.MaxValue]) == 64);
        Assert.Null(PHashService.SceneSetDistance([], reencoded));
    }

    [Fact]
    public void SceneParserSelectsEvenlyDistributedSceneFrames()
    {
        var times = SceneFrameSelector.ParseTimestamps("pts_time:1.0 pts_time:2.0 pts_time:5.0 pts_time:9.0", 10);
        var selected = SceneFrameSelector.SelectRepresentative(times, 10, 3);

        Assert.Equal(new[] { 1.0, 5.0, 9.0 }, selected);
    }

    [Fact]
    public void SceneSelectorFallsBackToAdaptiveFrames()
    {
        var selected = SceneFrameSelector.SelectRepresentative([], 10, 3);

        Assert.Equal(new[] { 2.5, 5.0, 7.5 }, selected);
    }

    private static GrayFrame CreateImage(int bars) =>
        Frames.Create(80, 60 + bars * 2, (x, y) =>
            y < bars || y >= bars + 60 ? (byte)0 : (byte)((x * 3 + (y - bars) * 5) % 256));

    private static GrayFrame CreateSolidImage(byte value) => Frames.Create(32, 32, (_, _) => value);

    private static GrayFrame CreateStripedImage() => Frames.Create(32, 32, (x, _) => (byte)(x % 4 < 2 ? 20 : 230));
}
