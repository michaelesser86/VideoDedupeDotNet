using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
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

    private static byte[] CreateImage(int bars)
    {
        using var image = new Image<Rgba32>(80, 60 + bars * 2);
        for (var y = bars; y < bars + 60; y++)
            for (var x = 0; x < image.Width; x++)
            {
                var value = (byte)((x * 3 + (y - bars) * 5) % 256);
                image[x, y] = new Rgba32(value, (byte)(255 - value), value);
            }
        using var stream = new MemoryStream();
        image.SaveAsPng(stream);
        return stream.ToArray();
    }

    private static byte[] CreateSolidImage(byte value)
    {
        using var image = new Image<Rgba32>(32, 32, new Rgba32(value, value, value));
        using var stream = new MemoryStream();
        image.SaveAsPng(stream);
        return stream.ToArray();
    }

    private static byte[] CreateStripedImage()
    {
        using var image = new Image<Rgba32>(32, 32);
        for (var y = 0; y < 32; y++)
            for (var x = 0; x < 32; x++)
            {
                var value = (byte)(x % 4 < 2 ? 20 : 230);
                image[x, y] = new Rgba32(value, value, value);
            }
        using var stream = new MemoryStream();
        image.SaveAsPng(stream);
        return stream.ToArray();
    }
}
