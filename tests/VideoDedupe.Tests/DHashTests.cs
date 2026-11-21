using System.Text;
using VideoDedupe.Core;

namespace VideoDedupe.Tests;

public class DHashTests
{
    [Theory]
    [InlineData(false, 0UL)]
    [InlineData(true, ulong.MaxValue)]
    public void OppositeGradientsProduceOppositeHashes(bool descending, ulong expected)
    {
        var frame = Frames.Create(9, 8, (x, _) => (byte)((descending ? 8 - x : x) * 30));
        Assert.Equal(expected, new DHashService().ComputeDHash64(frame));
    }

    [Theory]
    [InlineData(0UL, 0UL, 0)]
    [InlineData(0UL, ulong.MaxValue, 64)]
    [InlineData(0UL, 0x8000000000000000UL, 1)]
    [InlineData(0b1010UL, 0b1100UL, 2)]
    public void CountsDifferentBits(ulong a, ulong b, int expected)
    {
        Assert.Equal(expected, DHashService.HammingDistance(a, b));
        Assert.Equal(expected, DHashService.HammingDistance(b, a));
    }

    [Fact]
    public void ParsesPgmIncludingHeaderComments()
    {
        var header = Encoding.ASCII.GetBytes("P5\n# from ffmpeg\n3 2\n255\n");
        var frame = GrayFrame.ParsePgm([.. header, 1, 2, 3, 4, 5, 6]);

        Assert.Equal(3, frame.Width);
        Assert.Equal(2, frame.Height);
        Assert.Equal(6, frame[2, 1]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("P6\n1 1\n255\n\0\0\0")]
    [InlineData("P5\n4 4\n255\n\0")]
    [InlineData("P5\n1 1\n65535\n\0\0")]
    public void RejectsInvalidPgm(string data)
    {
        Assert.Throws<InvalidDataException>(() => GrayFrame.ParsePgm(Encoding.Latin1.GetBytes(data)));
    }

    [Fact]
    public void AreaDownscaleAveragesCoveredPixels()
    {
        var frame = new GrayFrame(4, 2, [0, 100, 200, 40, 0, 100, 200, 40]);
        Assert.Equal(new[] { 50.0, 120.0 }, frame.Downscale((0, 0, 4, 2), 2, 1));
    }
}

internal static class Frames
{
    public static GrayFrame Create(int width, int height, Func<int, int, byte> pixel)
    {
        var pixels = new byte[width * height];
        for (var y = 0; y < height; y++)
            for (var x = 0; x < width; x++)
                pixels[y * width + x] = pixel(x, y);
        return new GrayFrame(width, height, pixels);
    }
}
