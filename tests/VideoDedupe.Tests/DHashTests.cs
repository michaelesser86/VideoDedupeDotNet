using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using VideoDedupe.Core;

namespace VideoDedupe.Tests;

public class DHashTests
{
    [Theory]
    [InlineData(false, 0UL)]
    [InlineData(true, ulong.MaxValue)]
    public void OppositeGradientsProduceOppositeHashes(bool descending, ulong expected)
    {
        using var image = new Image<Rgba32>(9, 8);
        for (var y = 0; y < 8; y++)
            for (var x = 0; x < 9; x++)
            {
                var value = (byte)((descending ? 8 - x : x) * 30);
                image[x, y] = new Rgba32(value, value, value);
            }
        using var png = new MemoryStream();
        image.SaveAsPng(png);
        Assert.Equal(expected, new DHashService().ComputeDHash64(png.ToArray()));
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
    public void RejectsEmptyImage()
    {
        Assert.Throws<ArgumentException>(() => new DHashService().ComputeDHash64(Array.Empty<byte>()));
    }
}
