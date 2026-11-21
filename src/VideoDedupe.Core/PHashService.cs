using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;

namespace VideoDedupe.Core;

/// <summary>A small, dependency-free perceptual hash used to refine dHash matches.</summary>
public sealed class PHashService
{
    public ulong ComputePHash64(byte[] pngBytes)
    {
        if (pngBytes is null || pngBytes.Length == 0)
            throw new ArgumentException("pngBytes is null/empty", nameof(pngBytes));

        using var image = Image.Load<Rgba32>(pngBytes);
        DHashService.CropBlackBars(image);
        image.Mutate(x => x.Resize(32, 32).Grayscale());

        var pixels = new double[32, 32];
        for (var y = 0; y < 32; y++)
            for (var x = 0; x < 32; x++)
                pixels[x, y] = image[x, y].R;

        var low = new double[8, 8];
        for (var v = 0; v < 8; v++)
            for (var u = 0; u < 8; u++)
            {
                double sum = 0;
                for (var y = 0; y < 32; y++)
                    for (var x = 0; x < 32; x++)
                        sum += pixels[x, y] * Math.Cos((2 * x + 1) * u * Math.PI / 64)
                            * Math.Cos((2 * y + 1) * v * Math.PI / 64);
                low[u, v] = sum;
            }

        var median = low.Cast<double>().Skip(1).Order().ElementAt(31);
        ulong hash = 0;
        var bit = 0;
        for (var v = 0; v < 8; v++)
            for (var u = 0; u < 8; u++, bit++)
                if (low[u, v] > median)
                    hash |= 1UL << bit;
        return hash;
    }

    /// <summary>
    /// Average distance from each scene hash of <paramref name="seed"/> to its closest hash in
    /// <paramref name="candidate"/>. Re-encodes often detect a different number of cuts, so slot
    /// numbers do not line up; matching by best fit keeps shifted scenes comparable.
    /// </summary>
    public static double? SceneSetDistance(IReadOnlyCollection<ulong> seed, IReadOnlyCollection<ulong> candidate)
    {
        if (seed.Count == 0 || candidate.Count == 0)
            return null;
        return seed.Average(a => candidate.Min(b => DHashService.HammingDistance(a, b)));
    }
}
