namespace VideoDedupe.Core;

/// <summary>A small, dependency-free perceptual hash (8×8 low frequencies of a 32×32 DCT).</summary>
public sealed class PHashService
{
    private const int Size = 32;
    private const int Low = 8;

    // Cos[u, x] of the DCT-II; separable, so the 2-D transform is two 1-D passes.
    private static readonly double[,] Cos = CreateCosTable();

    public ulong ComputePHash64(GrayFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var pixels = frame.Downscale(frame.ContentBounds(), Size, Size);

        // rows[u, y] = Σx pixels[x, y] · cos(u, x)
        var rows = new double[Low, Size];
        for (var u = 0; u < Low; u++)
            for (var y = 0; y < Size; y++)
            {
                double sum = 0;
                for (var x = 0; x < Size; x++)
                    sum += pixels[y * Size + x] * Cos[u, x];
                rows[u, y] = sum;
            }

        // low[u, v] = Σy rows[u, y] · cos(v, y)
        var low = new double[Low * Low];
        for (var v = 0; v < Low; v++)
            for (var u = 0; u < Low; u++)
            {
                double sum = 0;
                for (var y = 0; y < Size; y++)
                    sum += rows[u, y] * Cos[v, y];
                low[v * Low + u] = sum;
            }

        // Median of the 63 AC coefficients; the DC term only carries overall brightness.
        var median = low.Skip(1).Order().ElementAt(31);
        ulong hash = 0;
        for (var bit = 0; bit < Low * Low; bit++)
            if (low[bit] > median)
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

    private static double[,] CreateCosTable()
    {
        var table = new double[Low, Size];
        for (var u = 0; u < Low; u++)
            for (var x = 0; x < Size; x++)
                table[u, x] = Math.Cos((2 * x + 1) * u * Math.PI / (2 * Size));
        return table;
    }
}
