using System.Numerics;

namespace VideoDedupe.Core;

/// <summary>64-bit difference hash: compares horizontally adjacent cells of a 9×8 downscale.</summary>
public sealed class DHashService
{
    /// <summary>Bump whenever hash values change, so cached hashes are recomputed.</summary>
    public const string Version = "gray-area-v1";

    public ulong ComputeDHash64(GrayFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var cells = frame.Downscale(frame.ContentBounds(), 9, 8);

        // Compare whole gray levels: in flat areas neighbouring cells differ only by encoding
        // noise, and exact averages would turn that noise into random bits. Rounded, such
        // cells compare equal and yield a stable 0 bit.
        for (var i = 0; i < cells.Length; i++)
            cells[i] = Math.Round(cells[i]);

        ulong hash = 0;
        var bit = 0;
        for (var y = 0; y < 8; y++)
            for (var x = 0; x < 8; x++, bit++)
                if (cells[y * 9 + x] > cells[y * 9 + x + 1])
                    hash |= 1UL << bit;
        return hash;
    }

    public static int HammingDistance(ulong a, ulong b) => BitOperations.PopCount(a ^ b);
}
