using System.Numerics;

namespace VideoDedupe.Core;

/// <summary>A frame's difference hash and the hash of its mirror image.</summary>
public readonly record struct FrameHash(ulong Normal, ulong Mirrored);

/// <summary>64-bit difference hash: compares horizontally adjacent cells of a 9×8 downscale.</summary>
public sealed class DHashService
{
    /// <summary>Bump whenever hash values change, so cached hashes are recomputed.</summary>
    public const string Version = "gray-area-v1";

    public ulong ComputeDHash64(GrayFrame frame) => ComputeHashes(frame).Normal;

    /// <summary>
    /// Hash of the frame and of its horizontal mirror image, from the same downscale. The mirror
    /// hash costs a few comparisons instead of a second frame extraction.
    /// </summary>
    public FrameHash ComputeHashes(GrayFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var cells = frame.Downscale(frame.ContentBounds(), 9, 8);

        // Compare whole gray levels: in flat areas neighbouring cells differ only by encoding
        // noise, and exact averages would turn that noise into random bits. Rounded, such
        // cells compare equal and yield a stable 0 bit.
        for (var i = 0; i < cells.Length; i++)
            cells[i] = Math.Round(cells[i]);

        ulong normal = 0, mirrored = 0;
        var bit = 0;
        for (var y = 0; y < 8; y++)
        {
            var row = y * 9;
            for (var x = 0; x < 8; x++, bit++)
            {
                if (cells[row + x] > cells[row + x + 1])
                    normal |= 1UL << bit;
                // Mirrored, cell x of the row is cell 8 - x of the original.
                if (cells[row + 8 - x] > cells[row + 7 - x])
                    mirrored |= 1UL << bit;
            }
        }
        return new FrameHash(normal, mirrored);
    }

    public static int HammingDistance(ulong a, ulong b) => BitOperations.PopCount(a ^ b);

    /// <summary>
    /// Nearly flat frames (black, fades, plain title cards) hash to almost no set bits (or almost
    /// all) and look alike in every video, so two of them say nothing about a match.
    /// </summary>
    public static bool IsInformative(ulong hash) => BitOperations.PopCount(hash) is >= 8 and <= 56;
}
