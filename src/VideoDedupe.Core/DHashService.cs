using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;

namespace VideoDedupe.Core;

public sealed class DHashService
{
    public ulong ComputeDHash64(byte[] pngBytes)
    {
        if (pngBytes is null || pngBytes.Length == 0)
            throw new ArgumentException("pngBytes is null/empty", nameof(pngBytes));
        using var img = Image.Load<Rgba32>(pngBytes);
        img.Mutate(x => x.Resize(9, 8).Grayscale());

        ulong hash = 0;
        int bit = 0;

        for (int y = 0; y < 8; y++)
        {
            for (int x = 0; x < 8; x++)
            {
                var left = img[x, y].R;
                var right = img[x + 1, y].R;

                if (left > right)
                    hash |= (1UL << bit);

                bit++;
            }
        }

        return hash;
    }

    public static int HammingDistance(ulong a, ulong b)
    {
        ulong x = a ^ b;
        int count = 0;
        while (x != 0)
        {
            x &= (x - 1);
            count++;
        }
        return count;
    }
}
