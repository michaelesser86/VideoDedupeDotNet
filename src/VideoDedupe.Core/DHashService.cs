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
        CropBlackBars(img);
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

    // Narrow black borders are common after letterboxing.  They otherwise dominate
    // a small perceptual hash and make the same picture look needlessly different.
    internal static void CropBlackBars(Image<Rgba32> image)
    {
        const byte blackThreshold = 18;
        const double mostlyBlack = .985;
        var left = 0;
        var right = image.Width - 1;
        var top = 0;
        var bottom = image.Height - 1;

        bool RowIsBlack(int y) => Enumerable.Range(0, image.Width)
            .Count(x => Luminance(image[x, y]) <= blackThreshold) >= image.Width * mostlyBlack;
        bool ColumnIsBlack(int x) => Enumerable.Range(0, image.Height)
            .Count(y => Luminance(image[x, y]) <= blackThreshold) >= image.Height * mostlyBlack;

        while (top < bottom && RowIsBlack(top)) top++;
        while (bottom > top && RowIsBlack(bottom)) bottom--;
        while (left < right && ColumnIsBlack(left)) left++;
        while (right > left && ColumnIsBlack(right)) right--;

        // Keep at least a meaningful image. This also avoids treating an almost
        // completely black video frame as a crop candidate.
        if (right - left + 1 >= 8 && bottom - top + 1 >= 8 &&
            (left != 0 || top != 0 || right != image.Width - 1 || bottom != image.Height - 1))
            image.Mutate(x => x.Crop(new Rectangle(left, top, right - left + 1, bottom - top + 1)));
    }

    private static byte Luminance(Rgba32 pixel) =>
        (byte)((pixel.R * 299 + pixel.G * 587 + pixel.B * 114) / 1000);
}
