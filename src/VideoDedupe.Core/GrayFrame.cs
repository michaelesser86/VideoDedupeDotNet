using System.Text;

namespace VideoDedupe.Core;

/// <summary>An 8-bit grayscale frame, as produced by <c>ffmpeg -pix_fmt gray -vcodec pgm</c>.</summary>
public sealed class GrayFrame
{
    public int Width { get; }
    public int Height { get; }
    /// <summary>Row-major luminance, <c>Width * Height</c> bytes.</summary>
    public byte[] Pixels { get; }

    public GrayFrame(int width, int height, byte[] pixels)
    {
        if (width <= 0 || height <= 0)
            throw new ArgumentOutOfRangeException(nameof(width), "Frame must not be empty.");
        ArgumentNullException.ThrowIfNull(pixels);
        if (pixels.Length != (long)width * height)
            throw new ArgumentException("Pixel count does not match the frame size.", nameof(pixels));
        Width = width;
        Height = height;
        Pixels = pixels;
    }

    public byte this[int x, int y] => Pixels[y * Width + x];

    /// <summary>The frame flipped left to right (a mirrored re-upload).</summary>
    public GrayFrame MirrorHorizontal()
    {
        var mirrored = new byte[Pixels.Length];
        for (var y = 0; y < Height; y++)
        {
            var row = y * Width;
            for (var x = 0; x < Width; x++)
                mirrored[row + x] = Pixels[row + Width - 1 - x];
        }
        return new GrayFrame(Width, Height, mirrored);
    }

    /// <summary>Parses a binary PGM (P5) image with a maximum value of 255.</summary>
    /// <exception cref="InvalidDataException">The bytes are not such an image.</exception>
    public static GrayFrame ParsePgm(ReadOnlySpan<byte> data)
    {
        var pos = 0;
        if (ReadToken(data, ref pos) != "P5")
            throw new InvalidDataException("Not a binary PGM (P5) image.");
        var width = ReadNumber(data, ref pos);
        var height = ReadNumber(data, ref pos);
        var maxValue = ReadNumber(data, ref pos);
        if (maxValue != 255)
            throw new InvalidDataException($"Unsupported PGM maximum value {maxValue}.");
        pos++; // exactly one whitespace byte separates the header from the pixels

        var length = (long)width * height;
        if (width <= 0 || height <= 0 || data.Length - pos < length)
            throw new InvalidDataException("PGM pixel data is truncated.");
        return new GrayFrame(width, height, data.Slice(pos, (int)length).ToArray());
    }

    private static int ReadNumber(ReadOnlySpan<byte> data, ref int pos) =>
        int.TryParse(ReadToken(data, ref pos), out var value) && value > 0
            ? value
            : throw new InvalidDataException("Malformed PGM header.");

    private static string ReadToken(ReadOnlySpan<byte> data, ref int pos)
    {
        while (pos < data.Length)
        {
            if (data[pos] == (byte)'#')
                while (pos < data.Length && data[pos] != (byte)'\n') pos++;
            else if (IsWhitespace(data[pos])) pos++;
            else break;
        }
        var start = pos;
        while (pos < data.Length && !IsWhitespace(data[pos]) && pos - start < 16) pos++;
        if (pos == start)
            throw new InvalidDataException("Malformed PGM header.");
        return Encoding.ASCII.GetString(data[start..pos]);
    }

    private static bool IsWhitespace(byte b) => b is (byte)' ' or (byte)'\t' or (byte)'\n' or (byte)'\r';

    /// <summary>
    /// Bounds of the picture without letterbox/pillarbox bars. Narrow black borders otherwise
    /// dominate a small perceptual hash and make the same picture look needlessly different.
    /// </summary>
    public (int Left, int Top, int Width, int Height) ContentBounds()
    {
        const byte blackThreshold = 18;
        const double mostlyBlack = .985;

        bool RowIsBlack(int y)
        {
            var row = Pixels.AsSpan(y * Width, Width);
            var black = 0;
            foreach (var p in row) if (p <= blackThreshold) black++;
            return black >= Width * mostlyBlack;
        }

        bool ColumnIsBlack(int x, int top, int bottom)
        {
            var black = 0;
            for (var y = top; y <= bottom; y++) if (Pixels[y * Width + x] <= blackThreshold) black++;
            return black >= (bottom - top + 1) * mostlyBlack;
        }

        int t = 0, b = Height - 1;
        while (t < b && RowIsBlack(t)) t++;
        while (b > t && RowIsBlack(b)) b--;
        int l = 0, r = Width - 1;
        while (l < r && ColumnIsBlack(l, t, b)) l++;
        while (r > l && ColumnIsBlack(r, t, b)) r--;

        // Keep at least a meaningful image. This also avoids treating an almost
        // completely black frame as a crop candidate.
        return r - l + 1 >= 8 && b - t + 1 >= 8 ? (l, t, r - l + 1, b - t + 1) : (0, 0, Width, Height);
    }

    /// <summary>
    /// Area-averaged downscale of a region to <paramref name="targetWidth"/> × <paramref name="targetHeight"/>.
    /// Every source pixel contributes with its covered fraction, so small details cannot alias.
    /// </summary>
    public double[] Downscale((int Left, int Top, int Width, int Height) region, int targetWidth, int targetHeight)
    {
        var (left, top, width, height) = region;
        var result = new double[targetWidth * targetHeight];
        var sx = (double)width / targetWidth;
        var sy = (double)height / targetHeight;

        for (var ty = 0; ty < targetHeight; ty++)
        {
            var y0 = ty * sy;
            var y1 = y0 + sy;
            for (var tx = 0; tx < targetWidth; tx++)
            {
                var x0 = tx * sx;
                var x1 = x0 + sx;
                double sum = 0, area = 0;
                for (var y = (int)y0; y < Math.Min(height, (int)Math.Ceiling(y1)); y++)
                {
                    var wy = Math.Min(y + 1, y1) - Math.Max(y, y0);
                    var rowOffset = (top + y) * Width + left;
                    for (var x = (int)x0; x < Math.Min(width, (int)Math.Ceiling(x1)); x++)
                    {
                        var w = wy * (Math.Min(x + 1, x1) - Math.Max(x, x0));
                        sum += Pixels[rowOffset + x] * w;
                        area += w;
                    }
                }
                result[ty * targetWidth + tx] = area > 0 ? sum / area : 0;
            }
        }
        return result;
    }
}
