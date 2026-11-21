using System.Security.Cryptography;

namespace VideoDedupe.Infrastructure;

/// <summary>
/// A cheap content fingerprint for files of equal size: the first and last megabyte plus 16
/// evenly spread 64 KB blocks, hashed with SHA-256. Reading 3 MB instead of a whole multi-GB
/// video keeps the check fast even on a hard disk; two different videos of the exact same size
/// do not agree at all 18 places.
/// </summary>
public static class ContentSampler
{
    private const int EdgeBytes = 1 << 20;
    private const int BlockBytes = 64 << 10;
    private const int Blocks = 16;

    public static async Task<string> FingerprintAsync(string path, long expectedSize, CancellationToken ct)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            bufferSize: 1, FileOptions.Asynchronous | FileOptions.RandomAccess);
        var size = stream.Length;
        if (size != expectedSize)
            throw new IOException($"File changed since the last scan: {path}");

        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[EdgeBytes];
        foreach (var (offset, count) in Ranges(size))
        {
            stream.Position = offset;
            var read = 0;
            while (read < count)
            {
                var n = await stream.ReadAsync(buffer.AsMemory(read, count - read), ct);
                if (n == 0) throw new IOException($"Unexpected end of file: {path}");
                read += n;
            }
            sha.AppendData(buffer, 0, count);
        }
        return Convert.ToHexString(sha.GetHashAndReset());
    }

    /// <summary>The byte ranges read; small files are read completely.</summary>
    internal static IEnumerable<(long Offset, int Count)> Ranges(long size)
    {
        if (size <= 2L * EdgeBytes + Blocks * BlockBytes)
        {
            for (long offset = 0; offset < size; offset += EdgeBytes)
                yield return (offset, (int)Math.Min(EdgeBytes, size - offset));
            yield break;
        }

        yield return (0, EdgeBytes);
        var middle = size - 2L * EdgeBytes - BlockBytes;
        for (var i = 0; i < Blocks; i++)
            yield return (EdgeBytes + middle * i / (Blocks - 1), BlockBytes);
        yield return (size - EdgeBytes, EdgeBytes);
    }
}
