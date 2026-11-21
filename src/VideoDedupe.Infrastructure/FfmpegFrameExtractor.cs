using System.Globalization;

namespace VideoDedupe.Infrastructure;

public sealed class FfmpegFrameExtractor
{
    private readonly FfmpegTools _tools;
    internal FfmpegTools Tools => _tools;

    public FfmpegFrameExtractor(FfmpegTools tools) => _tools = tools ?? throw new ArgumentNullException(nameof(tools));

    /// <summary>Width used for hashing: plenty for 9×8 dHash, 32×32 pHash and black-bar detection.</summary>
    public const int HashWidth = 256;

    /// <param name="width">Scale the frame to this width inside ffmpeg (aspect kept). Encoding and
    /// decoding a full 4K PNG costs far more than the hash or thumbnail needs.</param>
    public async Task<byte[]> ExtractFramePngAsync(string filePath, double timestampSec, CancellationToken ct,
        int? width = null)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("filePath is null/empty", nameof(filePath));
        if (width is <= 0)
            throw new ArgumentOutOfRangeException(nameof(width));

        var ts = timestampSec.ToString(CultureInfo.InvariantCulture);
        var scale = width is null ? "" : $"-vf scale={width.Value}:-2 ";

        // Fast path: -ss before -i
        var fastArgs = $"-hide_banner -loglevel error -ss {ts} -i \"{filePath}\" -frames:v 1 -an {scale}-f image2pipe -vcodec png -";

        try
        {
            return await _tools.RunToBytesAsync(_tools.FfmpegPath, fastArgs, ct);
        }
        catch (ExternalToolException)
        {
            // Fallback: more accurate/reliable seek (-ss after -i)
            var slowArgs = $"-hide_banner -loglevel error -i \"{filePath}\" -ss {ts} -frames:v 1 -an {scale}-f image2pipe -vcodec png -";
            return await _tools.RunToBytesAsync(_tools.FfmpegPath, slowArgs, ct);
        }
    }
}
