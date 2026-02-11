using System.Globalization;

namespace VideoDedupe.Infrastructure;

public sealed class FfmpegFrameExtractor
{
    private readonly FfmpegTools _tools;

    public FfmpegFrameExtractor(FfmpegTools tools) => _tools = tools ?? throw new ArgumentNullException(nameof(tools));

    public async Task<byte[]> ExtractFramePngAsync(string filePath, double timestampSec, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("filePath is null/empty", nameof(filePath));

        var ts = timestampSec.ToString(CultureInfo.InvariantCulture);

        // Fast path: -ss before -i
        var fastArgs = $"-hide_banner -loglevel error -ss {ts} -i \"{filePath}\" -frames:v 1 -an -f image2pipe -vcodec png -";

        try
        {
            return await _tools.RunToBytesAsync(_tools.FfmpegPath, fastArgs, ct);
        }
        catch
        {
            // Fallback: more accurate/reliable seek (-ss after -i)
            var slowArgs = $"-hide_banner -loglevel error -i \"{filePath}\" -ss {ts} -frames:v 1 -an -f image2pipe -vcodec png -";
            return await _tools.RunToBytesAsync(_tools.FfmpegPath, slowArgs, ct);
        }
    }
}
