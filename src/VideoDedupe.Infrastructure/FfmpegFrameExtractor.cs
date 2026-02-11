namespace VideoDedupe.Infrastructure;

public sealed class FfmpegFrameExtractor
{
    private readonly FfmpegTools _tools;

    public FfmpegFrameExtractor(FfmpegTools _tools) => _tools = _tools;

    public async Task<byte[]> ExtractFramePngAsync(string filePath, double timestampSec, CancellationToken ct)
    {
        var ts = timestampSec.ToString(System.Globalization.CultureInfo.InvariantCulture);

        // -ss vor -i: schnell; 1 Frame; Ausgabe als PNG über stdout
        var args = $"-v error -ss {ts} -i \"{filePath}\" -frames:v 1 -f image2pipe -vcodec png -";
        return await _tools.RunToBytesAsync(_tools.FfmpegPath, args, ct);
    }
}
