using System.Text.Json;

namespace VideoDedupe.Infrastructure;

public sealed class FfprobeService
{
    private readonly FfmpegTools _tools;

    public FfprobeService(FfmpegTools tools) => _tools = tools;

    public async Task<ProbeResult> ProbeAsync(string filePath, CancellationToken ct)
    {
        var args = $"-v error -print_format json -show_format -show_streams \"{filePath}\"";
        var (code, stdout, stderr) = await _tools.RunAsync(_tools.FfprobePath, args, ct);
        if (code != 0) throw new InvalidOperationException($"ffprobe failed: {stderr}");

        using var doc = JsonDocument.Parse(stdout);

        double? duration = null;
        string? container = null;

        if (doc.RootElement.TryGetProperty("format", out var fmt))
        {
            if (fmt.TryGetProperty("duration", out var durEl) &&
                double.TryParse(durEl.GetString(), out var d))
                duration = d;

            if (fmt.TryGetProperty("format_name", out var cn))
                container = cn.GetString();
        }

        int? width = null;
        int? height = null;
        double? fps = null;
        string? vcodec = null;

        if (doc.RootElement.TryGetProperty("streams", out var streams) &&
            streams.ValueKind == JsonValueKind.Array)
        {
            foreach (var s in streams.EnumerateArray())
            {
                if (s.TryGetProperty("codec_type", out var ctEl) &&
                    ctEl.GetString() == "video")
                {
                    if (s.TryGetProperty("width", out var w)) width = w.GetInt32();
                    if (s.TryGetProperty("height", out var h)) height = h.GetInt32();
                    if (s.TryGetProperty("codec_name", out var c)) vcodec = c.GetString();

                    if (s.TryGetProperty("avg_frame_rate", out var fr) || s.TryGetProperty("r_frame_rate", out fr))
                        fps = ParseFps(fr.GetString());

                    break;
                }
            }
        }

        return new ProbeResult(duration, width, height, fps, vcodec, container);
    }

    private static double? ParseFps(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;
        var parts = s.Split('/');
        if (parts.Length == 2 &&
            double.TryParse(parts[0], out var n) &&
            double.TryParse(parts[1], out var d) &&
            d != 0)
            return n / d;

        if (double.TryParse(s, out var v)) return v;
        return null;
    }

    public record ProbeResult(double? DurationSec, int? Width, int? Height, double? Fps, string? VideoCodec, string? Container);
}
