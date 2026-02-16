using System.Globalization;
using System.Text.Json;

using VideoDedupe.Infrastructure;

public sealed partial class FfprobeService
{
    private readonly FfmpegTools _tools;
    public FfprobeService(FfmpegTools tools) => _tools = tools;

    public readonly record struct ProbeResult(
        double? DurationSec,
        int? Width,
        int? Height,
        double? Fps,
        string? VideoCodec,
        string? Container
    );

    public async Task<ProbeResult> ProbeAsync(string filePath, CancellationToken ct)
    {
        var args =
            "-v error -print_format json " +
            "-show_format -show_streams " +
            $"\"{filePath}\"";

        // Wenn deine RunAsync Signatur anders ist, sag’s kurz – aber du meintest: RunAsync.
        var (exitCode, stdout, stderr) = await _tools.RunAsync("ffprobe", args, ct);

        if (exitCode != 0)
            throw new InvalidOperationException($"ffprobe failed ({exitCode}). stderr: {stderr}");

        if (string.IsNullOrWhiteSpace(stdout))
            throw new InvalidOperationException("ffprobe returned empty stdout.");

        using var doc = JsonDocument.Parse(stdout);
        var root = doc.RootElement;

        // Container
        string? container = null;
        if (root.TryGetProperty("format", out var fmt) &&
            fmt.TryGetProperty("format_name", out var fn))
        {
            container = fn.GetString();
        }

        // Duration (format.duration ist normalerweise String in Sekunden)
        double? durationSec = null;
        if (root.TryGetProperty("format", out fmt) &&
            fmt.TryGetProperty("duration", out var durEl))
        {
            var durStr = durEl.ValueKind == JsonValueKind.String ? durEl.GetString()
                       : durEl.ValueKind == JsonValueKind.Number ? durEl.GetDouble().ToString(CultureInfo.InvariantCulture)
                       : null;

            if (!string.IsNullOrWhiteSpace(durStr) &&
                double.TryParse(durStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) &&
                d > 0.1 && d <= 48 * 3600)
            {
                durationSec = d;
            }
        }

        // 1. Videostream finden
        int? width = null;
        int? height = null;
        double? fps = null;
        string? codec = null;

        if (root.TryGetProperty("streams", out var streams) && streams.ValueKind == JsonValueKind.Array)
        {
            foreach (var s in streams.EnumerateArray())
            {
                if (!s.TryGetProperty("codec_type", out var ctEl) || ctEl.GetString() != "video")
                    continue;

                if (s.TryGetProperty("width", out var wEl) && wEl.ValueKind == JsonValueKind.Number)
                    width = wEl.GetInt32();

                if (s.TryGetProperty("height", out var hEl) && hEl.ValueKind == JsonValueKind.Number)
                    height = hEl.GetInt32();

                if (s.TryGetProperty("codec_name", out var cEl))
                    codec = cEl.GetString();

                // fps aus r_frame_rate (z.B. "30000/1001")
                if (s.TryGetProperty("r_frame_rate", out var rfrEl))
                    fps = TryParseFraction(rfrEl.GetString());

                // Fallback: stream.duration (manchmal vorhanden, wenn format.duration fehlt)
                if (durationSec is null && s.TryGetProperty("duration", out var sdEl))
                {
                    var sdStr = sdEl.ValueKind == JsonValueKind.String ? sdEl.GetString()
                               : sdEl.ValueKind == JsonValueKind.Number ? sdEl.GetDouble().ToString(CultureInfo.InvariantCulture)
                               : null;

                    if (!string.IsNullOrWhiteSpace(sdStr) &&
                        double.TryParse(sdStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var sd) &&
                        sd > 0.1 && sd <= 48 * 3600)
                    {
                        durationSec = sd;
                    }
                }

                break; // nur erster Videostream
            }
        }

        return new ProbeResult(durationSec, width, height, fps, codec, container);
    }

    private static double? TryParseFraction(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;
        var parts = s.Split('/');
        if (parts.Length != 2) return null;

        if (!double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out var a)) return null;
        if (!double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out var b)) return null;
        if (b == 0) return null;

        return a / b;
    }
}
