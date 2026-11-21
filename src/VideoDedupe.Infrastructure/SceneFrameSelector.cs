using System.Globalization;
using System.Text.RegularExpressions;

namespace VideoDedupe.Infrastructure;

/// <summary>Chooses stable, representative frames; scene changes are preferred over fixed percentages.</summary>
public sealed class SceneFrameSelector
{
    private static readonly Regex PtsTime = new(@"pts_time:(?<time>-?[0-9.]+)", RegexOptions.Compiled);
    private readonly FfmpegTools _tools;

    public SceneFrameSelector(FfmpegTools tools) => _tools = tools;

    public async Task<IReadOnlyList<double>> SelectAsync(string filePath, double durationSec, int count,
        double sceneThreshold, CancellationToken ct)
    {
        if (count < 1) throw new ArgumentOutOfRangeException(nameof(count));
        var timestamps = new List<double>();
        try
        {
            var threshold = sceneThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            // Decoding only keyframes at low resolution is several times faster than a full decode.
            // Encoders place keyframes on hard cuts, so the detected cuts stay the same; otherwise
            // the next keyframe (still inside the new scene) is reported.
            string[] args = ["-hide_banner", "-loglevel", "info", "-skip_frame", "nokey", "-i", filePath,
                "-vf", $"scale=160:-2,select='gt(scene,{threshold})',showinfo", "-an", "-sn", "-dn", "-f", "null", "-"];
            var result = await _tools.RunAsync(_tools.FfmpegPath, args, ct);
            if (result.ExitCode == 0)
                timestamps = ParseTimestamps(result.StdErr, durationSec).ToList();
        }
        catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
        {
            System.Diagnostics.Trace.TraceWarning($"Scene detection failed: {filePath}: {ex.Message}");
        }

        return SelectRepresentative(timestamps, durationSec, count);
    }

    /// <summary>Every pts_time of a showinfo log, in output order.</summary>
    public static List<double> ParsePtsTimes(string output) =>
        PtsTime.Matches(output)
            .Select(match => double.TryParse(match.Groups["time"].Value, NumberStyles.Float,
                CultureInfo.InvariantCulture, out var value) ? value : double.NaN)
            .ToList();

    public static IReadOnlyList<double> ParseTimestamps(string output, double durationSec) =>
        PtsTime.Matches(output)
            .Select(match => double.TryParse(match.Groups["time"].Value, NumberStyles.Float,
                CultureInfo.InvariantCulture, out var value) ? value : double.NaN)
            .Where(value => double.IsFinite(value) && value > 0.1 && value < durationSec - 0.1)
            .DistinctBy(value => Math.Round(value, 3))
            .Order()
            .ToList();

    public static IReadOnlyList<double> SelectRepresentative(IReadOnlyList<double> sceneTimes, double durationSec, int count)
    {
        var safeDuration = Math.Max(durationSec, 0.2);
        if (sceneTimes.Count >= count)
            return Enumerable.Range(0, count)
                .Select(index => sceneTimes[(int)Math.Round(index * (sceneTimes.Count - 1.0) / Math.Max(1, count - 1))])
                .Select(time => Math.Clamp(time, 0.1, safeDuration - 0.1))
                .ToList();

        // A deterministic fallback gives every video the same number of cache slots,
        // even when ffmpeg cannot identify enough cuts (slideshows, fades, short clips).
        return Enumerable.Range(1, count)
            .Select(index => Math.Clamp(safeDuration * index / (count + 1.0), 0.1, safeDuration - 0.1))
            .ToList();
    }
}
