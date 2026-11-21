using System.Globalization;

using VideoDedupe.Core;

namespace VideoDedupe.Infrastructure;

public sealed class FfmpegFrameExtractor
{
    private readonly FfmpegTools _tools;
    internal FfmpegTools Tools => _tools;

    public FfmpegFrameExtractor(FfmpegTools tools) => _tools = tools ?? throw new ArgumentNullException(nameof(tools));

    /// <summary>Width used for hashing: plenty for 9×8 dHash, 32×32 pHash and black-bar detection.</summary>
    public const int HashWidth = 256;

    /// <summary>
    /// One frame as 8-bit grayscale, scaled inside ffmpeg. Hashing only needs luminance, so this
    /// skips colour conversion and PNG compression entirely.
    /// </summary>
    public async Task<GrayFrame> ExtractGrayFrameAsync(string filePath, double timestampSec, CancellationToken ct,
        int width = HashWidth)
    {
        var bytes = await ExtractAsync(filePath, timestampSec, ct, width, ["-pix_fmt", "gray", "-vcodec", "pgm"]);
        return GrayFrame.ParsePgm(bytes);
    }

    /// <summary>
    /// Several frames of one file from a single ffmpeg process: the file is opened once per
    /// timestamp as a separate input (each with its own fast seek, so the frames are exactly those
    /// of <see cref="ExtractGrayFrameAsync"/>) and the frames are stacked into one image. This saves
    /// a process start, container open and decoder setup per frame.
    /// </summary>
    /// <returns>One entry per timestamp; <c>null</c> where that frame could not be extracted.</returns>
    public async Task<IReadOnlyList<GrayFrame?>> ExtractGrayFramesAsync(string filePath,
        IReadOnlyList<double> timestampsSec, CancellationToken ct, int width = HashWidth)
    {
        if (timestampsSec.Count == 0)
            return [];
        if (timestampsSec.Count > 1)
        {
            try
            {
                return SplitStack(await RunStackedAsync(filePath, timestampsSec, width, ct), timestampsSec.Count);
            }
            catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
            {
                // e.g. one timestamp cannot be decoded: retry frame by frame so only that one is lost.
                System.Diagnostics.Trace.TraceWarning($"Stacked extraction failed, retrying per frame: {filePath}: {ex.Message}");
            }
        }

        var frames = new GrayFrame?[timestampsSec.Count];
        for (var i = 0; i < frames.Length; i++)
        {
            try
            {
                frames[i] = await ExtractGrayFrameAsync(filePath, timestampsSec[i], ct, width);
            }
            catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
            {
                System.Diagnostics.Trace.TraceWarning($"Frame at {timestampsSec[i]}s failed: {filePath}: {ex.Message}");
            }
        }
        return frames;
    }

    private async Task<GrayFrame> RunStackedAsync(string filePath, IReadOnlyList<double> timestamps, int width,
        CancellationToken ct)
    {
        var args = new List<string> { "-hide_banner", "-loglevel", "error" };
        foreach (var ts in timestamps)
            args.AddRange(["-ss", ts.ToString(CultureInfo.InvariantCulture), "-i", filePath]);

        var graph = string.Concat(timestamps.Select((_, i) =>
            $"[{i}:v]trim=end_frame=1,scale={width}:-2,format=gray[f{i}];"));
        graph += string.Concat(timestamps.Select((_, i) => $"[f{i}]")) + $"vstack=inputs={timestamps.Count}";

        args.AddRange(["-filter_complex", graph, "-frames:v", "1", "-an",
            "-f", "image2pipe", "-vcodec", "pgm", "-"]);
        return GrayFrame.ParsePgm(await _tools.RunToBytesAsync(_tools.FfmpegPath, args, ct));
    }

    internal static IReadOnlyList<GrayFrame?> SplitStack(GrayFrame stack, int count)
    {
        if (stack.Height % count != 0)
            throw new InvalidDataException($"Stacked frame height {stack.Height} is not divisible by {count}.");
        var height = stack.Height / count;
        var size = stack.Width * height;
        return Enumerable.Range(0, count)
            .Select(i => (GrayFrame?)new GrayFrame(stack.Width, height, stack.Pixels.AsSpan(i * size, size).ToArray()))
            .ToList();
    }

    /// <param name="width">Scale the frame to this width inside ffmpeg (aspect kept). Encoding and
    /// decoding a full 4K PNG costs far more than a thumbnail needs.</param>
    /// <summary>Size of the keyframe images: small, and fixed so the raw stream splits into frames.</summary>
    public const int KeyframeWidth = 128, KeyframeHeight = 72;

    /// <summary>
    /// Every keyframe of the file with its time, decoded at low resolution. Only keyframes are
    /// decoded, which is several times faster than the full video, but the whole file is read.
    /// </summary>
    public async Task<IReadOnlyList<(long TimeMs, GrayFrame Frame)>> ExtractKeyframesAsync(string filePath, CancellationToken ct)
    {
        string[] args =
        [
            "-hide_banner", "-loglevel", "info", "-skip_frame", "nokey", "-i", filePath,
            "-an", "-sn", "-dn", "-fps_mode", "passthrough",
            "-vf", $"scale={KeyframeWidth}:{KeyframeHeight}:flags=area,format=gray,showinfo",
            "-f", "rawvideo", "-pix_fmt", "gray", "-"
        ];
        var (raw, log) = await _tools.RunToBytesWithLogAsync(_tools.FfmpegPath, args, ct);
        return SplitKeyframes(raw, log);
    }

    /// <summary><paramref name="perSecond"/> frames per second of the whole file (a full decode, so short files only).</summary>
    public async Task<IReadOnlyList<(long TimeMs, GrayFrame Frame)>> ExtractDenseFramesAsync(string filePath, int perSecond,
        CancellationToken ct)
    {
        string[] args =
        [
            "-hide_banner", "-loglevel", "info", "-i", filePath, "-an", "-sn", "-dn",
            "-vf", $"fps={perSecond},scale={KeyframeWidth}:{KeyframeHeight}:flags=area,format=gray,showinfo",
            "-f", "rawvideo", "-pix_fmt", "gray", "-"
        ];
        var (raw, log) = await _tools.RunToBytesWithLogAsync(_tools.FfmpegPath, args, ct);
        return SplitKeyframes(raw, log);
    }

    internal static IReadOnlyList<(long TimeMs, GrayFrame Frame)> SplitKeyframes(byte[] raw, string log)
    {
        const int size = KeyframeWidth * KeyframeHeight;
        var times = SceneFrameSelector.ParsePtsTimes(log);
        var count = Math.Min(raw.Length / size, times.Count);
        var frames = new List<(long, GrayFrame)>(count);
        for (var i = 0; i < count; i++)
            frames.Add(((long)Math.Round(times[i] * 1000), new GrayFrame(KeyframeWidth, KeyframeHeight, raw.AsSpan(i * size, size).ToArray())));
        return frames;
    }

    public Task<byte[]> ExtractFramePngAsync(string filePath, double timestampSec, CancellationToken ct,
        int? width = null) =>
        ExtractAsync(filePath, timestampSec, ct, width, ["-vcodec", "png"]);

    private async Task<byte[]> ExtractAsync(string filePath, double timestampSec, CancellationToken ct,
        int? width, string[] output)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("filePath is null/empty", nameof(filePath));
        if (width is <= 0)
            throw new ArgumentOutOfRangeException(nameof(width));

        var ts = timestampSec.ToString(CultureInfo.InvariantCulture);
        string[] scale = width is null ? [] : ["-vf", $"scale={width.Value}:-2"];
        string[] tail = ["-frames:v", "1", "-an", .. scale, "-f", "image2pipe", .. output, "-"];

        try
        {
            // Fast path: -ss before -i seeks to the preceding keyframe and decodes forward.
            return await _tools.RunToBytesAsync(_tools.FfmpegPath,
                ["-hide_banner", "-loglevel", "error", "-ss", ts, "-i", filePath, .. tail], ct);
        }
        catch (ExternalToolException)
        {
            // Fallback: decode from the start for files whose index does not allow seeking.
            return await _tools.RunToBytesAsync(_tools.FfmpegPath,
                ["-hide_banner", "-loglevel", "error", "-i", filePath, "-ss", ts, .. tail], ct);
        }
    }
}
