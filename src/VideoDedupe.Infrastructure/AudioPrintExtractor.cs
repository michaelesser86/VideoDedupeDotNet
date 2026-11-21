using System.Globalization;

using VideoDedupe.Core;

namespace VideoDedupe.Infrastructure;

/// <summary>Decodes a window of a file's soundtrack and fingerprints it.</summary>
public sealed class AudioPrintExtractor
{
    /// <summary>Seconds of audio around the middle of the file. Seeking there reads only this
    /// part of the file instead of the whole soundtrack.</summary>
    public const double WindowSec = 120;

    /// <summary>Identifies how cached fingerprints were made.</summary>
    public const string Strategy = "audio-hk-v1;w=120";

    private readonly FfmpegTools _tools;

    public AudioPrintExtractor(FfmpegTools tools) => _tools = tools;

    /// <summary>
    /// The fingerprint, <see cref="AudioFingerprint.Empty"/> when the file has no audio stream,
    /// or null when decoding failed for a reason that may not repeat.
    /// </summary>
    public async Task<AudioFingerprint?> ExtractAsync(string filePath, double durationSec, CancellationToken ct)
    {
        var start = Math.Max(0, durationSec / 2 - WindowSec / 2);
        string[] args =
        [
            "-hide_banner", "-loglevel", "error",
            "-ss", start.ToString("0.###", CultureInfo.InvariantCulture), "-i", filePath,
            "-t", WindowSec.ToString(CultureInfo.InvariantCulture),
            "-vn", "-sn", "-dn", "-ac", "1", "-ar", AudioFingerprint.SampleRate.ToString(CultureInfo.InvariantCulture),
            "-f", "s16le", "-"
        ];

        byte[] pcm;
        try
        {
            pcm = await _tools.RunToBytesAsync(_tools.FfmpegPath, args, ct);
        }
        catch (ExternalToolException ex) when (IsMissingAudio(ex.Message))
        {
            return AudioFingerprint.Empty;
        }
        catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
        {
            System.Diagnostics.Trace.TraceWarning($"Audio fingerprint failed: {filePath}: {ex.Message}");
            return null;
        }

        var samples = new short[pcm.Length / 2];
        Buffer.BlockCopy(pcm, 0, samples, 0, samples.Length * 2);
        if (!BitConverter.IsLittleEndian)
            for (var i = 0; i < samples.Length; i++)
                samples[i] = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(samples[i]);
        return AudioFingerprint.Compute(samples);
    }

    // Without an audio stream ffmpeg has nothing to write ("does not contain any stream", or an
    // empty output when -vn removed the only stream).
    private static bool IsMissingAudio(string message) =>
        message.Contains("does not contain any stream", StringComparison.OrdinalIgnoreCase) ||
        message.Contains("produced no output", StringComparison.OrdinalIgnoreCase) ||
        message.Contains("matches no streams", StringComparison.OrdinalIgnoreCase);
}
