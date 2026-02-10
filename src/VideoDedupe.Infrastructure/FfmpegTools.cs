using System.Diagnostics;

namespace VideoDedupe.Infrastructure;

public sealed class FfmpegTools
{
    public string FfprobePath { get; }
    public string FfmpegPath { get; }

    public FfmpegTools(string? ffprobePath = null, string? ffmpegPath = null)
    {
        FfprobePath = ffprobePath ?? "ffprobe";
        FfmpegPath = ffmpegPath ?? "ffmpeg";
    }

    public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(string exe, string arguments, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var p = Process.Start(psi) ?? throw new InvalidOperationException($"Failed to start: {exe}");
        var stdoutTask = p.StandardOutput.ReadToEndAsync();
        var stderrTask = p.StandardError.ReadToEndAsync();

        await p.WaitForExitAsync(ct);
        return (p.ExitCode, await stdoutTask, await stderrTask);
    }
}
