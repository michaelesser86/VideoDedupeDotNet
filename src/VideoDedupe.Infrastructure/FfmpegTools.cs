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
        ct.ThrowIfCancellationRequested();
        if (!File.Exists(exe) && exe.IndexOf(Path.DirectorySeparatorChar) >= 0)
            throw new FileNotFoundException($"ffprobe not found at path: {exe}");

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
        var stdoutTask = p.StandardOutput.ReadToEndAsync(ct);
        var stderrTask = p.StandardError.ReadToEndAsync(ct);
        try
        {
            await Task.WhenAll(stdoutTask, stderrTask, p.WaitForExitAsync(ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await TerminateAsync(p);
            throw;
        }
        return (p.ExitCode, await stdoutTask, await stderrTask);
    }

    public async Task<byte[]> RunToBytesAsync(string exe, string arguments, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var p = Process.Start(psi)
            ?? throw new InvalidOperationException($"Failed to start: {exe}");

        // Read stderr in parallel (important for diagnostics)
        var stderrTask = p.StandardError.ReadToEndAsync(ct);

        await using var ms = new MemoryStream();
        try
        {
            await Task.WhenAll(p.StandardOutput.BaseStream.CopyToAsync(ms, ct),
                stderrTask, p.WaitForExitAsync(ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await TerminateAsync(p);
            throw;
        }
        var stderr = await stderrTask;

        if (p.ExitCode != 0)
            throw new ExternalToolException($"{exe} failed ({p.ExitCode}). stderr: {stderr}");

        if (ms.Length == 0)
            throw new ExternalToolException($"{exe} produced no output. args: {arguments} stderr: {stderr}");

        return ms.ToArray();
    }

    private static async Task TerminateAsync(Process process)
    {
        try
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException) when (process.HasExited)
        {
            // The process exited between checking its state and killing it.
        }
        await process.WaitForExitAsync(CancellationToken.None);
    }
}
