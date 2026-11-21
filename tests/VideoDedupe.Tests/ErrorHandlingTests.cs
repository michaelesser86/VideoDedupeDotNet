using System.Diagnostics;
using System.Text;
using Microsoft.Data.Sqlite;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class ErrorHandlingTests
{
    [Fact]
    public async Task SchemaErrorsAreNotSuppressed()
    {
        using var workspace = new TestWorkspace();
        await workspace.Db.ExecuteAsync(@"
CREATE VIEW ScanRoot AS SELECT 1 AS Id;
CREATE TABLE MediaFile(Id INTEGER PRIMARY KEY, Path TEXT);");
        await Assert.ThrowsAsync<SqliteException>(() => DbInitializer.EnsureCreatedAsync(workspace.Db));
    }

    [Fact]
    public async Task PreCanceledExtractionDoesNotStartProcessOrFallback()
    {
        using var workspace = new TestWorkspace();
        var extractor = new FfmpegFrameExtractor(new FfmpegTools(
            ffmpegPath: Path.Combine(workspace.Root, "does-not-exist")));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            extractor.ExtractFramePngAsync("video.mp4", 1, new CancellationToken(true)));
    }

    [Fact]
    public void DatabaseAndCancellationErrorsAreNotRecoverableMediaErrors()
    {
        Assert.False(MediaErrors.IsRecoverable(new OperationCanceledException()));
        Assert.False(MediaErrors.IsRecoverable(new SqliteException("write failed", 1)));
        Assert.False(MediaErrors.IsRecoverable(new NullReferenceException()));
        Assert.True(MediaErrors.IsRecoverable(new ExternalToolException("bad video")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationTerminatesRunningProcess(bool binaryOutput)
    {
        using var workspace = new TestWorkspace();
        var marker = Path.Combine(workspace.Root, "pid.txt");
        string executable;
        string[] arguments;
        if (OperatingSystem.IsWindows())
        {
            executable = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System),
                "WindowsPowerShell", "v1.0", "powershell.exe");
            var script = $"[IO.File]::WriteAllText('{marker.Replace("'", "''")}', [string]$PID); while ($true) {{ Start-Sleep -Milliseconds 100 }}";
            arguments = ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", Convert.ToBase64String(Encoding.Unicode.GetBytes(script))];
        }
        else
        {
            executable = "/bin/sh";
            var script = Path.Combine(workspace.Root, "wait.sh");
            await File.WriteAllTextAsync(script, $"echo $$ > '{marker.Replace("'", "'\\''")}'\nwhile :; do sleep 1; done\n");
            arguments = [script];
        }

        using var cts = new CancellationTokenSource();
        var tools = new FfmpegTools();
        Task run = binaryOutput ? tools.RunToBytesAsync(executable, arguments, cts.Token)
            : tools.RunAsync(executable, arguments, cts.Token);
        Process? process = null;
        try
        {
            // Only a stand-in for ffmpeg has to start here. On a fresh CI runner the first
            // Windows PowerShell start took over 15 s, so allow a minute; a working machine
            // writes the marker in well under a second.
            var deadline = Stopwatch.StartNew();
            int pid;
            // The child may still hold the marker open right after writing it.
            while (!TryReadPid(marker, out pid))
            {
                if (run.IsCompleted) await run;
                Assert.True(deadline.Elapsed < TimeSpan.FromMinutes(1),
                    "The child process did not write its PID within a minute.");
                await Task.Delay(25);
            }
            process = Process.GetProcessById(pid);
            Assert.False(process.HasExited);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.True(process.HasExited);
        }
        finally
        {
            cts.Cancel();
            if (process is not null)
            {
                if (!process.HasExited) process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
                process.Dispose();
            }
            try { await run.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (OperationCanceledException) { }
        }
    }

    private static bool TryReadPid(string marker, out int pid)
    {
        pid = 0;
        try
        {
            return File.Exists(marker) && int.TryParse(File.ReadAllText(marker).Trim(), out pid);
        }
        catch (IOException)
        {
            return false;
        }
    }
}
