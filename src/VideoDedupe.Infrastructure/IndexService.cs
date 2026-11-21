namespace VideoDedupe.Infrastructure;

public sealed record IndexProgress(
    int Discovered,
    int Processed,
    int ProbedOk,
    int ProbedFail,
    string? CurrentPath,
    string? Error = null,
    int Unchanged = 0
);

public sealed class IndexService
{
    /// <summary>Containers ffmpeg reads that commonly hold videos.</summary>
    public static readonly IReadOnlySet<string> VideoExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        ".mp4", ".m4v", ".mkv", ".webm", ".avi", ".mov", ".wmv", ".flv",
        ".mpg", ".mpeg", ".ts", ".m2ts", ".mts", ".3gp", ".ogv"
    };

    private readonly AppDb _db;
    private readonly FfprobeService _probe;

    public IndexService(AppDb db, FfprobeService probe)
    {
        _db = db;
        _probe = probe;
    }

    public async Task RunAsync(
        IProgress<IndexProgress>? progress,
        CancellationToken ct)
    {
        await DbInitializer.EnsureCreatedAsync(_db);

        var roots = await _db.ListScanRootsAsync();
        var enabled = roots.Where(r => r.IsEnabled == 1).ToList();

        if (enabled.Count == 0)
            throw new InvalidOperationException("No enabled scan roots. Add one with roots-add or enable in UI.");

        // 1) Discover files
        var files = new List<(string RootPath, string FilePath, string? ExcludeText)>();

        foreach (var r in enabled)
        {
            ct.ThrowIfCancellationRequested();

            if (!Directory.Exists(r.Path))
                continue;

            // One unreadable folder (system folders, other users' profiles, a vanished
            // network share) must not abort the whole scan.
            var opt = new EnumerationOptions
            {
                RecurseSubdirectories = r.IncludeSubdirs == 1,
                IgnoreInaccessible = true,
                AttributesToSkip = FileAttributes.System,
                MatchCasing = MatchCasing.CaseInsensitive
            };

            foreach (var f in Directory.EnumerateFiles(r.Path, "*", opt))
            {
                ct.ThrowIfCancellationRequested();
                if (!VideoExtensions.Contains(Path.GetExtension(f)) || IsExcluded(f, r.ExcludeText))
                    continue;

                files.Add((r.Path, f, r.ExcludeText));
            }
        }

        progress?.Report(new IndexProgress(
            Discovered: files.Count,
            Processed: 0,
            ProbedOk: 0,
            ProbedFail: 0,
            CurrentPath: null));

        // 2) Probe + upsert (limited parallelism)
        int processed = 0, ok = 0, fail = 0, unchanged = 0;
        var known = await _db.ListIndexedFileStatesAsync();

        // ffprobe only reads container headers; a few at once hide the process start latency.
        var parallel = Math.Clamp(Environment.ProcessorCount / 2, 2, 8);
        using var sem = new SemaphoreSlim(parallel);

        var tasks = files.Select(async item =>
        {
            await sem.WaitAsync(ct);
            try
            {
                ct.ThrowIfCancellationRequested();

                var full = Path.GetFullPath(item.FilePath);
                var fi = new FileInfo(full);

                var modifiedUtc = fi.LastWriteTimeUtc.ToString("O");
                var scannedUtc = DateTime.UtcNow.ToString("O");

                // Same size and timestamp as the last successful probe: ffprobe would only
                // repeat itself. Failed probes and returning (deleted) files are re-probed.
                if (known.TryGetValue(full, out var state) && state.IsDeleted == 0 && state.DurationSec is not null &&
                    state.SizeBytes == fi.Length && state.ModifiedUtc == modifiedUtc)
                {
                    Interlocked.Increment(ref unchanged);
                    progress?.Report(new IndexProgress(files.Count, Interlocked.Increment(ref processed),
                        Volatile.Read(ref ok), Volatile.Read(ref fail), full, Unchanged: Volatile.Read(ref unchanged)));
                    return;
                }

                FfprobeService.ProbeResult meta;
                string? error = null;
                try
                {
                    meta = await _probe.ProbeAsync(full, ct);
                    Interlocked.Increment(ref ok);
                }
                catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
                {
                    error = ex.Message;
                    System.Diagnostics.Trace.TraceWarning($"Probe failed: {full}: {ex}");
                    meta = new FfprobeService.ProbeResult(null, null, null, null, null, null);
                    Interlocked.Increment(ref fail);
                }

                await _db.UpsertMediaFileAsync(new AppDb.MediaFileRow
                {
                    Path = full,
                    SizeBytes = fi.Length,
                    ModifiedUtc = modifiedUtc,
                    LastScannedUtc = scannedUtc,
                    DurationSec = meta.DurationSec,
                    Width = meta.Width,
                    Height = meta.Height,
                    Fps = meta.Fps,
                    VideoCodec = meta.VideoCodec,
                    Container = meta.Container
                });

                var p = Interlocked.Increment(ref processed);

                progress?.Report(new IndexProgress(
                    Discovered: files.Count,
                    Processed: p,
                    ProbedOk: Volatile.Read(ref ok),
                    ProbedFail: Volatile.Read(ref fail),
                    CurrentPath: full,
                    Error: error,
                    Unchanged: Volatile.Read(ref unchanged)));
            }
            finally
            {
                sem.Release();
            }
        }).ToList();

        await Task.WhenAll(tasks);

        // 3) Files that vanished from a reachable root leave the active library. A missing
        //    root (e.g. an unplugged drive) was skipped above and is left untouched here.
        foreach (var r in enabled.Where(r => Directory.Exists(r.Path)))
        {
            foreach (var (id, path) in await _db.ListActiveFilesUnderAsync(r.Path))
            {
                ct.ThrowIfCancellationRequested();
                if (!File.Exists(path))
                    await _db.MarkMediaFileDeletedAsync(id);
            }
        }

        progress?.Report(new IndexProgress(
            Discovered: files.Count,
            Processed: processed,
            ProbedOk: ok,
            ProbedFail: fail,
            CurrentPath: null,
            Unchanged: unchanged));
    }

    private static bool IsExcluded(string path, string? excludeText)
    {
        if (string.IsNullOrWhiteSpace(excludeText))
            return false;

        var tokens = excludeText.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var t in tokens)
        {
            var token = t.Trim('*').Trim();
            if (token.Length == 0) continue;

            if (path.Contains(token, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}
