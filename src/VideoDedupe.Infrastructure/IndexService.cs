namespace VideoDedupe.Infrastructure;

public sealed record IndexProgress(
    int Discovered,
    int Processed,
    int ProbedOk,
    int ProbedFail,
    string? CurrentPath
);

public sealed class IndexService
{
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

            var opt = r.IncludeSubdirs == 1 ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            foreach (var f in Directory.EnumerateFiles(r.Path, "*.mp4", opt))
            {
                ct.ThrowIfCancellationRequested();
                if (IsExcluded(f, r.ExcludeText))
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
        int processed = 0, ok = 0, fail = 0;

        // adjust if you want: 2–4 is a good start
        var parallel = Math.Max(1, Math.Min(Environment.ProcessorCount / 2, 4));
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

                FfprobeService.ProbeResult meta;
                try
                {
                    meta = await _probe.ProbeAsync(full, ct);
                    Interlocked.Increment(ref ok);
                }
                catch
                {
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
                    CurrentPath: full));
            }
            finally
            {
                sem.Release();
            }
        }).ToList();

        await Task.WhenAll(tasks);

        progress?.Report(new IndexProgress(
            Discovered: files.Count,
            Processed: processed,
            ProbedOk: ok,
            ProbedFail: fail,
            CurrentPath: null));
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
