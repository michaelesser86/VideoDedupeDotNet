using System.Globalization;

using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

var dbPath = AppPaths.DefaultDatabase;
var remaining = new List<string>();
foreach (var a in args)
{
    if (a.StartsWith("--db=", StringComparison.OrdinalIgnoreCase))
        dbPath = a.Split('=', 2)[1];
    else
        remaining.Add(a);
}

if (remaining.Count == 0)
{
    Console.WriteLine($"""
        Usage (default database: {AppPaths.DefaultDatabase}):
          roots-add <path>          [--db=<file>]
          roots-list                [--db=<file>]
          roots-toggle <id>         [--db=<file>]
          scan-index                [--db=<file>]
          files-list                [--db=<file>] [--take=50]
          dupe-candidates           [--db=<file>] [--tol=0.25] [--min=2]
          dupe-build                [--db=<file>] [--tol=0.25] [--min=2] [--maxdist=10] [--frames=20,50,80]
                                    [--clear=true] [--refine-frames=6] [--scene-threshold=0.3]
                                    [--max-phash=16] [--margin=4] [--parallel=0]
                                    [--mirror=true] [--audio=true] [--max-audio-ber=0.25]
          dupe-list                 [--db=<file>] [--take=20]
        """);
    return 1;
}

var db = new AppDb(dbPath);
await DbInitializer.EnsureCreatedAsync(db);
var options = remaining.Skip(1).ToList();
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

try
{
    switch (remaining[0])
    {
        case "roots-add":
            {
                if (options.Count == 0) { Console.Error.WriteLine("Missing path"); return 1; }
                var id = await db.AddScanRootAsync(options[0]);
                Console.WriteLine($"Added root id={id} path={options[0]}");
                break;
            }
        case "roots-list":
            foreach (var r in await db.ListScanRootsAsync())
                Console.WriteLine($"{r.Id}\t{(r.IsEnabled == 1 ? "on" : "off")}\t{r.Path}");
            break;
        case "roots-toggle":
            {
                if (options.Count == 0 || !long.TryParse(options[0], out var rid))
                {
                    Console.Error.WriteLine("Missing/invalid id");
                    return 1;
                }
                await db.ToggleScanRootAsync(rid);
                Console.WriteLine($"Toggled root id={rid}");
                break;
            }
        case "files-list":
            foreach (var f in await db.ListMediaFilesAsync(IntOption("--take", 50)))
                Console.WriteLine($"{f.Id}\t{f.SizeBytes}\t{f.ModifiedUtc}\t{f.Path}");
            break;
        case "scan-index":
            {
                var svc = new IndexService(db, new FfprobeService(new FfmpegTools()));
                IndexProgress? last = null;
                var progress = new SynchronousProgress<IndexProgress>(p =>
                {
                    last = p;
                    if (p.Error is not null)
                        Console.Error.WriteLine($"Probe failed: {p.CurrentPath}: {p.Error}");
                    if (p.Processed > 0 && p.Processed % 100 == 0)
                        Console.WriteLine($"Indexed {p.Processed}/{p.Discovered}");
                });
                await svc.RunAsync(progress, cts.Token);
                Console.WriteLine($"Index finished. files={last?.Discovered} probed={last?.ProbedOk} " +
                    $"unchanged={last?.Unchanged} failed={last?.ProbedFail}");
                break;
            }
        case "dupe-candidates":
            {
                // Duration-only preview: which files would be compared by dupe-build.
                var opt = DupeBuildService.ParseArgs(options);
                var files = await db.ListMediaFilesForCandidatesAsync();
                var groupNo = 0;
                foreach (var bucket in DurationCandidates.CreateRanges(files, f => f.DurationSec, opt.TolSec, opt.MinItems))
                {
                    groupNo++;
                    Console.WriteLine($"\nRange {groupNo}  {bucket.Min(f => f.DurationSec):F2}–{bucket.Max(f => f.DurationSec):F2}s  items={bucket.Count}");
                    foreach (var f in bucket.OrderByDescending(x => x.SizeBytes))
                        Console.WriteLine($"  - {f.DurationSec,8:F2}s  {f.SizeBytes / 1024 / 1024,6} MiB  {f.VideoCodec ?? "?",-6} {f.Path}");
                }
                if (groupNo == 0)
                    Console.WriteLine("No candidates found. Try a larger tolerance: --tol=0.5");
                break;
            }
        case "dupe-build":
            {
                var tools = new FfmpegTools();
                var svc = new DupeBuildService(db, new FfmpegFrameExtractor(tools), new DHashService());
                var lastPhase = "";
                var progress = new SynchronousProgress<DupeBuildProgress>(p =>
                {
                    // One line per phase plus every 25 steps, so long runs stay readable.
                    if (p.Phase != lastPhase || (p.Done > 0 && p.Done % 25 == 0))
                        Console.WriteLine($"{p.Phase}: {p.Done}/{p.Total}");
                    lastPhase = p.Phase;
                });
                var saved = await svc.RunAsync(DupeBuildService.ParseArgs(options), progress, cts.Token);
                Console.WriteLine($"Saved duplicate groups: {saved}");
                break;
            }
        case "dupe-list":
            foreach (var (g, members) in await db.ListDuplicateGroupsWithMembersAsync(IntOption("--take", 20)))
            {
                Console.WriteLine($"\nGroup {g.Id}  algo={g.Algorithm} frames={g.Frames} tol={g.TolSec} maxAvgDist={g.MaxAvgDist} created={g.CreatedUtc}");
                Console.WriteLine($"Members: {members.Count}");
                foreach (var m in members.OrderBy(x => x.AvgDist))
                {
                    var notes = new List<string>();
                    if (m.IsReference) notes.Add("reference");
                    if (m.Mirrored) notes.Add("mirrored");
                    if (m.PHashDist is double p) notes.Add($"scenes={p:F1}");
                    if (m.AudioBer is double a) notes.Add($"audio={a:F2}");
                    var suffix = notes.Count > 0 ? $"  [{string.Join(", ", notes)}]" : "";
                    Console.WriteLine($"  - avgDist={m.AvgDist,5:F1}  {m.File.Path}{suffix}");
                }
            }
            break;
        default:
            Console.Error.WriteLine($"Unknown command: {remaining[0]}");
            return 1;
    }
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Canceled.");
    return 130;
}
return 0;

int IntOption(string name, int fallback) =>
    options.Select(o => o.Split('=', 2))
        .Where(p => p.Length == 2 && p[0].Equals(name, StringComparison.OrdinalIgnoreCase))
        .Select(p => int.TryParse(p[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : fallback)
        .DefaultIfEmpty(fallback)
        .Last();

/// <summary>Reports on the calling thread; <see cref="Progress{T}"/> would post to the thread pool
/// and could print lines out of order or after the command finished.</summary>
sealed class SynchronousProgress<T>(Action<T> report) : IProgress<T>
{
    private readonly Lock _gate = new();
    public void Report(T value) { lock (_gate) report(value); }
}
