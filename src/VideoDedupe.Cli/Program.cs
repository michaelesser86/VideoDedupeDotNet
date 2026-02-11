using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

string dbPath = "videodedupe.db";
var remaining = new List<string>();

foreach (var a in args)
{
    if (a.StartsWith("--db=", StringComparison.OrdinalIgnoreCase))
        dbPath = a.Split("=", 2)[1];
    else
        remaining.Add(a);
}

if (remaining.Count == 0)
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  scan-index [--db=videodedupe.db]");
    Console.WriteLine("  files-list [--db=videodedupe.db]");
    Console.WriteLine("  dupe-candidates [--db=videodedupe.db] [--tol=0.25] [--min=2]");
    Console.WriteLine("  dupe-verify [--db=videodedupe.db] [--tol=0.25] [--min=2] [--maxdist=6]");

    Console.WriteLine("  roots-add <path> [--db=videodedupe.db]");
    Console.WriteLine("  roots-list [--db=videodedupe.db]");
    Console.WriteLine("  roots-toggle <id> [--db=videodedupe.db]");
    return;
}

var db = new AppDb(dbPath);
await DbInitializer.EnsureCreatedAsync(db);

var cmd = remaining[0];

switch (cmd)
{
    case "roots-add":
        {
            if (remaining.Count < 2) { Console.WriteLine("Missing path"); return; }
            var id = await db.AddScanRootAsync(remaining[1]);
            Console.WriteLine($"Added root id={id} path={remaining[1]}");
            break;
        }
    case "roots-list":
        {
            var roots = await db.ListScanRootsAsync();
            foreach (var r in roots)
                Console.WriteLine($"{r.Id}\t{r.IsEnabled}\t{r.Path}");
            break;
        }
    case "roots-toggle":
        {
            if (remaining.Count < 2 || !long.TryParse(remaining[1], out var rid))
            {
                Console.WriteLine("Missing/invalid id");
                return;
            }
            await db.ToggleScanRootAsync(rid);
            Console.WriteLine($"Toggled root id={rid}");
            break;
        }
    case "files-list":
        {
            var rows = await db.ListMediaFilesAsync(50);
            foreach (var f in rows)
                Console.WriteLine($"{f.Id}\t{f.SizeBytes}\t{f.ModifiedUtc}\t{f.Path}");
            break;
        }

    case "scan-index":
        {
            var roots = await db.ListScanRootsAsync();
            var enabled = roots.Where(r => r.IsEnabled == 1).ToList();

            if (enabled.Count == 0)
            {
                Console.WriteLine("No enabled scan roots. Add one with roots-add or toggle one on.");
                return;
            }

            int indexed = 0;
            int skipped = 0;

            var tools = new FfmpegTools();
            var probe = new FfprobeService(tools);

            foreach (var r in enabled)
            {
                if (!Directory.Exists(r.Path))
                {
                    Console.WriteLine($"Missing root: {r.Path}");
                    continue;
                }

                Console.WriteLine($"Indexing root: {r.Path}");

                foreach (var file in Directory.EnumerateFiles(r.Path, "*.mp4", SearchOption.AllDirectories))
                {
                    try
                    {
                        var full = Path.GetFullPath(file);
                        var fi = new FileInfo(full);

                        // store as ISO-8601
                        var modifiedUtc = fi.LastWriteTimeUtc.ToString("O");
                        var scannedUtc = DateTime.UtcNow.ToString("O");
                        FfprobeService.ProbeResult meta;
                        try
                        {
                            meta = await probe.ProbeAsync(full, CancellationToken.None);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"ffprobe failed: {full} :: {ex.Message}");
                            meta = new FfprobeService.ProbeResult(null, null, null, null, null, null);
                        }
                        await db.UpsertMediaFileAsync(new AppDb.MediaFileRow
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

                        indexed++;
                        if (indexed % 250 == 0)
                            Console.WriteLine($"Indexed {indexed} files...");
                    }
                    catch (Exception ex)
                    {
                        skipped++;
                        Console.WriteLine($"Skip: {file} :: {ex.Message}");
                    }
                }
            }

            Console.WriteLine($"Done. Indexed={indexed}, Skipped={skipped}");
            break;
        }
    case "dupe-candidates":
        {
            // defaults
            double tol = 0.25;
            int minItems = 2;

            // parse optional args from remaining list: --tol=0.25 --min=2
            foreach (var a in remaining.Skip(1))
            {
                if (a.StartsWith("--tol=", StringComparison.OrdinalIgnoreCase) &&
                    double.TryParse(a.Split("=", 2)[1], System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture, out var t))
                    tol = t;

                if (a.StartsWith("--min=", StringComparison.OrdinalIgnoreCase) &&
                    int.TryParse(a.Split("=", 2)[1], out var m))
                    minItems = m;
            }

            var files = await db.ListMediaFilesForCandidatesAsync();

            // bucket by resolution + rounded duration (0.1s)
            var buckets = files
                .GroupBy(f => (f.Width!.Value, f.Height!.Value, DurKey: Math.Round(f.DurationSec!.Value, 1)))
                .Where(g => g.Count() >= minItems)
                .OrderByDescending(g => g.Count())
                .ToList();

            int groupNo = 0;

            foreach (var b in buckets)
            {
                // refine: split bucket into sub-groups by tolerance around a seed duration
                var remainingFiles = b.OrderBy(f => f.DurationSec).ToList();

                while (remainingFiles.Count >= minItems)
                {
                    var seed = remainingFiles[0];
                    var seedDur = seed.DurationSec!.Value;

                    var cluster = remainingFiles
                        .Where(f => Math.Abs(f.DurationSec!.Value - seedDur) <= tol)
                        .ToList();

                    // remove clustered
                    foreach (var c in cluster)
                        remainingFiles.Remove(c);

                    if (cluster.Count < minItems)
                        continue;

                    groupNo++;

                    var w = b.Key.Item1;
                    var h = b.Key.Item2;
                    var avgDur = cluster.Average(x => x.DurationSec!.Value);

                    Console.WriteLine($"\nGroup {groupNo}  {w}x{h}  ~{avgDur:F2}s  items={cluster.Count}");

                    foreach (var f in cluster.OrderByDescending(x => x.SizeBytes))
                    {
                        Console.WriteLine($"  - {f.Path}");
                        Console.WriteLine($"    dur={f.DurationSec:F2}s  size={(f.SizeBytes / 1024 / 1024)} MiB  codec={f.VideoCodec ?? "?"}");
                    }
                }
            }

            if (groupNo == 0)
                Console.WriteLine("No candidates found. Try a larger tolerance: --tol=0.5");

            break;
        }
    case "dupe-verify":
        {
            // Args:
            // --tol=0.25 duration tolerance in seconds
            // --min=2 minimum items per group
            // --maxdist=10 maximum average hamming distance (0..64)
            // --frames=20,50,80
            double tol = 0.25;
            int minItems = 2;
            double maxAvgDist = 10.0;
            var positions = new List<int> { 20, 50, 80 };

            foreach (var a in remaining.Skip(1))
            {
                if (a.StartsWith("--tol=", StringComparison.OrdinalIgnoreCase) &&
                    double.TryParse(a.Split("=", 2)[1],
                        System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture, out var t))
                    tol = t;

                if (a.StartsWith("--min=", StringComparison.OrdinalIgnoreCase) &&
                    int.TryParse(a.Split("=", 2)[1], out var m))
                    minItems = m;

                if (a.StartsWith("--maxdist=", StringComparison.OrdinalIgnoreCase) &&
                    double.TryParse(a.Split("=", 2)[1],
                        System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture, out var d))
                    maxAvgDist = d;

                if (a.StartsWith("--frames=", StringComparison.OrdinalIgnoreCase))
                {
                    var raw = a.Split("=", 2)[1];
                    var parsed = raw.Split(',')
                        .Select(s => s.Trim())
                        .Where(s => int.TryParse(s, out _))
                        .Select(int.Parse)
                        .Where(p => p > 0 && p < 100)
                        .Distinct()
                        .OrderBy(p => p)
                        .ToList();

                    if (parsed.Count > 0)
                        positions = parsed;
                }
            }

            var tools = new FfmpegTools();
            var extractor = new FfmpegFrameExtractor(tools);
            var hasher = new DHashService();

            var files = await db.ListMediaFilesForCandidatesAsync();

            // Candidate buckets by (width,height,rounded duration)
            var buckets = files
                .GroupBy(f => (f.Width!.Value, f.Height!.Value, DurKey: Math.Round(f.DurationSec!.Value, 1)))
                .Where(g => g.Count() >= minItems)
                .OrderByDescending(g => g.Count())
                .ToList();

            int groupNo = 0;

            foreach (var b in buckets)
            {
                // refine each bucket into subclusters by duration tolerance
                var bucketFiles = b.OrderBy(f => f.DurationSec).ToList();

                while (bucketFiles.Count >= minItems)
                {
                    var seed = bucketFiles[0];
                    var seedDur = seed.DurationSec!.Value;

                    var cluster = bucketFiles
                        .Where(f => Math.Abs(f.DurationSec!.Value - seedDur) <= tol)
                        .ToList();

                    foreach (var c in cluster)
                        bucketFiles.Remove(c);

                    if (cluster.Count < minItems)
                        continue;

                    // Load/compute hashes for each file in the cluster
                    var verified = new List<(AppDb.MediaFileRow File, Dictionary<int, ulong> Hashes)>();

                    foreach (var f in cluster)
                    {
                        if (f.Id == 0) continue;

                        if (f.DurationSec is null || f.DurationSec <= 1.0)
                        {
                            Console.WriteLine($"Skip hashes (no/short duration): {f.Path}");
                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(f.Path) || !File.Exists(f.Path))
                        {
                            Console.WriteLine($"Skip hashes (missing file): {f.Path}");
                            continue;
                        }

                        var map = new Dictionary<int, ulong>();

                        foreach (var pos in positions)
                        {
                            var h = await db.GetFrameHashAsync(f.Id, pos);

                            if (h is null)
                            {
                                try
                                {
                                    var ts = f.DurationSec.Value * (pos / 100.0);
                                    var png = await extractor.ExtractFramePngAsync(f.Path, ts, CancellationToken.None);
                                    var computed = hasher.ComputeDHash64(png);

                                    await db.UpsertFrameHashAsync(f.Id, pos, computed);
                                    h = computed;
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Skip frame {pos}%: {f.Path} :: {ex.Message}");
                                    continue;
                                }
                            }

                            if (h is not null)
                                map[pos] = h.Value;
                        }

                        if (map.Count == 0)
                            continue;

                        verified.Add((f, map));
                    }

                    if (verified.Count < minItems)
                        continue;

                    // Use first verified as seed; compare by average Hamming across shared positions
                    var seedHashes = verified[0].Hashes;

                    double AvgDist(Dictionary<int, ulong> a, Dictionary<int, ulong> c)
                    {
                        var keys = a.Keys.Intersect(c.Keys).ToList();
                        if (keys.Count == 0) return 9999;

                        double sum = 0;
                        foreach (var k in keys)
                            sum += DHashService.HammingDistance(a[k], c[k]);

                        return sum / keys.Count;
                    }

                    var final = verified
                        .Select(v => (v.File, Avg: AvgDist(seedHashes, v.Hashes)))
                        .Where(x => x.Avg <= maxAvgDist)
                        .OrderBy(x => x.Avg)
                        .ToList();

                    if (final.Count < minItems)
                        continue;

                    groupNo++;

                    var w = b.Key.Item1;
                    var hgt = b.Key.Item2;
                    var avgDur = final.Average(x => x.File.DurationSec!.Value);

                    Console.WriteLine($"\nVerified Group {groupNo}  {w}x{hgt}  ~{avgDur:F2}s  items={final.Count}  tol={tol}  maxAvgDist={maxAvgDist}  frames={string.Join(",", positions)}");

                    foreach (var x in final.OrderByDescending(z => z.File.SizeBytes))
                    {
                        Console.WriteLine($"  - avgDist={x.Avg,5:F1}  {x.File.Path}");
                        Console.WriteLine($"    dur={x.File.DurationSec:F2}s  size={(x.File.SizeBytes / 1024 / 1024)} MiB  codec={x.File.VideoCodec ?? "?"}");
                    }
                }
            }

            if (groupNo == 0)
                Console.WriteLine("No verified duplicates found. Try --tol=0.5 or --maxdist=12 or --frames=50");

            break;
        }

    default:
        {
            Console.WriteLine("Unknown command.");
            break;
        }
}
