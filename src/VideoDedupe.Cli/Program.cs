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

    default:
        {
            Console.WriteLine("Unknown command.");
            break;
        }
}
