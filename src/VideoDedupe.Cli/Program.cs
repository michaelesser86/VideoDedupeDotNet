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
        if (remaining.Count < 2) { Console.WriteLine("Missing path"); return; }
        var id = await db.AddScanRootAsync(remaining[1]);
        Console.WriteLine($"Added root id={id} path={remaining[1]}");
        break;

    case "roots-list":
        var roots = await db.ListScanRootsAsync();
        foreach (var r in roots)
            Console.WriteLine($"{r.Id}\t{r.IsEnabled}\t{r.Path}");
        break;

    case "roots-toggle":
        if (remaining.Count < 2 || !long.TryParse(remaining[1], out var rid))
        {
            Console.WriteLine("Missing/invalid id");
            return;
        }
        await db.ToggleScanRootAsync(rid);
        Console.WriteLine($"Toggled root id={rid}");
        break;

    default:
        Console.WriteLine("Unknown command.");
        break;
}
