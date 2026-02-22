// File: src/VideoDedupe.Infrastructure/DupeBuildService.cs
using System.Globalization;

using VideoDedupe.Core;

namespace VideoDedupe.Infrastructure;

public sealed record DupeBuildOptions(
    bool ClearExisting = true,
    double TolSec = 0.25,
    int MinItems = 2,
    double MaxAvgDist = 10.0,
    IReadOnlyList<int>? FramePercents = null
);

public sealed record DupeBuildProgress(
    string Phase,
    int Total,
    int Done,
    string? Current = null
);

public sealed class DupeBuildService
{
    private readonly AppDb _db;
    private readonly FfmpegFrameExtractor _extractor;
    private readonly DHashService _hasher;

    public DupeBuildService(AppDb db, FfmpegFrameExtractor extractor, DHashService hasher)
    {
        _db = db;
        _extractor = extractor;
        _hasher = hasher;
    }

    public async Task<int> RunAsync(
        DupeBuildOptions opt,
        IProgress<DupeBuildProgress>? progress,
        CancellationToken ct)
    {
        await DbInitializer.EnsureCreatedAsync(_db);

        var positions = (opt.FramePercents is { Count: > 0 }
                ? opt.FramePercents
                : new List<int> { 20, 50, 80 })
            .Where(p => p > 0 && p < 100)
            .Distinct()
            .OrderBy(p => p)
            .ToList();

        if (positions.Count == 0)
            positions = new List<int> { 20, 50, 80 };

        // 1) Clear existing groups (optional)
        if (opt.ClearExisting)
        {
            ct.ThrowIfCancellationRequested();
            progress?.Report(new DupeBuildProgress("Clearing existing groups", 1, 0));
            await _db.ClearDuplicateGroupsAsync();
            progress?.Report(new DupeBuildProgress("Clearing existing groups", 1, 1));
        }

        // 2) Load candidates + bucketize
        ct.ThrowIfCancellationRequested();
        progress?.Report(new DupeBuildProgress("Loading candidates", 1, 0));
        var files = await _db.ListMediaFilesForCandidatesAsync();
        progress?.Report(new DupeBuildProgress("Loading candidates", 1, 1));

        // Bucket by resolution + rounded duration (0.1s)
        var buckets = files
            .Where(f => f.Width is not null && f.Height is not null && f.DurationSec is not null)
             .GroupBy(f => (Width: f.Width!.Value, Height: f.Height!.Value, DurKey: Math.Round(f.DurationSec!.Value, 1)))
            .Where(g => g.Count() >= opt.MinItems)
            .OrderByDescending(g => g.Count())
            .ToList();

        int savedGroups = 0;

        // 3) Process each bucket
        for (int bi = 0; bi < buckets.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();

            var b = buckets[bi];
            progress?.Report(new DupeBuildProgress(
                Phase: "Processing buckets",
                Total: buckets.Count,
                Done: bi,
                Current: $"{b.Key.Width}x{b.Key.Height} durKey={b.Key.DurKey} count={b.Count()}"));

            var bucketFiles = b.OrderBy(f => f.DurationSec).ToList();

            while (bucketFiles.Count >= opt.MinItems)
            {
                ct.ThrowIfCancellationRequested();

                var seed = bucketFiles[0];
                var seedDur = seed.DurationSec!.Value;

                var cluster = bucketFiles
                    .Where(f => Math.Abs(f.DurationSec!.Value - seedDur) <= opt.TolSec)
                    .ToList();

                foreach (var c in cluster)
                    bucketFiles.Remove(c);

                if (cluster.Count < opt.MinItems)
                    continue;

                // 4) Compute/Load frame hashes for each file in cluster
                var verified = new List<(AppDb.MediaFileRow File, Dictionary<int, ulong> Hashes)>();

                // progress hint (cluster)
                progress?.Report(new DupeBuildProgress(
                    Phase: "Hashing cluster",
                    Total: cluster.Count,
                    Done: 0,
                    Current: $"{cluster.Count} files"));

                for (int ci = 0; ci < cluster.Count; ci++)
                {
                    ct.ThrowIfCancellationRequested();

                    var f = cluster[ci];

                    progress?.Report(new DupeBuildProgress(
                        Phase: "Hashing cluster",
                        Total: cluster.Count,
                        Done: ci,
                        Current: f.Path));

                    if (f.Id == 0) continue;
                    if (f.DurationSec is null || f.DurationSec <= 1.0) continue;
                    if (string.IsNullOrWhiteSpace(f.Path) || !File.Exists(f.Path)) continue;

                    var map = new Dictionary<int, ulong>();

                    foreach (var pos in positions)
                    {
                        ct.ThrowIfCancellationRequested();

                        var h = await _db.GetFrameHashAsync(f.Id, pos);
                        if (h is null)
                        {
                            try
                            {
                                // clamp timestamp inside the video (stability)
                                var dur = f.DurationSec.Value;
                                var ts = dur * (pos / 100.0);
                                ts = Math.Clamp(ts, 0.1, Math.Max(0.1, dur - 0.1));

                                var png = await _extractor.ExtractFramePngAsync(f.Path, ts, ct);
                                var computed = _hasher.ComputeDHash64(png);
                                await _db.UpsertFrameHashAsync(f.Id, pos, computed);
                                h = computed;
                            }
                            catch
                            {
                                // keep going; missing a frame is ok
                                continue;
                            }
                        }

                        if (h is not null)
                            map[pos] = h.Value;
                    }

                    if (map.Count > 0)
                        verified.Add((f, map));
                }

                if (verified.Count < opt.MinItems)
                    continue;

                // 5) Distance filter relative to seed
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

                progress?.Report(new DupeBuildProgress(
                    Phase: "Verifying cluster",
                    Total: verified.Count,
                    Done: 0));

                var final = verified
                    .Select(v => (v.File, Avg: AvgDist(seedHashes, v.Hashes)))
                    .Where(x => x.Avg <= opt.MaxAvgDist)
                    .OrderBy(x => x.Avg)
                    .ToList();

                if (final.Count < opt.MinItems)
                    continue;

                // 6) Save group + members
                ct.ThrowIfCancellationRequested();

                progress?.Report(new DupeBuildProgress(
                    Phase: "Saving group",
                    Total: final.Count,
                    Done: 0,
                    Current: $"members={final.Count}"));

                var groupId = await _db.InsertDuplicateGroupAsync(
                    algorithm: "dhash",
                    frames: string.Join(",", positions),
                    tolSec: opt.TolSec,
                    maxAvgDist: opt.MaxAvgDist);

                var members = final.Select(x => new AppDb.DuplicateMemberRow
                {
                    GroupId = groupId,
                    MediaFileId = x.File.Id,
                    AvgDist = x.Avg
                });

                await _db.InsertDuplicateMembersAsync(groupId, members);

                savedGroups++;

                progress?.Report(new DupeBuildProgress(
                    Phase: "Saved groups",
                    Total: 0,
                    Done: savedGroups,
                    Current: null));
            }
        }

        progress?.Report(new DupeBuildProgress(
            Phase: "Done",
            Total: 1,
            Done: 1,
            Current: $"Saved duplicate groups: {savedGroups}"));

        return savedGroups;
    }

    public static DupeBuildOptions ParseArgs(IEnumerable<string> args)
    {
        // Mirrors your CLI defaults + flags:
        // --tol=0.25 --min=2 --maxdist=10 --frames=20,50,80 --clear=true
        double tol = 0.25;
        int minItems = 2;
        double maxAvgDist = 10.0;
        var positions = new List<int> { 20, 50, 80 };
        bool clear = true;

        foreach (var a in args)
        {
            if (a.StartsWith("--tol=", StringComparison.OrdinalIgnoreCase) &&
                double.TryParse(a.Split("=", 2)[1], NumberStyles.Any, CultureInfo.InvariantCulture, out var t))
                tol = t;

            if (a.StartsWith("--min=", StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(a.Split("=", 2)[1], out var m))
                minItems = m;

            if (a.StartsWith("--maxdist=", StringComparison.OrdinalIgnoreCase) &&
                double.TryParse(a.Split("=", 2)[1], NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
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

            if (a.StartsWith("--clear=", StringComparison.OrdinalIgnoreCase) &&
                bool.TryParse(a.Split("=", 2)[1], out var c))
                clear = c;
        }

        return new DupeBuildOptions(
            ClearExisting: clear,
            TolSec: tol,
            MinItems: minItems,
            MaxAvgDist: maxAvgDist,
            FramePercents: positions);
    }
}
