// File: src/VideoDedupe.Infrastructure/DupeBuildService.cs
using System.Globalization;

using VideoDedupe.Core;

namespace VideoDedupe.Infrastructure;

public sealed record DupeBuildOptions(
    bool ClearExisting = true,
    double TolSec = 0.25,
    int MinItems = 2,
    double MaxAvgDist = 10.0,
    IReadOnlyList<int>? FramePercents = null,
    int RefinementFrames = 6,
    double SceneThreshold = 0.30,
    double MaxPHashDist = 16.0,
    double BorderlineMargin = 4.0
);

public sealed record DupeBuildProgress(
    string Phase,
    int Total,
    int Done,
    string? Current = null,
    int OverallTotal = 0,
    int OverallDone = 0
);

public sealed class DupeBuildService
{
    private readonly AppDb _db;
    private readonly FfmpegFrameExtractor _extractor;
    private readonly DHashService _hasher;
    private readonly PHashService _pHasher;
    private readonly SceneFrameSelector _sceneSelector;

    public DupeBuildService(AppDb db, FfmpegFrameExtractor extractor, DHashService hasher)
    {
        _db = db;
        _extractor = extractor;
        _hasher = hasher;
        _pHasher = new PHashService();
        _sceneSelector = new SceneFrameSelector(extractor.Tools);
    }

    public async Task<int> RunAsync(
        DupeBuildOptions opt,
        IProgress<DupeBuildProgress>? progress,
        CancellationToken ct)
    {
        if (!double.IsFinite(opt.TolSec) || opt.TolSec < 0)
            throw new ArgumentOutOfRangeException(nameof(opt.TolSec));
        if (opt.MinItems < 2)
            throw new ArgumentOutOfRangeException(nameof(opt.MinItems));
        if (opt.RefinementFrames < 1 || !double.IsFinite(opt.SceneThreshold) || opt.SceneThreshold < 0 ||
            !double.IsFinite(opt.MaxPHashDist) || opt.MaxPHashDist < 0 ||
            !double.IsFinite(opt.BorderlineMargin) || opt.BorderlineMargin < 0)
            throw new ArgumentOutOfRangeException(nameof(opt));

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

        // 2) Load candidates and split only at gaps larger than the tolerance
        ct.ThrowIfCancellationRequested();
        progress?.Report(new DupeBuildProgress("Loading candidates", 1, 0));
        var files = await _db.ListMediaFilesForCandidatesAsync();
        var candidateTotal = files.Count;
        var completedCandidates = new HashSet<long>();
        progress?.Report(new DupeBuildProgress("Loading candidates", 1, 1,
            OverallTotal: candidateTotal, OverallDone: 0));

        var buckets = DurationCandidates.CreateRanges(files, f => f.DurationSec, opt.TolSec, opt.MinItems);

        // Overlapping seed windows visit the same file many times. Keep every result for the
        // whole run so each file is loaded/extracted at most once.
        var frameHashes = await _db.GetAllFrameHashesAsync();
        var hashed = new Dictionary<long, Dictionary<int, ulong>?>();
        var sceneHashes = new Dictionary<long, Dictionary<int, ulong>>();

        int savedGroups = 0;

        // 3) Process each bucket
        for (int bi = 0; bi < buckets.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();

            var b = buckets[bi];
            progress?.Report(new DupeBuildProgress(
                Phase: "Processing duration ranges",
                Total: buckets.Count,
                Done: bi,
                OverallTotal: candidateTotal,
                OverallDone: completedCandidates.Count,
                Current: $"{b[0].DurationSec:F2}–{b[^1].DurationSec:F2}s count={b.Count}"));

            var bucketFiles = b.OrderBy(f => f.DurationSec).ToList();

            while (bucketFiles.Count >= opt.MinItems)
            {
                ct.ThrowIfCancellationRequested();

                var seed = bucketFiles[0];
                var seedDur = seed.DurationSec!.Value;

                var cluster = bucketFiles
                    .Where(f => Math.Abs(f.DurationSec!.Value - seedDur) <= opt.TolSec)
                    .ToList();

                // Unmatched candidates must remain available for the next seed.
                bucketFiles.RemoveAt(0);

                if (cluster.Count < opt.MinItems)
                    continue;

                // 4) Compute/Load frame hashes for each file in cluster
                var verified = new List<(AppDb.MediaFileRow File, Dictionary<int, ulong> Hashes)>();

                // progress hint (cluster)
                progress?.Report(new DupeBuildProgress(
                    Phase: "Hashing cluster",
                    Total: cluster.Count,
                    Done: 0,
                    Current: $"{cluster.Count} files",
                    OverallTotal: candidateTotal,
                    OverallDone: completedCandidates.Count));

                for (int ci = 0; ci < cluster.Count; ci++)
                {
                    ct.ThrowIfCancellationRequested();

                    var f = cluster[ci];

                    progress?.Report(new DupeBuildProgress(
                        Phase: "Hashing cluster",
                        Total: cluster.Count,
                        Done: ci,
                        Current: f.Path,
                        OverallTotal: candidateTotal,
                        OverallDone: completedCandidates.Count));

                    if (f.Id == 0) continue;
                    if (hashed.TryGetValue(f.Id, out var known))
                    {
                        if (known is not null) verified.Add((f, known));
                        continue;
                    }
                    if (f.DurationSec is null || f.DurationSec <= 1.0 ||
                        string.IsNullOrWhiteSpace(f.Path) || !File.Exists(f.Path))
                    {
                        hashed[f.Id] = null;
                        completedCandidates.Add(f.Id);
                        continue;
                    }

                    var map = new Dictionary<int, ulong>();
                    var cachedForFile = frameHashes.GetValueOrDefault(f.Id);

                    foreach (var pos in positions)
                    {
                        ct.ThrowIfCancellationRequested();

                        ulong? h = cachedForFile is not null && cachedForFile.TryGetValue(pos, out var c) ? c : null;
                        if (h is null)
                        {
                            try
                            {
                                // clamp timestamp inside the video (stability)
                                var dur = f.DurationSec.Value;
                                var ts = dur * (pos / 100.0);
                                ts = Math.Clamp(ts, 0.1, Math.Max(0.1, dur - 0.1));

                                var png = await _extractor.ExtractFramePngAsync(f.Path, ts, ct, FfmpegFrameExtractor.HashWidth);
                                var computed = _hasher.ComputeDHash64(png);
                                await _db.UpsertFrameHashAsync(f.Id, pos, computed);
                                h = computed;
                            }
                            catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
                            {
                                System.Diagnostics.Trace.TraceWarning($"Frame {pos}% failed: {f.Path}: {ex}");
                                progress?.Report(new DupeBuildProgress("Frame failed", cluster.Count, ci,
                                    $"{f.Path} ({pos}%): {ex.Message}"));
                                continue;
                            }
                        }

                        if (h is not null)
                            map[pos] = h.Value;
                    }

                    hashed[f.Id] = map.Count > 0 ? map : null;
                    if (map.Count > 0)
                        verified.Add((f, map));
                    completedCandidates.Add(f.Id);
                    progress?.Report(new DupeBuildProgress(
                        Phase: "Hashing cluster",
                        Total: cluster.Count,
                        Done: ci + 1,
                        Current: f.Path,
                        OverallTotal: candidateTotal,
                        OverallDone: completedCandidates.Count));
                }

                // Distances are relative to the seed. If it could not be hashed, the next
                // seed covers the remaining files instead of a random stand-in reference.
                if (verified.Count < opt.MinItems || verified[0].File.Id != seed.Id)
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

                var preliminary = verified
                    .Select(v => (v.File, Avg: AvgDist(seedHashes, v.Hashes)))
                    .Where(x => x.Avg <= opt.MaxAvgDist + opt.BorderlineMargin)
                    .OrderBy(x => x.Avg)
                    .ToList();

                // dHash is cheap and narrows the set.  Only values near its decision
                // boundary are refined with scene-selected pHashes.
                var refined = new Dictionary<long, double>();
                var refinementCandidates = preliminary.Where(x => x.Avg > opt.MaxAvgDist - opt.BorderlineMargin).ToList();
                if (refinementCandidates.Count > 0)
                {
                    var seedSceneHashes = await GetScenePHashesCachedAsync(sceneHashes, verified[0].File, opt, ct);
                    foreach (var candidate in refinementCandidates)
                    {
                        var candidateHashes = await GetScenePHashesCachedAsync(sceneHashes, candidate.File, opt, ct);
                        if (PHashService.SceneSetDistance(seedSceneHashes.Values, candidateHashes.Values) is double pHashDistance)
                            refined[candidate.File.Id] = pHashDistance;
                    }
                }

                var final = preliminary
                    .Where(x => x.Avg <= opt.MaxAvgDist ||
                        (refined.TryGetValue(x.File.Id, out var pHashDistance) && pHashDistance <= opt.MaxPHashDist))
                    .ToList();

                if (final.Count < opt.MinItems)
                    continue;

                foreach (var match in final)
                    bucketFiles.Remove(match.File);

                // 6) Save group + members
                ct.ThrowIfCancellationRequested();

                progress?.Report(new DupeBuildProgress(
                    Phase: "Saving group",
                    Total: final.Count,
                    Done: 0,
                    Current: $"members={final.Count}"));

                var members = final.Select(x => new AppDb.DuplicateMemberRow
                {
                    MediaFileId = x.File.Id,
                    AvgDist = x.Avg
                });

                await _db.InsertDuplicateGroupWithMembersAsync(
                    algorithm: "dhash+scene-phash",
                    frames: $"dhash={string.Join(",", positions)};scene-phash={opt.RefinementFrames}",
                    tolSec: opt.TolSec,
                    maxAvgDist: opt.MaxAvgDist,
                    members: members,
                    ct: ct);

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
            Current: $"Saved duplicate groups: {savedGroups}",
            OverallTotal: candidateTotal,
            OverallDone: candidateTotal));

        return savedGroups;
    }

    // Incomplete results are kept too: re-running scene detection within the same run
    // would fail the same way and decode the whole video again.
    private async Task<Dictionary<int, ulong>> GetScenePHashesCachedAsync(Dictionary<long, Dictionary<int, ulong>> runCache,
        AppDb.MediaFileRow file, DupeBuildOptions opt, CancellationToken ct)
    {
        if (!runCache.TryGetValue(file.Id, out var hashes))
            runCache[file.Id] = hashes = await GetScenePHashesAsync(file, opt, ct);
        return hashes;
    }

    private async Task<Dictionary<int, ulong>> GetScenePHashesAsync(AppDb.MediaFileRow file, DupeBuildOptions opt,
        CancellationToken ct)
    {
        var strategy = SceneStrategyKey(opt);
        var cached = await _db.GetVisualHashesAsync(file.Id, strategy);
        if (cached.Count >= opt.RefinementFrames)
            return cached;

        var timestamps = await _sceneSelector.SelectAsync(file.Path, file.DurationSec!.Value,
            opt.RefinementFrames, opt.SceneThreshold, ct);
        for (var slot = 0; slot < timestamps.Count; slot++)
        {
            if (cached.ContainsKey(slot)) continue;
            try
            {
                var png = await _extractor.ExtractFramePngAsync(file.Path, timestamps[slot], ct, FfmpegFrameExtractor.HashWidth);
                var hash = _pHasher.ComputePHash64(png);
                await _db.UpsertVisualHashAsync(file.Id, strategy, slot, hash);
                cached[slot] = hash;
            }
            catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
            {
                System.Diagnostics.Trace.TraceWarning($"Scene frame {slot} failed: {file.Path}: {ex.Message}");
            }
        }
        return cached;
    }

    // Cached scene hashes are only valid for the settings that selected their frames.
    internal static string SceneStrategyKey(DupeBuildOptions opt) =>
        string.Create(CultureInfo.InvariantCulture,
            $"scene-phash-v2;n={opt.RefinementFrames};t={opt.SceneThreshold:0.###}");

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
