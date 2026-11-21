using System.Collections.Concurrent;
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
    double BorderlineMargin = 4.0,
    int MaxParallelism = 0
)
{
    public static readonly IReadOnlyList<int> DefaultFramePercents = [20, 50, 80];

    /// <summary>Concurrent ffmpeg processes; 0 picks a value from the CPU count.</summary>
    public int EffectiveParallelism => MaxParallelism > 0
        ? MaxParallelism
        : Math.Clamp(Environment.ProcessorCount / 2, 2, 8);
}

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
    private const string Algorithm = "dhash+scene-phash";

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
        Validate(opt);
        await DbInitializer.EnsureCreatedAsync(_db);
        var positions = NormalizePositions(opt.FramePercents);

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
        progress?.Report(new DupeBuildProgress("Loading candidates", 1, 1,
            OverallTotal: candidateTotal, OverallDone: 0));

        var buckets = DurationCandidates.CreateRanges(files, f => f.DurationSec, opt.TolSec, opt.MinItems)
            .Select(b => b.OrderBy(f => f.DurationSec).ToList())
            .ToList();

        // 3) Hash every file that can be part of a group, in parallel. ffmpeg dominates the
        //    run time and a single frame extraction barely uses one core, so running several
        //    at once scales almost linearly. The clustering below is unchanged and only reads
        //    these results, so the groups are the same as with sequential hashing.
        var needed = buckets.SelectMany(b => FilesInQualifyingWindows(b, opt.TolSec, opt.MinItems)).ToList();
        var hashed = await HashAllAsync(needed, positions, opt, candidateTotal - needed.Count,
            candidateTotal, progress, ct);

        // 4) Cluster
        var sceneHashes = new ConcurrentDictionary<long, Lazy<Task<Dictionary<int, ulong>>>>();
        int savedGroups = 0;

        for (int bi = 0; bi < buckets.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();

            var bucketFiles = buckets[bi];
            progress?.Report(new DupeBuildProgress(
                Phase: "Comparing duration ranges",
                Total: buckets.Count,
                Done: bi,
                OverallTotal: candidateTotal,
                OverallDone: candidateTotal,
                Current: $"{bucketFiles[0].DurationSec:F2}–{bucketFiles[^1].DurationSec:F2}s count={bucketFiles.Count}"));

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

                var verified = cluster
                    .Where(f => hashed.TryGetValue(f.Id, out var h) && h is not null)
                    .Select(f => (File: f, Hashes: hashed[f.Id]!))
                    .ToList();

                // Distances are relative to the seed. If it could not be hashed, the next
                // seed covers the remaining files instead of a random stand-in reference.
                if (verified.Count < opt.MinItems || verified[0].File.Id != seed.Id)
                    continue;

                // 5) Distance filter relative to seed
                var seedHashes = verified[0].Hashes;
                var preliminary = verified
                    .Select(v => (v.File, Avg: AverageDistance(seedHashes, v.Hashes)))
                    .Where(x => x.Avg <= opt.MaxAvgDist + opt.BorderlineMargin)
                    .OrderBy(x => x.Avg)
                    .ToList();

                // dHash is cheap and narrows the set. Only values near its decision
                // boundary are refined with scene-selected pHashes.
                var refined = new Dictionary<long, double>();
                var refinementCandidates = preliminary.Where(x => x.Avg > opt.MaxAvgDist - opt.BorderlineMargin).ToList();
                if (refinementCandidates.Count > 0)
                {
                    progress?.Report(new DupeBuildProgress("Refining borderline matches", refinementCandidates.Count, 0,
                        seed.Path, candidateTotal, candidateTotal));
                    var involved = refinementCandidates.Select(c => c.File).Prepend(seed).DistinctBy(f => f.Id).ToList();
                    var sceneResults = await ParallelMapAsync(involved, opt.EffectiveParallelism,
                        f => GetScenePHashesCachedAsync(sceneHashes, f, opt, ct), ct);
                    var seedSceneHashes = sceneResults[seed.Id];
                    foreach (var candidate in refinementCandidates)
                    {
                        if (PHashService.SceneSetDistance(seedSceneHashes.Values, sceneResults[candidate.File.Id].Values)
                            is double pHashDistance)
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
                await _db.InsertDuplicateGroupWithMembersAsync(
                    algorithm: Algorithm,
                    frames: $"dhash={string.Join(",", positions)};scene-phash={opt.RefinementFrames}",
                    tolSec: opt.TolSec,
                    maxAvgDist: opt.MaxAvgDist,
                    members: final.Select(x => new AppDb.DuplicateMemberRow { MediaFileId = x.File.Id, AvgDist = x.Avg }),
                    ct: ct);

                savedGroups++;
                progress?.Report(new DupeBuildProgress("Saved groups", 0, savedGroups,
                    OverallTotal: candidateTotal, OverallDone: candidateTotal));
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

    private static void Validate(DupeBuildOptions opt)
    {
        if (!double.IsFinite(opt.TolSec) || opt.TolSec < 0)
            throw new ArgumentOutOfRangeException(nameof(opt.TolSec));
        if (opt.MinItems < 2)
            throw new ArgumentOutOfRangeException(nameof(opt.MinItems));
        if (opt.RefinementFrames < 1 || !double.IsFinite(opt.SceneThreshold) || opt.SceneThreshold < 0 ||
            !double.IsFinite(opt.MaxPHashDist) || opt.MaxPHashDist < 0 ||
            !double.IsFinite(opt.BorderlineMargin) || opt.BorderlineMargin < 0 || opt.MaxParallelism < 0)
            throw new ArgumentOutOfRangeException(nameof(opt));
    }

    public static List<int> NormalizePositions(IEnumerable<int>? framePercents)
    {
        var positions = (framePercents ?? []).Where(p => p > 0 && p < 100).Distinct().Order().ToList();
        return positions.Count > 0 ? positions : DupeBuildOptions.DefaultFramePercents.ToList();
    }

    /// <summary>
    /// Files that fall into at least one seed window holding <paramref name="minItems"/> files.
    /// Clustering only ever looks at such windows (over a shrinking subset), so no other file
    /// can end up in a group and hashing it would be wasted work.
    /// </summary>
    internal static IEnumerable<AppDb.MediaFileRow> FilesInQualifyingWindows(
        IReadOnlyList<AppDb.MediaFileRow> sortedBucket, double tolerance, int minItems)
    {
        var include = new bool[sortedBucket.Count];
        var end = 0;
        for (var start = 0; start < sortedBucket.Count; start++)
        {
            end = Math.Max(end, start);
            while (end + 1 < sortedBucket.Count &&
                   sortedBucket[end + 1].DurationSec!.Value - sortedBucket[start].DurationSec!.Value <= tolerance)
                end++;
            if (end - start + 1 >= minItems)
                for (var i = start; i <= end; i++) include[i] = true;
        }
        return sortedBucket.Where((_, i) => include[i]);
    }

    private async Task<Dictionary<long, Dictionary<int, ulong>?>> HashAllAsync(
        IReadOnlyList<AppDb.MediaFileRow> files, IReadOnlyList<int> positions, DupeBuildOptions opt,
        int alreadyDone, int overallTotal, IProgress<DupeBuildProgress>? progress, CancellationToken ct)
    {
        var cached = await _db.GetAllFrameHashesAsync();
        var done = 0;
        progress?.Report(new DupeBuildProgress("Hashing frames", files.Count, 0,
            OverallTotal: overallTotal, OverallDone: alreadyDone));

        var results = await ParallelMapAsync(files, opt.EffectiveParallelism, async f =>
        {
            var hashes = await HashFileAsync(f, positions, cached.GetValueOrDefault(f.Id), ct);
            var n = Interlocked.Increment(ref done);
            progress?.Report(new DupeBuildProgress("Hashing frames", files.Count, n, f.Path,
                overallTotal, alreadyDone + n));
            return hashes;
        }, ct);
        return results.ToDictionary(r => r.Key, r => r.Value);
    }

    private async Task<Dictionary<int, ulong>?> HashFileAsync(AppDb.MediaFileRow f,
        IReadOnlyList<int> positions, Dictionary<int, ulong>? cached, CancellationToken ct)
    {
        if (f.DurationSec is not double dur || dur <= 1.0 || string.IsNullOrWhiteSpace(f.Path) || !File.Exists(f.Path))
            return null;

        var map = new Dictionary<int, ulong>();
        var computed = new Dictionary<int, ulong>();
        foreach (var pos in positions)
            if (cached is not null && cached.TryGetValue(pos, out var known))
                map[pos] = known;

        var missing = positions.Where(p => !map.ContainsKey(p)).ToList();
        if (missing.Count > 0)
        {
            // clamp timestamps inside the video (stability)
            var timestamps = missing.Select(pos => Math.Clamp(dur * (pos / 100.0), 0.1, Math.Max(0.1, dur - 0.1))).ToList();
            var frames = await _extractor.ExtractGrayFramesAsync(f.Path, timestamps, ct);
            for (var i = 0; i < missing.Count; i++)
                if (frames[i] is { } frame)
                    map[missing[i]] = computed[missing[i]] = _hasher.ComputeDHash64(frame);
        }

        if (computed.Count > 0)
            await _db.UpsertFrameHashesAsync(f.Id, computed);
        return map.Count > 0 ? map : null;
    }

    private static double AverageDistance(Dictionary<int, ulong> a, Dictionary<int, ulong> b)
    {
        double sum = 0;
        var shared = 0;
        foreach (var (position, hash) in a)
        {
            if (!b.TryGetValue(position, out var other)) continue;
            sum += DHashService.HammingDistance(hash, other);
            shared++;
        }
        return shared == 0 ? double.PositiveInfinity : sum / shared;
    }

    /// <summary>Runs <paramref name="map"/> for every file with bounded concurrency.</summary>
    private static async Task<Dictionary<long, T>> ParallelMapAsync<T>(IReadOnlyList<AppDb.MediaFileRow> files,
        int parallelism, Func<AppDb.MediaFileRow, Task<T>> map, CancellationToken ct)
    {
        var results = new ConcurrentDictionary<long, T>();
        await Parallel.ForEachAsync(files,
            new ParallelOptions { MaxDegreeOfParallelism = parallelism, CancellationToken = ct },
            async (f, _) => results[f.Id] = await map(f));
        return new Dictionary<long, T>(results);
    }

    // Each file's scene hashes are computed once per run, even when several seeds ask for it
    // concurrently. Incomplete results are kept too: re-running scene detection would fail
    // the same way and decode the whole video again.
    private Task<Dictionary<int, ulong>> GetScenePHashesCachedAsync(
        ConcurrentDictionary<long, Lazy<Task<Dictionary<int, ulong>>>> runCache,
        AppDb.MediaFileRow file, DupeBuildOptions opt, CancellationToken ct) =>
        runCache.GetOrAdd(file.Id, _ => new Lazy<Task<Dictionary<int, ulong>>>(
            () => GetScenePHashesAsync(file, opt, ct))).Value;

    private async Task<Dictionary<int, ulong>> GetScenePHashesAsync(AppDb.MediaFileRow file, DupeBuildOptions opt,
        CancellationToken ct)
    {
        var strategy = SceneStrategyKey(opt);
        var cached = await _db.GetVisualHashesAsync(file.Id, strategy);
        if (cached.Count >= opt.RefinementFrames)
            return cached;

        var timestamps = await _sceneSelector.SelectAsync(file.Path, file.DurationSec!.Value,
            opt.RefinementFrames, opt.SceneThreshold, ct);
        var slots = Enumerable.Range(0, timestamps.Count).Where(slot => !cached.ContainsKey(slot)).ToList();
        var frames = await _extractor.ExtractGrayFramesAsync(file.Path, slots.Select(s => timestamps[s]).ToList(), ct);
        for (var i = 0; i < slots.Count; i++)
        {
            if (frames[i] is not { } frame) continue;
            var hash = _pHasher.ComputePHash64(frame);
            await _db.UpsertVisualHashAsync(file.Id, strategy, slots[i], hash);
            cached[slots[i]] = hash;
        }
        return cached;
    }

    // Cached scene hashes are only valid for the settings that selected their frames.
    internal static string SceneStrategyKey(DupeBuildOptions opt) =>
        string.Create(CultureInfo.InvariantCulture,
            $"scene-phash-v2;n={opt.RefinementFrames};t={opt.SceneThreshold:0.###}");

    /// <summary>
    /// Parses <c>--name=value</c> options; unknown or malformed values keep their defaults.
    /// <c>--tol --min --maxdist --frames --clear --refine-frames --scene-threshold
    /// --max-phash --margin --parallel</c>
    /// </summary>
    public static DupeBuildOptions ParseArgs(IEnumerable<string> args)
    {
        var opt = new DupeBuildOptions();
        foreach (var a in args)
        {
            var parts = a.Split('=', 2);
            if (parts.Length != 2 || !parts[0].StartsWith("--", StringComparison.Ordinal))
                continue;
            var value = parts[1];
            bool Double(out double d) => double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out d);
            bool Int(out int i) => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out i);

            opt = parts[0].ToLowerInvariant() switch
            {
                "--tol" when Double(out var d) => opt with { TolSec = d },
                "--min" when Int(out var i) => opt with { MinItems = i },
                "--maxdist" when Double(out var d) => opt with { MaxAvgDist = d },
                "--frames" => opt with
                {
                    FramePercents = NormalizePositions(value.Split(',')
                        .Select(s => int.TryParse(s.Trim(), out var p) ? p : -1))
                },
                "--clear" when bool.TryParse(value, out var b) => opt with { ClearExisting = b },
                "--refine-frames" when Int(out var i) => opt with { RefinementFrames = i },
                "--scene-threshold" when Double(out var d) => opt with { SceneThreshold = d },
                "--max-phash" when Double(out var d) => opt with { MaxPHashDist = d },
                "--margin" when Double(out var d) => opt with { BorderlineMargin = d },
                "--parallel" when Int(out var i) => opt with { MaxParallelism = i },
                _ => opt
            };
        }
        return opt;
    }
}
