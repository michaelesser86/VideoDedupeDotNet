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
    int MaxParallelism = 0,
    bool UseAudio = true,
    double MaxAudioBer = 0.25,
    bool MatchMirrored = true,
    bool FindTrimmed = false
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
    private const string Algorithm = "dhash+scene-phash+audio";

    private readonly AppDb _db;
    private readonly FfmpegFrameExtractor _extractor;
    private readonly DHashService _hasher;
    private readonly PHashService _pHasher;
    private readonly SceneFrameSelector _sceneSelector;
    private readonly AudioPrintExtractor _audio;

    public DupeBuildService(AppDb db, FfmpegFrameExtractor extractor, DHashService hasher)
    {
        _db = db;
        _extractor = extractor;
        _hasher = hasher;
        _pHasher = new PHashService();
        _sceneSelector = new SceneFrameSelector(extractor.Tools);
        _audio = new AudioPrintExtractor(extractor.Tools);
    }

    /// <summary>One file's distance to a group's reference file.</summary>
    internal sealed record Match(AppDb.MediaFileRow File, double Avg, bool Mirrored)
    {
        public double? PHashDist { get; set; }
        public double? AudioBer { get; set; }
        public string? TrimKind { get; set; }
        public double? TrimDiffSec { get; set; }
    }

    /// <summary>A group found in this run, saved after all phases are done.</summary>
    internal sealed record PendingGroup(long ReferenceId, List<Match> Members);

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
        var notDuplicates = await _db.GetNotDuplicatePairsAsync();
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
        //    Shortened copies have no partner of the same length, so that search needs every file.
        var needed = opt.FindTrimmed
            ? files.Where(f => f.DurationSec > 1.0).ToList()
            : buckets.SelectMany(b => FilesInQualifyingWindows(b, opt.TolSec, opt.MinItems)).ToList();
        var hashed = await HashAllAsync(needed, positions, opt, candidateTotal - needed.Count,
            candidateTotal, progress, ct);

        // 4) Cluster
        var sceneHashes = new ConcurrentDictionary<(long, bool), Lazy<Task<Dictionary<int, ulong>>>>();
        var audioPrints = new ConcurrentDictionary<long, Lazy<Task<AudioFingerprint?>>>();
        var groups = new List<PendingGroup>();
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
                    .Select(v => Compare(seedHashes, v.Hashes, v.File, opt.MatchMirrored))
                    .Where(x => x.Avg <= opt.MaxAvgDist + opt.BorderlineMargin)
                    .OrderBy(x => x.Avg)
                    .ToList();

                // dHash is cheap and narrows the set. Only values near its decision
                // boundary are refined with scene-selected pHashes, and what that does not
                // confirm with a comparison of the soundtracks.
                var refinementCandidates = preliminary.Where(x => x.Avg > opt.MaxAvgDist - opt.BorderlineMargin).ToList();
                if (refinementCandidates.Count > 0)
                {
                    progress?.Report(new DupeBuildProgress("Refining borderline matches", refinementCandidates.Count, 0,
                        seed.Path, candidateTotal, candidateTotal));
                    await RefineWithScenesAsync(seed, refinementCandidates, sceneHashes, opt, ct);

                    var unconfirmed = refinementCandidates.Where(c => c.Avg > opt.MaxAvgDist && !ConfirmedByScenes(c, opt)).ToList();
                    if (opt.UseAudio && unconfirmed.Count > 0)
                        await RefineWithAudioAsync(seed, unconfirmed, audioPrints, opt, ct);
                }

                var accepted = preliminary
                    .Where(x => x.Avg <= opt.MaxAvgDist || ConfirmedByScenes(x, opt) ||
                        x.AudioBer <= opt.MaxAudioBer)
                    .ToList();
                var final = ApplyNotDuplicate(seed.Id, accepted, notDuplicates);

                if (final.Count < opt.MinItems)
                    continue;

                foreach (var match in final)
                    bucketFiles.Remove(match.File);
                groups.Add(new PendingGroup(seed.Id, final));
            }
        }

        // 5) Shortened copies (optional): pairs of different length that share their start or end.
        if (opt.FindTrimmed)
            await AddTrimmedCopiesAsync(files, hashed, positions, groups, notDuplicates, opt,
                candidateTotal, progress, ct);

        // 6) Save groups + members
        foreach (var group in groups)
        {
            ct.ThrowIfCancellationRequested();
            await _db.InsertDuplicateGroupWithMembersAsync(
                algorithm: Algorithm,
                frames: $"dhash={string.Join(",", positions)};scene-phash={opt.RefinementFrames}",
                tolSec: opt.TolSec,
                maxAvgDist: opt.MaxAvgDist,
                members: group.Members.Select(x => new AppDb.DuplicateMemberRow
                {
                    MediaFileId = x.File.Id,
                    AvgDist = x.Avg,
                    Mirrored = x.Mirrored,
                    PHashDist = x.PHashDist,
                    AudioBer = x.AudioBer,
                    IsReference = x.File.Id == group.ReferenceId,
                    TrimKind = x.TrimKind,
                    TrimDiffSec = x.TrimDiffSec
                }),
                ct: ct);

            savedGroups++;
            progress?.Report(new DupeBuildProgress("Saved groups", 0, savedGroups,
                OverallTotal: candidateTotal, OverallDone: candidateTotal));
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

    /// <summary>Timestamp of a hash key: a percent position, or one of the trim anchors.</summary>
    internal static double TimeOf(int key, double durationSec) => key switch
    {
        TrimMatcher.StartAnchorKey => TrimMatcher.AnchorSec,
        TrimMatcher.EndAnchorKey => durationSec - TrimMatcher.AnchorSec,
        // clamp timestamps inside the video (stability)
        _ => Math.Clamp(durationSec * (key / 100.0), 0.1, Math.Max(0.1, durationSec - 0.1))
    };

    private async Task AddTrimmedCopiesAsync(IReadOnlyList<AppDb.MediaFileRow> files,
        Dictionary<long, Dictionary<int, FrameHash>?> hashed, IReadOnlyList<int> positions, List<PendingGroup> groups,
        IReadOnlySet<(long A, long B)> notDuplicates, DupeBuildOptions opt, int candidateTotal,
        IProgress<DupeBuildProgress>? progress, CancellationToken ct)
    {
        progress?.Report(new DupeBuildProgress("Searching shortened copies", 1, 0,
            OverallTotal: candidateTotal, OverallDone: candidateTotal));
        var anchored = files
            .Where(f => hashed.GetValueOrDefault(f.Id) is { } h &&
                        h.ContainsKey(TrimMatcher.StartAnchorKey) && h.ContainsKey(TrimMatcher.EndAnchorKey))
            .Select(f => (File: f, Hashes: hashed[f.Id]!))
            .ToList();
        var candidates = TrimMatcher.FindCandidates(anchored, opt.TolSec, opt.MaxAvgDist, opt.MatchMirrored)
            .Where(c => !notDuplicates.Contains(PairKey(c.Longer.Id, c.Shorter.Id)))
            .ToList();

        // Shared openings or credits also match at the anchors, so every candidate is checked
        // where both copies must show the same picture: at the shorter file's own positions.
        var done = 0;
        var verified = new ConcurrentBag<(TrimMatcher.Candidate Candidate, double Avg)>();
        await Parallel.ForEachAsync(candidates,
            new ParallelOptions { MaxDegreeOfParallelism = opt.EffectiveParallelism, CancellationToken = ct },
            async (c, token) =>
            {
                if (await VerifyTrimAsync(c, hashed[c.Shorter.Id]!, positions, token) is double avg && avg <= opt.MaxAvgDist)
                    verified.Add((c, avg));
                progress?.Report(new DupeBuildProgress("Checking shortened copies", candidates.Count,
                    Interlocked.Increment(ref done), c.Shorter.Path, candidateTotal, candidateTotal));
            });

        var groupOf = new Dictionary<long, PendingGroup>();
        foreach (var g in groups)
            foreach (var m in g.Members)
                groupOf[m.File.Id] = g;

        foreach (var (c, avg) in verified.OrderBy(v => v.Avg).ThenBy(v => v.Candidate.Longer.Id).ThenBy(v => v.Candidate.Shorter.Id))
        {
            var longerGroup = groupOf.GetValueOrDefault(c.Longer.Id);
            var shorterGroup = groupOf.GetValueOrDefault(c.Shorter.Id);
            if (longerGroup is not null && shorterGroup is not null)
                continue; // both already grouped; merging groups could chain unrelated files

            if (longerGroup is null && shorterGroup is null)
            {
                if (opt.MinItems > 2) continue;
                var group = new PendingGroup(c.Longer.Id,
                [
                    new Match(c.Longer, 0, false),
                    new Match(c.Shorter, avg, c.Mirrored)
                    {
                        TrimKind = c.Kind,
                        TrimDiffSec = c.Shorter.DurationSec - c.Longer.DurationSec
                    }
                ]);
                groups.Add(group);
                groupOf[c.Longer.Id] = groupOf[c.Shorter.Id] = group;
                continue;
            }

            // One of them is already grouped: the other joins that group.
            var target = (longerGroup ?? shorterGroup)!;
            var (newcomer, partner) = longerGroup is null ? (c.Longer, c.Shorter) : (c.Shorter, c.Longer);
            if (target.Members.Any(m => notDuplicates.Contains(PairKey(m.File.Id, newcomer.Id))))
                continue;
            var partnerMatch = target.Members.First(m => m.File.Id == partner.Id);
            target.Members.Add(new Match(newcomer, avg, partnerMatch.Mirrored ^ c.Mirrored)
            {
                TrimKind = c.Kind,
                TrimDiffSec = newcomer.DurationSec - partner.DurationSec
            });
            groupOf[newcomer.Id] = target;
        }
    }

    /// <summary>
    /// Average distance between the shorter file's frames and the frames of the longer file at
    /// the same moments of the shared footage, or null when too few frames could be compared.
    /// </summary>
    private async Task<double?> VerifyTrimAsync(TrimMatcher.Candidate c, Dictionary<int, FrameHash> shorterHashes,
        IReadOnlyList<int> positions, CancellationToken ct)
    {
        var shorterDur = c.Shorter.DurationSec!.Value;
        var offset = c.Kind == AppDb.TrimKinds.SameStart ? 0 : c.Longer.DurationSec!.Value - shorterDur;
        var times = positions
            .Where(shorterHashes.ContainsKey)
            .Select(p => (Position: p, TimeMs: (long)Math.Round((TimeOf(p, shorterDur) + offset) * 1000)))
            .ToList();

        var cached = await _db.GetTimedFrameHashesAsync(c.Longer.Id);
        var missing = times.Where(t => !cached.ContainsKey(t.TimeMs)).Select(t => t.TimeMs).Distinct().ToList();
        if (missing.Count > 0 && File.Exists(c.Longer.Path))
        {
            var frames = await _extractor.ExtractGrayFramesAsync(c.Longer.Path, missing.Select(ms => ms / 1000.0).ToList(), ct);
            var computed = new Dictionary<long, FrameHash>();
            for (var i = 0; i < missing.Count; i++)
                if (frames[i] is { } frame)
                    cached[missing[i]] = computed[missing[i]] = _hasher.ComputeHashes(frame);
            if (computed.Count > 0)
                await _db.UpsertTimedFrameHashesAsync(c.Longer.Id, computed);
        }

        var distances = times
            .Where(t => cached.ContainsKey(t.TimeMs))
            .Select(t => (double)DHashService.HammingDistance(cached[t.TimeMs].Normal,
                c.Mirrored ? shorterHashes[t.Position].Mirrored : shorterHashes[t.Position].Normal))
            .ToList();
        return distances.Count >= 2 ? distances.Average() : null;
    }

    private static bool ConfirmedByScenes(Match m, DupeBuildOptions opt) => m.PHashDist <= opt.MaxPHashDist;

    /// <summary>
    /// Distance of <paramref name="file"/> to the reference, as is or as a mirror image,
    /// whichever is closer.
    /// </summary>
    internal static Match Compare(Dictionary<int, FrameHash> reference, Dictionary<int, FrameHash> other,
        AppDb.MediaFileRow file, bool matchMirrored)
    {
        var normal = AverageDistance(reference, other, mirrored: false);
        if (!matchMirrored)
            return new Match(file, normal, false);
        var mirrored = AverageDistance(reference, other, mirrored: true);
        return mirrored < normal ? new Match(file, mirrored, true) : new Match(file, normal, false);
    }

    /// <summary>
    /// Drops files the user marked as different from the seed or from a closer member. The seed
    /// always stays; the others are taken by increasing distance.
    /// </summary>
    internal static List<Match> ApplyNotDuplicate(long seedId, IEnumerable<Match> matches,
        IReadOnlySet<(long A, long B)> notDuplicates)
    {
        var result = new List<Match>();
        foreach (var m in matches.OrderBy(m => m.File.Id != seedId).ThenBy(m => m.Avg))
        {
            if (result.Count == 0 && m.File.Id != seedId)
                return [];
            if (result.All(r => !notDuplicates.Contains(PairKey(r.File.Id, m.File.Id))))
                result.Add(m);
        }
        return result;
    }

    internal static (long, long) PairKey(long a, long b) => a < b ? (a, b) : (b, a);

    private async Task RefineWithScenesAsync(AppDb.MediaFileRow seed, IReadOnlyList<Match> candidates,
        ConcurrentDictionary<(long, bool), Lazy<Task<Dictionary<int, ulong>>>> cache, DupeBuildOptions opt,
        CancellationToken ct)
    {
        // A mirrored candidate is compared with mirror-image scene hashes; the seed stays as is.
        var requests = candidates.Select(c => (c.File, c.Mirrored)).Prepend((seed, false))
            .DistinctBy(r => (r.Item1.Id, r.Item2)).ToList();
        var results = new ConcurrentDictionary<(long, bool), Dictionary<int, ulong>>();
        await Parallel.ForEachAsync(requests,
            new ParallelOptions { MaxDegreeOfParallelism = opt.EffectiveParallelism, CancellationToken = ct },
            async (r, _) => results[(r.Item1.Id, r.Item2)] = await cache.GetOrAdd((r.Item1.Id, r.Item2),
                _ => new Lazy<Task<Dictionary<int, ulong>>>(() => GetScenePHashesAsync(r.Item1, r.Item2, opt, ct))).Value);

        var seedScenes = results[(seed.Id, false)];
        foreach (var candidate in candidates)
            candidate.PHashDist = PHashService.SceneSetDistance(seedScenes.Values,
                results[(candidate.File.Id, candidate.Mirrored)].Values);
    }

    private async Task RefineWithAudioAsync(AppDb.MediaFileRow seed, IReadOnlyList<Match> candidates,
        ConcurrentDictionary<long, Lazy<Task<AudioFingerprint?>>> cache, DupeBuildOptions opt, CancellationToken ct)
    {
        var files = candidates.Select(c => c.File).Prepend(seed).DistinctBy(f => f.Id).ToList();
        var prints = await ParallelMapAsync(files, opt.EffectiveParallelism,
            f => cache.GetOrAdd(f.Id, _ => new Lazy<Task<AudioFingerprint?>>(() => GetAudioPrintAsync(f, ct))).Value, ct);
        if (prints[seed.Id] is not { IsEmpty: false } seedPrint)
            return;

        // Durations differ by at most the tolerance, so the windows around the middle line up
        // within half of it; a few seconds of slack also cover differently trimmed silence.
        var maxOffset = (int)Math.Ceiling((opt.TolSec / 2 + 3) / AudioFingerprint.SecondsPerWord);
        var minOverlap = (int)(10 / AudioFingerprint.SecondsPerWord);
        foreach (var candidate in candidates)
            if (prints[candidate.File.Id] is { IsEmpty: false } print)
                candidate.AudioBer = AudioFingerprint.BitErrorRate(seedPrint, print, maxOffset, minOverlap);
    }

    private static void Validate(DupeBuildOptions opt)
    {
        if (!double.IsFinite(opt.TolSec) || opt.TolSec < 0)
            throw new ArgumentOutOfRangeException(nameof(opt.TolSec));
        if (opt.MinItems < 2)
            throw new ArgumentOutOfRangeException(nameof(opt.MinItems));
        if (opt.RefinementFrames < 1 || !double.IsFinite(opt.SceneThreshold) || opt.SceneThreshold < 0 ||
            !double.IsFinite(opt.MaxPHashDist) || opt.MaxPHashDist < 0 ||
            !double.IsFinite(opt.BorderlineMargin) || opt.BorderlineMargin < 0 || opt.MaxParallelism < 0 ||
            !double.IsFinite(opt.MaxAudioBer) || opt.MaxAudioBer is < 0 or > 1)
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

    private async Task<Dictionary<long, Dictionary<int, FrameHash>?>> HashAllAsync(
        IReadOnlyList<AppDb.MediaFileRow> files, IReadOnlyList<int> positions, DupeBuildOptions opt,
        int alreadyDone, int overallTotal, IProgress<DupeBuildProgress>? progress, CancellationToken ct)
    {
        var cached = await _db.GetAllFrameHashesAsync();
        var done = 0;
        progress?.Report(new DupeBuildProgress("Hashing frames", files.Count, 0,
            OverallTotal: overallTotal, OverallDone: alreadyDone));

        var results = await ParallelMapAsync(files, opt.EffectiveParallelism, async f =>
        {
            var hashes = await HashFileAsync(f, positions, opt.FindTrimmed, cached.GetValueOrDefault(f.Id), ct);
            var n = Interlocked.Increment(ref done);
            progress?.Report(new DupeBuildProgress("Hashing frames", files.Count, n, f.Path,
                overallTotal, alreadyDone + n));
            return hashes;
        }, ct);
        return results.ToDictionary(r => r.Key, r => r.Value);
    }

    private async Task<Dictionary<int, FrameHash>?> HashFileAsync(AppDb.MediaFileRow f,
        IReadOnlyList<int> positions, bool withAnchors, Dictionary<int, FrameHash>? cached, CancellationToken ct)
    {
        if (f.DurationSec is not double dur || dur <= 1.0 || string.IsNullOrWhiteSpace(f.Path) || !File.Exists(f.Path))
            return null;

        // Keys are percent positions; the negative anchor keys are fixed times from the start
        // and from the end, which a shortened copy shares with its original.
        var keys = positions.ToList();
        if (withAnchors && dur >= TrimMatcher.MinDurationSec)
            keys.AddRange([TrimMatcher.StartAnchorKey, TrimMatcher.EndAnchorKey]);

        var map = new Dictionary<int, FrameHash>();
        var computed = new Dictionary<int, FrameHash>();
        foreach (var key in keys)
            if (cached is not null && cached.TryGetValue(key, out var known))
                map[key] = known;

        var missing = keys.Where(p => !map.ContainsKey(p)).ToList();
        if (missing.Count > 0)
        {
            var timestamps = missing.Select(key => TimeOf(key, dur)).ToList();
            var frames = await _extractor.ExtractGrayFramesAsync(f.Path, timestamps, ct);
            for (var i = 0; i < missing.Count; i++)
                if (frames[i] is { } frame)
                    map[missing[i]] = computed[missing[i]] = _hasher.ComputeHashes(frame);
        }

        if (computed.Count > 0)
            await _db.UpsertFrameHashesAsync(f.Id, computed);
        return map.Count > 0 ? map : null;
    }

    private static double AverageDistance(Dictionary<int, FrameHash> reference, Dictionary<int, FrameHash> other,
        bool mirrored)
    {
        double sum = 0;
        var shared = 0;
        foreach (var (position, hash) in reference)
        {
            // Anchor frames (negative keys) only serve the search for shortened copies.
            if (position < 0 || !other.TryGetValue(position, out var o)) continue;
            sum += DHashService.HammingDistance(hash.Normal, mirrored ? o.Mirrored : o.Normal);
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
    // the same way and decode the whole video again. Both orientations come from the same
    // frames, so asking for the mirror image later costs no second scene detection.
    private async Task<Dictionary<int, ulong>> GetScenePHashesAsync(AppDb.MediaFileRow file, bool mirrored,
        DupeBuildOptions opt, CancellationToken ct)
    {
        var strategy = SceneStrategyKey(opt);
        var mirrorStrategy = strategy + ";mirror";
        var cached = await _db.GetVisualHashesAsync(file.Id, mirrored ? mirrorStrategy : strategy);
        if (cached.Count >= opt.RefinementFrames)
            return cached;

        var timestamps = await _sceneSelector.SelectAsync(file.Path, file.DurationSec!.Value,
            opt.RefinementFrames, opt.SceneThreshold, ct);
        var frames = await _extractor.ExtractGrayFramesAsync(file.Path, timestamps, ct);
        var normal = new Dictionary<int, ulong>();
        var mirror = new Dictionary<int, ulong>();
        for (var slot = 0; slot < frames.Count; slot++)
        {
            if (frames[slot] is not { } frame) continue;
            normal[slot] = _pHasher.ComputePHash64(frame);
            mirror[slot] = _pHasher.ComputePHash64(frame.MirrorHorizontal());
        }
        await _db.UpsertVisualHashesAsync(file.Id, strategy, normal);
        await _db.UpsertVisualHashesAsync(file.Id, mirrorStrategy, mirror);
        return mirrored ? mirror : normal;
    }

    private async Task<AudioFingerprint?> GetAudioPrintAsync(AppDb.MediaFileRow file, CancellationToken ct)
    {
        if (await _db.GetAudioPrintAsync(file.Id, AudioPrintExtractor.Strategy) is { } data)
        {
            try { return AudioFingerprint.Deserialize(data); }
            catch (InvalidDataException) { /* recompute below */ }
        }

        var print = await _audio.ExtractAsync(file.Path, file.DurationSec!.Value, ct);
        if (print is not null)
            await _db.UpsertAudioPrintAsync(file.Id, AudioPrintExtractor.Strategy, print.Serialize());
        return print;
    }

    // Cached scene hashes are only valid for the settings that selected their frames.
    internal static string SceneStrategyKey(DupeBuildOptions opt) =>
        string.Create(CultureInfo.InvariantCulture,
            $"scene-phash-v2;n={opt.RefinementFrames};t={opt.SceneThreshold:0.###}");

    /// <summary>
    /// Parses <c>--name=value</c> options; unknown or malformed values keep their defaults.
    /// <c>--tol --min --maxdist --frames --clear --refine-frames --scene-threshold
    /// --max-phash --margin --parallel --audio --max-audio-ber --mirror --trimmed</c>
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
                "--audio" when bool.TryParse(value, out var b) => opt with { UseAudio = b },
                "--max-audio-ber" when Double(out var d) => opt with { MaxAudioBer = d },
                "--mirror" when bool.TryParse(value, out var b) => opt with { MatchMirrored = b },
                "--trimmed" when bool.TryParse(value, out var b) => opt with { FindTrimmed = b },
                _ => opt
            };
        }
        return opt;
    }
}
