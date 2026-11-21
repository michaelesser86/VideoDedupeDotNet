using VideoDedupe.Core;

namespace VideoDedupe.Infrastructure;

/// <summary>
/// Finds excerpts: short files whose pictures appear somewhere inside a much longer file.
/// Every keyframe of the long file shows a picture that, in an excerpt, sits at most a tenth of
/// a second from one of the excerpt's densely sampled frames (5 per second). Each such pair
/// votes for an offset (where the excerpt starts in the long file); an offset with enough votes
/// is a candidate, which the caller then checks at the excerpt's own frame positions.
/// </summary>
internal static class ExcerptMatcher
{
    /// <summary>Shorter clips are too short to tell apart from a shared logo or intro.</summary>
    public const double MinExcerptSec = 20;

    /// <summary>Longer files are not sampled densely: decoding them completely costs too much.</summary>
    public const double MaxExcerptSec = 600;

    /// <summary>Dense sampling rate of possible excerpts.</summary>
    public const int SamplesPerSecond = 5;

    /// <summary>Frames this close count as the same picture.</summary>
    public const int MaxDistance = 4;

    /// <summary>Keyframes of the long file that must agree on one offset.</summary>
    public const int MinVotes = 3;

    /// <summary>A hash band shared by more frames than this is a common pattern, not evidence.</summary>
    public const int MaxBucket = 400;

    public const int MaxSourcesPerExcerpt = 3;

    public sealed record Candidate(AppDb.MediaFileRow Source, AppDb.MediaFileRow Excerpt, double OffsetSec, int Votes);

    public static bool CanBeExcerpt(AppDb.MediaFileRow f) => f.DurationSec is >= MinExcerptSec and <= MaxExcerptSec;

    public static List<Candidate> FindCandidates(
        IReadOnlyList<(AppDb.MediaFileRow File, IReadOnlyList<(long TimeMs, ulong Hash)> Keyframes)> sources,
        IReadOnlyList<(AppDb.MediaFileRow File, IReadOnlyList<(long TimeMs, ulong Hash)> Samples)> excerpts)
    {
        // Dense samples of all possible excerpts, indexed by four 16-bit bands: two hashes at
        // most 3 bits apart always share a band, and most at 4 bits do.
        var samples = new List<(int Excerpt, long TimeMs, ulong Hash)>();
        for (var x = 0; x < excerpts.Count; x++)
            foreach (var (time, hash) in excerpts[x].Samples)
                if (DHashService.IsInformative(hash))
                    samples.Add((x, time, hash));

        var index = new Dictionary<ushort, List<int>>[4];
        for (var band = 0; band < 4; band++)
            index[band] = new Dictionary<ushort, List<int>>();
        for (var s = 0; s < samples.Count; s++)
            for (var band = 0; band < 4; band++)
            {
                var key = Band(samples[s].Hash, band);
                if (!index[band].TryGetValue(key, out var list))
                    index[band][key] = list = [];
                list.Add(s);
            }

        // Offsets in tenths of a second; each keyframe of a source votes at most once per offset.
        var votes = new Dictionary<(int Excerpt, int Source, long Tenth), List<double>>();
        var seen = new HashSet<int>();
        var voted = new HashSet<(int, long)>();
        for (var src = 0; src < sources.Count; src++)
        {
            if (sources[src].File.DurationSec is not double sourceDur) continue;
            foreach (var (sourceTime, sourceHash) in sources[src].Keyframes)
            {
                if (!DHashService.IsInformative(sourceHash)) continue;
                seen.Clear();
                voted.Clear();
                for (var band = 0; band < 4; band++)
                {
                    if (!index[band].TryGetValue(Band(sourceHash, band), out var list) || list.Count > MaxBucket)
                        continue;
                    foreach (var s in list)
                    {
                        if (!seen.Add(s)) continue;
                        var (x, sampleTime, sampleHash) = samples[s];
                        var excerpt = excerpts[x].File;
                        if (excerpt.Id == sources[src].File.Id) continue;
                        // Much shorter only; copies of similar length are the trim search's job.
                        if (excerpt.DurationSec!.Value >= sourceDur * TrimMatcher.MinLengthRatio) continue;
                        if (DHashService.HammingDistance(sourceHash, sampleHash) > MaxDistance) continue;

                        var offset = (sourceTime - sampleTime) / 1000.0;
                        if (offset < -1 || offset + excerpt.DurationSec.Value > sourceDur + 1) continue;
                        var tenth = (long)Math.Round(offset * 10);
                        if (!voted.Add((x, tenth))) continue;
                        if (!votes.TryGetValue((x, src, tenth), out var values))
                            votes[(x, src, tenth)] = values = [];
                        values.Add(offset);
                    }
                }
            }
        }

        // Neighbouring offsets (±0.3 s) count together: samples are 0.2 s apart.
        var result = new List<Candidate>();
        foreach (var perExcerpt in votes.GroupBy(v => v.Key.Excerpt))
        {
            var best = perExcerpt
                .Select(v => (v.Key.Source, Values: Enumerable.Range(-3, 7)
                    .SelectMany(d => votes.GetValueOrDefault((v.Key.Excerpt, v.Key.Source, v.Key.Tenth + d)) ?? [])
                    .ToList()))
                .GroupBy(v => v.Source)
                .Select(g => g.MaxBy(v => v.Values.Count))
                .Where(v => v.Values.Count >= MinVotes)
                .OrderByDescending(v => v.Values.Count)
                .Take(MaxSourcesPerExcerpt);
            foreach (var (source, values) in best)
                result.Add(new Candidate(sources[source].File, excerpts[perExcerpt.Key].File, Median(values), values.Count));
        }
        return result;
    }

    private static ushort Band(ulong hash, int band) => (ushort)(hash >> (16 * band));

    private static double Median(List<double> values)
    {
        var sorted = values.Order().ToList();
        return sorted[sorted.Count / 2];
    }
}
