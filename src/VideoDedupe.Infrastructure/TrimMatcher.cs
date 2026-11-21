using System.Numerics;

using VideoDedupe.Core;

namespace VideoDedupe.Infrastructure;

/// <summary>
/// Finds candidate pairs of shortened copies: files of different length whose frame a fixed
/// time after the start (or before the end) matches. A copy with cut-off credits shares the
/// start; one with a cut-off intro shares the end.
/// </summary>
internal static class TrimMatcher
{
    /// <summary>Frame hash keys of the anchors (percent positions are positive).</summary>
    public const int StartAnchorKey = -1, EndAnchorKey = -2;

    /// <summary>Seconds between the anchor frame and the start or end of the file.</summary>
    public const double AnchorSec = 60;

    /// <summary>Shorter files have no room for anchors away from both ends.</summary>
    public const double MinDurationSec = 3 * AnchorSec;

    /// <summary>A copy must keep this share of the longer file; shorter ones are excerpts.</summary>
    public const double MinLengthRatio = 0.7;

    /// <summary>
    /// An anchor matching more files than this is a shared opening or credits sequence (a TV
    /// series, a studio logo), not a copy; such anchors are ignored.
    /// </summary>
    public const int MaxAnchorMatches = 5;

    public sealed record Candidate(AppDb.MediaFileRow Longer, AppDb.MediaFileRow Shorter, string Kind, bool Mirrored);

    /// <summary>Nearly flat frames (black, fades) hash to almost no set bits and match everything.</summary>
    public static bool IsInformative(ulong hash) => BitOperations.PopCount(hash) is >= 8 and <= 56;

    public static List<Candidate> FindCandidates(
        IReadOnlyList<(AppDb.MediaFileRow File, Dictionary<int, FrameHash> Hashes)> files,
        double tolSec, double maxDist, bool matchMirrored)
    {
        var result = new Dictionary<(long, long), Candidate>();
        foreach (var (key, kind) in new[] { (StartAnchorKey, AppDb.TrimKinds.SameStart), (EndAnchorKey, AppDb.TrimKinds.SameEnd) })
        {
            var items = files
                .Where(f => f.Hashes.TryGetValue(key, out var h) && IsInformative(h.Normal))
                .OrderBy(f => f.File.DurationSec)
                .ToList();

            var matches = new List<(int A, int B, bool Mirrored)>();
            var counts = new int[items.Count];
            for (var i = 0; i < items.Count; i++)
            {
                var durI = items[i].File.DurationSec!.Value;
                var hashI = items[i].Hashes[key];
                // Sorted by duration: later files are at least as long, and only those up to
                // durI / MinLengthRatio can still be a longer version of this one.
                for (var j = i + 1; j < items.Count; j++)
                {
                    var durJ = items[j].File.DurationSec!.Value;
                    if (durI < durJ * MinLengthRatio) break;
                    if (durJ - durI <= tolSec) continue; // same length: the regular search covers it

                    var hashJ = items[j].Hashes[key];
                    var normal = DHashService.HammingDistance(hashI.Normal, hashJ.Normal);
                    var mirrored = matchMirrored ? DHashService.HammingDistance(hashI.Normal, hashJ.Mirrored) : int.MaxValue;
                    if (Math.Min(normal, mirrored) > maxDist) continue;
                    matches.Add((i, j, mirrored < normal));
                    counts[i]++;
                    counts[j]++;
                }
            }

            foreach (var (i, j, mirrored) in matches)
            {
                if (counts[i] > MaxAnchorMatches || counts[j] > MaxAnchorMatches) continue;
                var pair = (items[i].File.Id, items[j].File.Id);
                result.TryAdd(pair, new Candidate(items[j].File, items[i].File, kind, mirrored));
            }
        }
        return result.Values.ToList();
    }
}
