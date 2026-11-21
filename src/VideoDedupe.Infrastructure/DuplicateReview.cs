using System.Globalization;

namespace VideoDedupe.Infrastructure;

/// <summary>Decides which copy of a duplicate group to keep, and why.</summary>
public static class BestFileSelector
{
    public const string KeepDecision = "keep";

    /// <summary>Why a file was chosen; lets the app word the reason in its own language.</summary>
    public enum ReasonKind
    {
        MarkedKeep, OnlyFile, HighestResolution, LargestFile, Newest, SameQuality,
        /// <summary>The file is in a folder the user prefers to keep.</summary>
        PreferredFolder,
        /// <summary>The other copies are in folders the user wants cleared first.</summary>
        OthersInAvoidedFolder
    }

    public sealed record Choice(AppDb.MediaFileRow File, string Reason, ReasonKind Kind);

    /// <summary>
    /// Files the user marked as keep win; then files from the highest-priority folder; among
    /// those the highest resolution, then the largest file (more bitrate at the same
    /// resolution), then the newest.
    /// </summary>
    public static Choice Choose(IReadOnlyList<AppDb.MediaFileRow> files,
        IReadOnlyDictionary<long, string>? decisions = null, FolderPriorities? priorities = null)
    {
        if (files.Count == 0)
            throw new ArgumentException("A group needs at least one file.", nameof(files));

        var kept = files.Where(f => decisions?.GetValueOrDefault(f.Id) == KeepDecision).ToList();
        if (kept.Count > 0)
            return new Choice(Rank(kept)[0], "marked as keep", ReasonKind.MarkedKeep);

        if (priorities is { HasRules: true })
        {
            var top = files.Max(f => priorities.Of(f.Path));
            var preferred = files.Where(f => priorities.Of(f.Path) == top).ToList();
            if (preferred.Count < files.Count)
                return top == FolderPriorities.Prefer
                    ? new Choice(Rank(preferred)[0], "in a preferred folder", ReasonKind.PreferredFolder)
                    : new Choice(Rank(preferred)[0], "the other copies are in folders to clear first", ReasonKind.OthersInAvoidedFolder);
        }

        var ranked = Rank(files);
        var best = ranked[0];
        if (ranked.Count == 1)
            return new Choice(best, "only file", ReasonKind.OnlyFile);

        var next = ranked[1];
        if (Pixels(best) > Pixels(next))
            return new Choice(best, $"highest resolution ({best.Width}×{best.Height})", ReasonKind.HighestResolution);
        if (best.SizeBytes > next.SizeBytes)
            return new Choice(best, "same resolution, largest file (most detail)", ReasonKind.LargestFile);
        if (Modified(best) > Modified(next))
            return new Choice(best, "same resolution and size, newest", ReasonKind.Newest);
        return new Choice(best, "same quality as the others", ReasonKind.SameQuality);
    }

    private static List<AppDb.MediaFileRow> Rank(IEnumerable<AppDb.MediaFileRow> files) => files
        .OrderByDescending(Pixels)
        .ThenByDescending(f => f.SizeBytes)
        .ThenByDescending(Modified)
        .ThenBy(f => f.Path, StringComparer.Ordinal)
        .ToList();

    private static long Pixels(AppDb.MediaFileRow f) => (f.Width ?? 0) * (long)(f.Height ?? 0);

    private static DateTime Modified(AppDb.MediaFileRow f) =>
        DateTime.TryParse(f.ModifiedUtc, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt)
            ? dt
            : DateTime.MinValue;

    /// <summary>Average bitrate in Mbit/s, from file size and duration.</summary>
    public static double? BitrateMbps(AppDb.MediaFileRow f) =>
        f.DurationSec is > 0 && f.SizeBytes > 0 ? f.SizeBytes * 8 / f.DurationSec.Value / 1_000_000 : null;
}

/// <summary>Which files a "clean up all groups" run moves to quarantine.</summary>
public static class CleanupPlanner
{
    public sealed record Group(long GroupId, IReadOnlyList<AppDb.MediaFileRow> Members);

    public sealed record Item(long GroupId, AppDb.MediaFileRow File, AppDb.MediaFileRow KeptFile);

    public sealed record Plan(IReadOnlyList<Item> Items, int GroupCount)
    {
        public long Bytes => Items.Sum(i => i.File.SizeBytes);
    }

    /// <summary>
    /// Every group keeps its best file (or the files marked as keep). A file that some group
    /// keeps is never moved, even if another group would drop it, so no group loses all of its
    /// copies. Missing files are ignored.
    /// </summary>
    public static Plan Create(IEnumerable<Group> groups, IReadOnlyDictionary<long, string> decisions,
        Func<string, bool> exists, FolderPriorities? priorities = null)
    {
        var usable = new List<(Group Group, List<AppDb.MediaFileRow> Present, List<AppDb.MediaFileRow> Keepers)>();
        foreach (var group in groups)
        {
            var present = group.Members.Where(m => exists(m.Path)).DistinctBy(m => m.Id).ToList();
            if (present.Count < 2)
                continue;
            var keepers = present.Where(m => decisions.GetValueOrDefault(m.Id) == BestFileSelector.KeepDecision).ToList();
            if (keepers.Count == 0)
                keepers.Add(BestFileSelector.Choose(present, decisions, priorities).File);
            usable.Add((group, present, keepers));
        }

        var keptIds = usable.SelectMany(u => u.Keepers).Select(k => k.Id).ToHashSet();
        var planned = new HashSet<long>();
        var items = new List<Item>();
        var groupCount = 0;
        foreach (var (group, present, keepers) in usable)
        {
            var before = items.Count;
            foreach (var member in present)
                if (!keptIds.Contains(member.Id) && planned.Add(member.Id))
                    items.Add(new Item(group.GroupId, member, keepers[0]));
            if (items.Count > before) groupCount++;
        }
        return new Plan(items, groupCount);
    }
}
