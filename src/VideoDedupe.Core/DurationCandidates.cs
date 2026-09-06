namespace VideoDedupe.Core;

public static class DurationCandidates
{
    // Split only at gaps larger than the tolerance: no eligible pair can cross a split.
    // Callers still check each candidate against its seed inside each range.
    public static List<List<T>> CreateRanges<T>(IEnumerable<T> files,
        Func<T, double?> duration, double tolerance, int minItems)
    {
        if (!double.IsFinite(tolerance) || tolerance < 0)
            throw new ArgumentOutOfRangeException(nameof(tolerance));
        if (minItems < 2)
            throw new ArgumentOutOfRangeException(nameof(minItems));

        var sorted = files.Select(file => (File: file, Duration: duration(file)))
            .Where(x => x.Duration is double d && double.IsFinite(d) && d > 0)
            .OrderBy(x => x.Duration).ToList();
        var ranges = new List<List<T>>();
        var current = new List<T>();
        double previous = 0;
        foreach (var item in sorted)
        {
            if (current.Count > 0 && item.Duration!.Value - previous > tolerance)
            {
                if (current.Count >= minItems) ranges.Add(current);
                current = new List<T>();
            }
            current.Add(item.File);
            previous = item.Duration!.Value;
        }
        if (current.Count >= minItems) ranges.Add(current);
        return ranges;
    }
}
