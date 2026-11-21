namespace VideoDedupe.Infrastructure;

/// <summary>
/// Which library folders the user wants to keep files from, and which to clear first.
/// A file belongs to the deepest library folder that contains it.
/// </summary>
public sealed class FolderPriorities
{
    public const int Prefer = 1;
    public const int Normal = 0;
    public const int Avoid = -1;

    public static FolderPriorities None { get; } = new([]);

    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private readonly List<(string Root, int Priority)> _roots;

    public FolderPriorities(IEnumerable<(string Path, int Priority)> roots)
    {
        _roots = roots
            .Where(r => !string.IsNullOrWhiteSpace(r.Path))
            .Select(r => (Root: Path.TrimEndingDirectorySeparator(r.Path), Priority: Math.Clamp(r.Priority, Avoid, Prefer)))
            .OrderByDescending(r => r.Root.Length)
            .ToList();
    }

    public bool HasRules => _roots.Any(r => r.Priority != Normal);

    public int Of(string path)
    {
        foreach (var (root, priority) in _roots)
        {
            if (path.Length > root.Length
                && path.StartsWith(root, PathComparison)
                // A drive root ("D:\") keeps its separator; any other root needs one next.
                && (Path.EndsInDirectorySeparator(root)
                    || path[root.Length] == Path.DirectorySeparatorChar
                    || path[root.Length] == Path.AltDirectorySeparatorChar))
                return priority;
        }
        return Normal;
    }
}
