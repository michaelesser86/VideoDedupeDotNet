namespace VideoDedupe.Infrastructure;

public sealed class FileMover
{
    public string QuarantineRoot { get; }

    public FileMover(string quarantineRoot)
    {
        QuarantineRoot = quarantineRoot;
    }

    public string EnsureQuarantineRoot()
    {
        Directory.CreateDirectory(QuarantineRoot);
        return QuarantineRoot;
    }

    public string MoveToQuarantine(string sourcePath)
    {
        EnsureQuarantineRoot();

        var fileName = Path.GetFileName(sourcePath);
        var target = Path.Combine(QuarantineRoot, fileName);

        // collision-safe rename
        if (File.Exists(target))
        {
            var baseName = Path.GetFileNameWithoutExtension(fileName);
            var ext = Path.GetExtension(fileName);
            var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            target = Path.Combine(QuarantineRoot, $"{baseName}_{stamp}{ext}");
        }

        File.Move(sourcePath, target);
        return target;
    }

    /// <summary>
    /// Moves a quarantined file back to where it came from. Never overwrites: if another file
    /// took its place meanwhile, the user has to decide.
    /// </summary>
    public static void Restore(string quarantinePath, string originalPath)
    {
        if (!File.Exists(quarantinePath))
            throw new FileNotFoundException($"Not in the quarantine folder anymore: {quarantinePath}", quarantinePath);
        if (File.Exists(originalPath))
            throw new IOException($"Another file already exists at {originalPath}");
        if (Path.GetDirectoryName(originalPath) is { Length: > 0 } directory)
            Directory.CreateDirectory(directory);
        File.Move(quarantinePath, originalPath);
    }
}
