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
}
