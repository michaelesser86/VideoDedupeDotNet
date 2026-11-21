namespace VideoDedupe.Infrastructure;

/// <summary>
/// Per-user storage. The build output folder is wiped by a clean build and is read-only
/// under Program Files, so neither the database nor settings belong there.
/// </summary>
public static class AppPaths
{
    public static string DataDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create),
        "VideoDedupe");

    public static string DefaultDatabase => Path.Combine(EnsureDataDirectory(), "videodedupe.db");

    public static string SettingsFile => Path.Combine(EnsureDataDirectory(), "settings.json");

    public static string DefaultQuarantine => Path.Combine(DataDirectory, "Quarantine");

    private static string EnsureDataDirectory()
    {
        Directory.CreateDirectory(DataDirectory);
        return DataDirectory;
    }
}
