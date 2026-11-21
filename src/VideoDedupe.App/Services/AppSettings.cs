using System;
using System.Diagnostics;
using System.IO;
using System.Text.Json;

using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.Services;

/// <summary>User choices that survive a restart, stored as JSON next to the default database.</summary>
public sealed class AppSettings
{
    private static readonly DupeBuildOptions Defaults = new();
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

    public string DbPath { get; set; } = AppPaths.DefaultDatabase;
    public string QuarantineRoot { get; set; } = AppPaths.DefaultQuarantine;
    public bool DupeClearExisting { get; set; } = Defaults.ClearExisting;
    public double DupeTolSec { get; set; } = Defaults.TolSec;
    public int DupeMinItems { get; set; } = Defaults.MinItems;
    public double DupeMaxAvgDist { get; set; } = Defaults.MaxAvgDist;
    public string DupeFramesCsv { get; set; } = string.Join(",", DupeBuildOptions.DefaultFramePercents);
    public bool DupeMatchMirrored { get; set; } = Defaults.MatchMirrored;
    public bool DupeUseAudio { get; set; } = Defaults.UseAudio;

    /// <summary>Missing or unreadable settings fall back to defaults instead of blocking startup.</summary>
    public static AppSettings Load(string path)
    {
        try
        {
            if (File.Exists(path))
                return JsonSerializer.Deserialize<AppSettings>(File.ReadAllText(path), Json) ?? new AppSettings();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
        {
            Trace.TraceWarning($"Settings ignored: {path}: {ex.Message}");
            return new AppSettings();
        }

        // First start with this version: earlier versions kept the database next to the
        // executable. Copy it (never move: the original stays as a fallback) so the library
        // does not seem to vanish.
        var settings = new AppSettings();
        var legacy = Path.Combine(AppContext.BaseDirectory, "videodedupe.db");
        if (File.Exists(legacy) && !File.Exists(settings.DbPath))
        {
            try
            {
                foreach (var suffix in new[] { "", "-wal", "-shm" })
                    if (File.Exists(legacy + suffix))
                        File.Copy(legacy + suffix, settings.DbPath + suffix);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                Trace.TraceWarning($"Old database not copied, using it in place: {legacy}: {ex.Message}");
                settings.DbPath = legacy;
            }
        }
        return settings;
    }

    public void Save(string path)
    {
        try
        {
            // Write-then-rename so a crash mid-write never leaves truncated settings.
            var temp = path + ".tmp";
            File.WriteAllText(temp, JsonSerializer.Serialize(this, Json));
            File.Move(temp, path, overwrite: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Trace.TraceWarning($"Settings not saved: {path}: {ex.Message}");
        }
    }
}
