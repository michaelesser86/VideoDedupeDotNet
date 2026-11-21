using System;
using System.Threading.Tasks;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>The window: shared state plus one view model per area.</summary>
public partial class MainViewModel : ObservableObject
{
    public AppState State { get; } = new();
    public LibraryViewModel Library { get; }
    public DupeBuildViewModel Build { get; }
    public GroupsViewModel Groups { get; }

    public MainViewModel()
    {
        var settings = AppSettings.Load(AppPaths.SettingsFile);
        State.DbPath = settings.DbPath;
        State.QuarantineRoot = settings.QuarantineRoot;

        Library = new LibraryViewModel(State);
        Build = new DupeBuildViewModel(State, settings);
        Groups = new GroupsViewModel(State, () => Build.FramePositions);
        Build.Finished += Groups.LoadGroupsAsync;
        State.DatabaseChanged += () =>
            State.Status = "Database changed. Load roots or groups for the selected database.";

        State.Status = $"Database: {State.DbPath}";
        _ = CheckToolsAsync();
    }

    // Without ffmpeg every scan fails file by file; say so once, up front.
    private async Task CheckToolsAsync()
    {
        try
        {
            var missing = await new FfmpegTools().FindMissingToolsAsync();
            if (missing.Count > 0)
                State.Status = $"{string.Join(" and ", missing)} not found. Install FFmpeg and add its bin folder " +
                               "to PATH, then restart VideoDedupe; scanning and detection need it.";
        }
        catch (Exception ex)
        {
            System.Diagnostics.Trace.TraceWarning($"Tool check failed: {ex.Message}");
        }
    }

    /// <summary>Called when the main window closes.</summary>
    public void SaveSettings()
    {
        var settings = new AppSettings { DbPath = State.DbPath, QuarantineRoot = State.QuarantineRoot };
        Build.SaveTo(settings);
        settings.Save(AppPaths.SettingsFile);
    }

    [RelayCommand]
    public async Task PickDbAsync()
    {
        try
        {
            if (await DesktopShell.PickDatabaseAsync() is { } path)
            {
                State.DbPath = path;
                State.Status = $"Selected: {path}";
            }
        }
        catch (Exception ex)
        {
            State.Status = $"Error: {ex.Message}";
        }
    }

    [RelayCommand]
    public async Task PickQuarantineAsync()
    {
        try
        {
            if (await DesktopShell.PickFolderAsync("Select quarantine folder") is { } path)
                State.QuarantineRoot = path;
        }
        catch (Exception ex)
        {
            State.Status = $"Error: {ex.Message}";
        }
    }
}
