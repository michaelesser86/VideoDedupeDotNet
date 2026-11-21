using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Avalonia;
using Avalonia.Styling;

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

    public IReadOnlyList<Choice> LanguageOptions { get; } =
        [new(Loc.English, "English", translate: false), new(Loc.German, "Deutsch", translate: false)];

    public IReadOnlyList<Choice> ThemeOptions { get; } =
        [new("System", "System"), new("Light", "Light"), new("Dark", "Dark")];

    public IReadOnlyList<Choice> ColorSchemeOptions { get; } =
        ColorSchemes.All.Select(s => new Choice(s.Id, s.Name, swatch: ColorSchemes.Gradient(s))).ToList();

    [ObservableProperty] private Choice selectedLanguage;
    [ObservableProperty] private Choice selectedTheme;
    [ObservableProperty] private Choice selectedColorScheme;

    public MainViewModel()
    {
        var settings = AppSettings.Load(AppPaths.SettingsFile);
        if (settings.Language is Loc.English or Loc.German)
            Loc.Instance.Language = settings.Language;
        selectedLanguage = LanguageOptions.First(o => o.Id == Loc.Instance.Language);
        selectedTheme = ThemeOptions.FirstOrDefault(o => o.Id == settings.Theme) ?? ThemeOptions[0];
        selectedColorScheme = ColorSchemeOptions.FirstOrDefault(o => o.Id == settings.ColorScheme) ?? ColorSchemeOptions[0];
        ApplyTheme(selectedTheme.Id);
        ColorSchemes.Apply(ColorSchemes.Find(selectedColorScheme.Id));

        State.DbPath = settings.DbPath;
        State.QuarantineRoot = settings.QuarantineRoot;

        Library = new LibraryViewModel(State);
        Build = new DupeBuildViewModel(State, settings);
        Groups = new GroupsViewModel(State, () => Build.FramePositions);
        Build.Finished += Groups.LoadGroupsAsync;
        // Another database: show its folders and groups right away.
        State.DatabaseChanged += () => _ = LoadAllAsync();

        State.Status = Loc.T("Ready.");
        _ = CheckToolsAsync();
    }

    partial void OnSelectedLanguageChanged(Choice value) => Loc.Instance.Language = value.Id;

    partial void OnSelectedThemeChanged(Choice value) => ApplyTheme(value.Id);

    partial void OnSelectedColorSchemeChanged(Choice value) => ColorSchemes.Apply(ColorSchemes.Find(value.Id));

    private static void ApplyTheme(string id)
    {
        if (Application.Current is { } app)
            app.RequestedThemeVariant = id switch
            {
                "Light" => ThemeVariant.Light,
                "Dark" => ThemeVariant.Dark,
                _ => ThemeVariant.Default
            };
    }

    /// <summary>Called once the window is shown: the user should not have to click "load".</summary>
    public async Task LoadAllAsync()
    {
        await Library.LoadRootsAsync();
        await Groups.LoadGroupsAsync();
        State.Status = Groups.Groups.Count > 0
            ? $"{Groups.Summary}."
            : Library.Roots.Count == 0
                ? Loc.T("Add a folder with videos to get started.")
                : Loc.T("Scan the library, then find duplicates.");
    }

    /// <summary>Called when the main window closes.</summary>
    public void SaveSettings()
    {
        var settings = new AppSettings
        {
            DbPath = State.DbPath,
            QuarantineRoot = State.QuarantineRoot,
            Theme = SelectedTheme.Id,
            Language = SelectedLanguage.Id,
            ColorScheme = SelectedColorScheme.Id
        };
        Build.SaveTo(settings);
        settings.Save(AppPaths.SettingsFile);
    }

    [RelayCommand]
    public async Task PickDbAsync()
    {
        try
        {
            if (await DesktopShell.PickDatabaseAsync() is { } path)
                State.DbPath = path;
        }
        catch (Exception ex)
        {
            State.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task PickQuarantineAsync()
    {
        try
        {
            if (await DesktopShell.PickFolderAsync(Loc.T("Select quarantine folder")) is { } path)
                State.QuarantineRoot = path;
        }
        catch (Exception ex)
        {
            State.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    // Without ffmpeg every scan fails file by file; say so once, up front.
    private async Task CheckToolsAsync()
    {
        try
        {
            var missing = await new FfmpegTools().FindMissingToolsAsync();
            if (missing.Count > 0)
                State.Status = Loc.F(
                    "{0} not found. Install FFmpeg and add its bin folder to PATH, then restart VideoDedupe; scanning and detection need it.",
                    string.Join(Loc.T(" and "), missing));
        }
        catch (Exception ex)
        {
            System.Diagnostics.Trace.TraceWarning($"Tool check failed: {ex.Message}");
        }
    }
}
