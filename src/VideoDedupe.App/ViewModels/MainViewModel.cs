using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;

using Avalonia.Controls;
using Avalonia.Platform.Storage;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

public partial class MainViewModel : ObservableObject
{
    [ObservableProperty] private string dbPath = "videodedupe.db";
    [ObservableProperty] private bool isBusy;
    [ObservableProperty] private string status = "";

    public ObservableCollection<GroupVm> Groups { get; } = new();

    [ObservableProperty] private GroupVm? selectedGroup;

    [RelayCommand]
    public async Task LoadGroupsAsync()
    {
        try
        {
            IsBusy = true;
            Status = "Loading groups...";

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            var groups = await db.ListDuplicateGroupsWithMembersAsync(take: 200);

            Groups.Clear();

            foreach (var (g, members) in groups)
            {
                var gvm = new GroupVm
                {
                    GroupId = g.Id,
                    Algorithm = g.Algorithm,
                    Frames = g.Frames,
                    TolSec = g.TolSec,
                    MaxAvgDist = g.MaxAvgDist,
                    CreatedUtc = g.CreatedUtc
                };

                foreach (var (file, avgDist) in members)
                    gvm.Members.Add(new MemberVm(file, avgDist));

                Groups.Add(gvm);
            }

            SelectedGroup = Groups.FirstOrDefault();
            Status = $"Loaded {Groups.Count} groups.";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    [RelayCommand]
    public async Task PickDbAsync()
    {
        try
        {
            var topLevel = TopLevel.GetTopLevel(App.Current?.ApplicationLifetime switch
            {
                Avalonia.Controls.ApplicationLifetimes.IClassicDesktopStyleApplicationLifetime desktop => desktop.MainWindow,
                _ => null
            });

            if (topLevel is null)
            {
                Status = "No UI context available.";
                return;
            }

            var files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
            {
                Title = "Select SQLite Database",
                AllowMultiple = false,
                FileTypeFilter = new[]
                {
                new FilePickerFileType("SQLite Database")
                {
                    Patterns = new[] { "*.db", "*.sqlite", "*.sqlite3" }
                },
                FilePickerFileTypes.All
            }
            });

            if (files.Count > 0)
            {
                DbPath = files[0].Path.LocalPath;
                Status = $"Selected: {DbPath}";
            }
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
    }
}

public partial class GroupVm : ObservableObject
{
    public long GroupId { get; set; }
    public string Algorithm { get; set; } = "";
    public string Frames { get; set; } = "";
    public double TolSec { get; set; }
    public double MaxAvgDist { get; set; }
    public string CreatedUtc { get; set; } = "";

    public ObservableCollection<MemberVm> Members { get; } = new();

    public string Title => $"#{GroupId}  items={Members.Count}";
}

public partial class MemberVm : ObservableObject
{
    public long MediaId => File.Id;
    public string Path => File.Path;
    public long SizeBytes => File.SizeBytes;
    public double? DurationSec => File.DurationSec;
    public int? Width => File.Width;
    public int? Height => File.Height;
    public string? VideoCodec => File.VideoCodec;

    public double AvgDist { get; }

    public AppDb.MediaFileRow File { get; }

    public MemberVm(AppDb.MediaFileRow file, double avgDist)
    {
        File = file;
        AvgDist = avgDist;
    }
}
