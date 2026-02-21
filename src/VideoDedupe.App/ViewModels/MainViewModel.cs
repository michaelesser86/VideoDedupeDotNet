using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Media.Imaging;
using Avalonia.Platform.Storage;
using Avalonia.Threading;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

public partial class MainViewModel : ObservableObject
{
    [ObservableProperty] private string dbPath = "videodedupe.db";
    [ObservableProperty] private bool isBusy;
    [ObservableProperty] private string status = "";
    [ObservableProperty] private string groupFilter = "";
    [ObservableProperty] private string groupSort = "SizeDesc"; // SizeDesc | Newest
    public ObservableCollection<GroupVm> Groups { get; } = new();
    public ObservableCollection<GroupVm> FilteredGroups { get; } = new();

    [ObservableProperty] private GroupVm? selectedGroup;

    [ObservableProperty] private string quarantineRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "VideoDedupe_Quarantine");


    private CancellationTokenSource? _thumbCts;

    private readonly FfmpegTools _tools = new();
    private readonly FfmpegFrameExtractor _extractor;
    private readonly Dictionary<(long MediaId, int Pos), Bitmap> _thumbCache = new();

    partial void OnGroupFilterChanged(string value) => RefreshGroupView();
    partial void OnGroupSortChanged(string value) => RefreshGroupView();
    public MainViewModel()
    {
        _extractor = new FfmpegFrameExtractor(_tools);
    }

    partial void OnSelectedGroupChanged(GroupVm? value)
    {
        // Cancel previous thumbnail load
        _thumbCts?.Cancel();
        _thumbCts?.Dispose();
        _thumbCts = null;

        if (value is null)
            return;

        // Debounce: in case user scrolls quickly
        _thumbCts = new CancellationTokenSource();
        var ct = _thumbCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(200, ct);
                await LoadThumbnailsInternalAsync(ct);
            }
            catch (OperationCanceledException) { }
        });
    }

    [RelayCommand]
    public async Task PickDbAsync()
    {
        try
        {
            var lifetime = Application.Current?.ApplicationLifetime as IClassicDesktopStyleApplicationLifetime;
            var topLevel = lifetime?.MainWindow is null ? null : TopLevel.GetTopLevel(lifetime.MainWindow);
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

                MarkBestMember(gvm);

                Groups.Add(gvm);
            }
            RefreshGroupView();
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
    public async Task LoadThumbnailsAsync()
    {
        _thumbCts?.Cancel();
        _thumbCts?.Dispose();
        _thumbCts = new CancellationTokenSource();

        await LoadThumbnailsInternalAsync(_thumbCts.Token);
    }

    private async Task LoadThumbnailsInternalAsync(CancellationToken ct)
    {
        if (SelectedGroup is null) return;

        try
        {
            IsBusy = true;
            Status = "Loading thumbnails...";

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            var positions = new[] { 20, 50, 80 };

            // limit concurrency (disk + ffmpeg)
            using var sem = new SemaphoreSlim(2);

            var members = SelectedGroup.Members.ToList();
            var tasks = members.Select(async m =>
            {
                ct.ThrowIfCancellationRequested();

                await sem.WaitAsync(ct);
                try
                {
                    await LoadThumbsForMemberAsync(m, positions, ct);
                }
                finally
                {
                    sem.Release();
                }
            }).ToList();

            await Task.WhenAll(tasks);
            Status = "Thumbnails loaded.";
        }
        catch (OperationCanceledException)
        {
            Status = "Thumbnail loading canceled.";
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

    private async Task LoadThumbsForMemberAsync(MemberVm m, int[] positions, CancellationToken ct)
    {
        // basic guards
        if (m.DurationSec is null || m.DurationSec <= 1.0) return;
        if (string.IsNullOrWhiteSpace(m.Path) || !File.Exists(m.Path)) return;

        await Dispatcher.UIThread.InvokeAsync(() => m.ThumbsLoading = true);

        try
        {
            foreach (var pos in positions)
            {
                ct.ThrowIfCancellationRequested();

                var key = (m.MediaId, pos);

                if (_thumbCache.TryGetValue(key, out var cached))
                {
                    await Dispatcher.UIThread.InvokeAsync(() => SetThumb(m, pos, cached));
                    continue;
                }

                var dur = m.DurationSec!.Value;

                // kaputte ffprobe-Werte abfangen
                if (dur <= 1.0 || dur > 48 * 3600)
                    return;

                var ts = dur * (pos / 100.0);
                ts = Math.Clamp(ts, 0.1, Math.Max(0.1, dur - 0.1));


                byte[] png;
                try
                {
                    png = await _extractor.ExtractFramePngAsync(m.Path, ts, ct);
                }
                catch
                {
                    continue;
                }

                Bitmap bmp;
                await using (var ms = new MemoryStream(png))
                    bmp = new Bitmap(ms);

                _thumbCache[key] = bmp;

                await Dispatcher.UIThread.InvokeAsync(() => SetThumb(m, pos, bmp));
            }
        }
        finally
        {
            await Dispatcher.UIThread.InvokeAsync(() => m.ThumbsLoading = false);
        }
    }

    private void RefreshGroupView()
    {
        IEnumerable<GroupVm> q = Groups;

        // sort
        q = GroupSort switch
        {
            "Newest" => q.OrderByDescending(g => g.GroupId), // Id steigt mit Zeit
            _ => q.OrderByDescending(g => g.Members.Count)   // SizeDesc
        };

        // filter (match in any member path)
        var f = (GroupFilter ?? "").Trim();
        if (f.Length > 0)
        {
            q = q.Where(g =>
                g.Members.Any(m =>
                    m.Path.Contains(f, StringComparison.OrdinalIgnoreCase)));
        }

        FilteredGroups.Clear();
        foreach (var g in q)
            FilteredGroups.Add(g);

        // keep selection valid
        if (SelectedGroup is null || !FilteredGroups.Contains(SelectedGroup))
            SelectedGroup = FilteredGroups.FirstOrDefault();
    }
    private static void SetThumb(MemberVm m, int pos, Bitmap bmp)
    {
        switch (pos)
        {
            case 20: m.Thumb20 = bmp; break;
            case 50: m.Thumb50 = bmp; break;
            case 80: m.Thumb80 = bmp; break;
        }
    }

    private static void MarkBestMember(GroupVm g)
    {
        foreach (var m in g.Members)
            m.IsBest = false;

        if (g.Members.Count == 0) return;

        static long Pixels(MemberVm m) => (m.Width ?? 0) * (long)(m.Height ?? 0);

        static DateTime ModifiedOrMin(MemberVm m)
        {
            var s = m.File.ModifiedUtc;
            if (!string.IsNullOrWhiteSpace(s) &&
                DateTime.TryParse(s, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt))
                return dt;
            return DateTime.MinValue;
        }

        var best = g.Members
            .OrderByDescending(Pixels)
            .ThenByDescending(m => m.SizeBytes)
            .ThenByDescending(ModifiedOrMin)
            .First();

        best.IsBest = true;
    }

    [RelayCommand]
    public async Task MarkKeepAsync(MemberVm? m)
    {
        if (m is null) return;

        try
        {
            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            await db.UpsertDecisionAsync(m.MediaId, "keep", note: m.Path);

            m.Decision = "keep";
            Status = "Marked KEEP.";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
    }

    [RelayCommand]
    public async Task QuarantineAsync(MemberVm? m)
    {
        if (m is null) return;

        try
        {
            IsBusy = true;
            Status = "Quarantining...";

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            if (!File.Exists(m.Path))
            {
                Status = "File missing.";
                return;
            }

            var mover = new VideoDedupe.Infrastructure.FileMover(QuarantineRoot);
            var newPath = mover.MoveToQuarantine(m.Path);

            await db.UpsertDecisionAsync(m.MediaId, "quarantine", note: $"moved to: {newPath}");

            m.Decision = "quarantine";
            Status = $"Moved to quarantine: {newPath}";
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
    public async Task KeepBestInSelectedGroupAsync()
    {
        if (SelectedGroup is null) return;

        var best = SelectedGroup.Members.FirstOrDefault(x => x.IsBest);
        if (best is null) return;

        // mark best as keep, others as quarantine (just marking; user can click quarantine later)
        await MarkKeepAsync(best);

        foreach (var m in SelectedGroup.Members)
        {
            if (m == best) continue;
            m.Suggested = "quarantine";
        }

        Status = "Best marked KEEP, others suggested QUARANTINE.";
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
    public AppDb.MediaFileRow File { get; }

    public long MediaId => File.Id;
    public string Path => File.Path;
    public long SizeBytes => File.SizeBytes;
    public double? DurationSec => File.DurationSec;
    public int? Width => File.Width;
    public int? Height => File.Height;
    public string? VideoCodec => File.VideoCodec;

    public double AvgDist { get; }

    [ObservableProperty] private Bitmap? thumb20;
    [ObservableProperty] private Bitmap? thumb50;
    [ObservableProperty] private Bitmap? thumb80;
    [ObservableProperty] private bool thumbsLoading;
    [ObservableProperty] private bool isBest;
    [ObservableProperty] private string? decision;   // "keep" / "quarantine"
    [ObservableProperty] private string? suggested;  // "quarantine"


    public MemberVm(AppDb.MediaFileRow file, double avgDist)
    {
        File = file;
        AvgDist = avgDist;
    }
}
