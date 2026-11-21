using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
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
    [ObservableProperty] private string dbPath = Path.Combine(AppContext.BaseDirectory, "videodedupe.db"); [ObservableProperty] private bool isBusy;
    [ObservableProperty] private string status = "";
    [ObservableProperty] private string groupFilter = "";
    [ObservableProperty] private string groupSort = "Largest files";
    public IReadOnlyList<string> GroupSortOptions { get; } = ["Largest files", "Newest"];
    public ObservableCollection<GroupVm> Groups { get; } = new();
    public ObservableCollection<GroupVm> FilteredGroups { get; } = new();
    public ObservableCollection<RootVm> Roots { get; } = new();

    [ObservableProperty] private RootVm? selectedRoot;
    [ObservableProperty] private GroupVm? selectedGroup;

    [ObservableProperty] private string quarantineRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "VideoDedupe_Quarantine");

    [ObservableProperty] private string newRootPath = "";
    private CancellationTokenSource? _thumbCts;

    private readonly FfmpegTools _tools = new();
    private readonly FfmpegFrameExtractor _extractor;
    private const int ThumbWidth = 320;
    private const int ThumbCacheLimit = 300;
    private readonly Dictionary<(string Database, long MediaId, string ModifiedUtc, int Pos), Bitmap> _thumbCache = new();
    private readonly Queue<(string Database, long MediaId, string ModifiedUtc, int Pos)> _thumbOrder = new();
    private int _busyOperations;
    [ObservableProperty] private bool isIndexing;
    [ObservableProperty] private int indexDiscovered;
    [ObservableProperty] private int indexProcessed;
    [ObservableProperty] private int indexOk;
    [ObservableProperty] private int indexFail;
    [ObservableProperty] private string indexCurrent = "";
    [ObservableProperty] private bool isDupeBuilding;
    [ObservableProperty] private string dupePhase = "";
    [ObservableProperty] private int dupeTotal;
    [ObservableProperty] private int dupeDone;
    [ObservableProperty] private string dupeCurrent = "";
    [ObservableProperty] private string dupeEta = "Estimating remaining time...";
    private readonly Stopwatch _dupeStopwatch = new();
    // Dupe-build settings (UI)
    [ObservableProperty] private bool dupeClearExisting = true;
    [ObservableProperty] private double dupeTolSec = 0.5;
    [ObservableProperty] private int dupeMinItems = 2;
    [ObservableProperty] private double dupeMaxAvgDist = 12.0;
    [ObservableProperty] private string dupeFramesCsv = "20,50,80";

    public double DupePercent => DupeTotal <= 0 ? 0 : (double)DupeDone / DupeTotal;

    private CancellationTokenSource? _dupeCts;

    partial void OnDupeTotalChanged(int value) => OnPropertyChanged(nameof(DupePercent));
    partial void OnDupeDoneChanged(int value) => OnPropertyChanged(nameof(DupePercent));

    public double IndexPercent => IndexDiscovered <= 0 ? 0 : (double)IndexProcessed / IndexDiscovered;

    private CancellationTokenSource? _indexCts;
    partial void OnGroupFilterChanged(string value) => RefreshGroupView();
    partial void OnGroupSortChanged(string value) => RefreshGroupView();

    partial void OnIndexDiscoveredChanged(int value) => OnPropertyChanged(nameof(IndexPercent));
    partial void OnIndexProcessedChanged(int value) => OnPropertyChanged(nameof(IndexPercent));
    partial void OnDbPathChanged(string value)
    {
        _indexCts?.Cancel();
        _dupeCts?.Cancel();
        Roots.Clear();
        Groups.Clear();
        FilteredGroups.Clear();
        SelectedRoot = null;
        SelectedGroup = null;
        CancelAndClearThumbnails();
        Status = "Database changed. Load roots or groups for the selected database.";
    }

    private void EnterBusy()
    {
        _busyOperations++;
        IsBusy = true;
    }

    private void ExitBusy()
    {
        _busyOperations = Math.Max(0, _busyOperations - 1);
        IsBusy = _busyOperations > 0;
    }

    private void CancelAndClearThumbnails()
    {
        _thumbCts?.Cancel();
        _thumbCts?.Dispose();
        _thumbCts = null;
        // Callers clear Groups/SelectedGroup, so no member keeps showing these bitmaps.
        foreach (var bitmap in _thumbCache.Values)
            bitmap.Dispose();
        _thumbCache.Clear();
        _thumbOrder.Clear();
    }
    public MainViewModel()
    {
        _extractor = new FfmpegFrameExtractor(_tools);
    }

    partial void OnSelectedGroupChanged(GroupVm? oldValue, GroupVm? newValue)
    {
        // Cancel previous thumbnail load
        _thumbCts?.Cancel();
        _thumbCts?.Dispose();
        _thumbCts = null;

        // Only the selected group references cached bitmaps, so eviction can never
        // dispose a bitmap that is still on screen. Re-selecting reloads from the cache.
        if (oldValue is not null)
            foreach (var member in oldValue.Members)
                member.Thumb20 = member.Thumb50 = member.Thumb80 = null;

        if (newValue is null)
            return;

        // Debounce: in case user scrolls quickly
        _thumbCts = new CancellationTokenSource();
        _ = LoadThumbnailsAfterDelayAsync(_thumbCts.Token);
    }

    // Runs on the UI thread: the cache, Status and IsBusy are only touched there.
    // ffmpeg and bitmap decoding are awaited without blocking it.
    private async Task LoadThumbnailsAfterDelayAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(200, ct);
            await LoadThumbnailsInternalAsync(ct);
        }
        catch (OperationCanceledException) { }
    }

    private List<int> ParseFrames(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv))
            return new List<int> { 20, 50, 80 };

        var parsed = csv.Split(',')
            .Select(s => s.Trim())
            .Where(s => int.TryParse(s, out _))
            .Select(int.Parse)
            .Where(p => p > 0 && p < 100)
            .Distinct()
            .OrderBy(p => p)
            .ToList();

        return parsed.Count > 0 ? parsed : new List<int> { 20, 50, 80 };
    }

    [RelayCommand]
    public async Task AddRootFromTextAsync()
    {
        try
        {
            EnterBusy();
            var path = (NewRootPath ?? "").Trim().Trim('"');

            if (string.IsNullOrWhiteSpace(path))
            {
                Status = "Please enter a folder path.";
                return;
            }

            if (!Directory.Exists(path))
            {
                Status = $"Folder does not exist: {path}";
                return;
            }

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            await db.AddScanRootAsync(path);

            Status = $"Added root: {path}";
            NewRootPath = "";

            await LoadRootsAsync();
        }
        catch (Exception ex)
        {
            Status = $"AddRootFromText failed: {ex.Message}";
        }
        finally
        {
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task DeleteFileAsync(MemberVm? m)
    {
        if (m is null) return;

        try
        {
            EnterBusy();
            if (string.IsNullOrWhiteSpace(m.Path))
            {
                Status = "File path missing.";
                return;
            }

            // 1) Datei löschen (Windows -> Recycle Bin)
            if (File.Exists(m.Path))
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    Microsoft.VisualBasic.FileIO.FileSystem.DeleteFile(
                        m.Path,
                        Microsoft.VisualBasic.FileIO.UIOption.OnlyErrorDialogs,
                        Microsoft.VisualBasic.FileIO.RecycleOption.SendToRecycleBin);
                }
                else
                {
                    File.Delete(m.Path);
                }
            }

            // 2) In DB markieren (wichtig, sonst bleibt die Datei "aktiv" in späteren Läufen)
            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);
            await db.MarkMediaFileDeletedAsync(m.MediaFileId);

            Status = "Deleted and marked in DB.";

            // Refresh every group: the same file can occur in more than one group.
            await LoadGroupsAsync();
        }
        catch (Exception ex)
        {
            Status = $"Delete failed: {ex.Message}";
        }
        finally
        {
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task PickDbAsync()
    {
        try
        {
            EnterBusy();
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
        finally
        {
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task LoadGroupsAsync()
    {
        try
        {
            EnterBusy();
            Status = "Loading groups...";

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            var groups = await db.ListDuplicateGroupsWithMembersAsync();
            var decisions = await db.GetDecisionsAsync(
                groups.SelectMany(g => g.Members).Select(member => member.File.Id));

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
                    gvm.Members.Add(new MemberVm(file, avgDist)
                    {
                        Decision = decisions.GetValueOrDefault(file.Id)
                    });

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
            ExitBusy();
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
    private async Task RunCliAsync(string args)
    {
        // args example: "scan-index --db=videodedupe.db"
        var psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project \"{Path.GetFullPath("src/VideoDedupe.Cli")}\" -- {args}",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        using var p = Process.Start(psi)!;

        // simple: read all
        var stdout = await p.StandardOutput.ReadToEndAsync();
        var stderr = await p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync();

        if (p.ExitCode != 0)
            throw new Exception($"CLI failed: {stderr}");

        // show last line
        var last = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
        Status = last ?? "Done.";
    }

    [RelayCommand]
    public void CancelScanIndex()
    {
        _indexCts?.Cancel();
    }

    [RelayCommand]
    public async Task RunScanIndexAsync()
    {
        if (IsIndexing) return;

        try
        {
            EnterBusy();
            IsIndexing = true;

            IndexDiscovered = 0;
            IndexProcessed = 0;
            IndexOk = 0;
            IndexFail = 0;
            IndexCurrent = "";
            Status = "Indexing...";

            _indexCts?.Cancel();
            _indexCts?.Dispose();
            _indexCts = new CancellationTokenSource();
            var ct = _indexCts.Token;

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            var tools = new FfmpegTools();
            var probe = new FfprobeService(tools);
            var svc = new IndexService(db, probe);

            var prog = new Progress<IndexProgress>(p =>
            {
                IndexDiscovered = p.Discovered;
                IndexProcessed = p.Processed;
                IndexOk = p.ProbedOk;
                IndexFail = p.ProbedFail;
                IndexCurrent = p.Error is null ? p.CurrentPath ?? "" : $"{p.CurrentPath}: {p.Error}";
            });

            await svc.RunAsync(prog, ct);

            Status = $"Index done. Files={IndexDiscovered}, ok={IndexOk}, fail={IndexFail}";
            await LoadRootsAsync(); // optional refresh
        }
        catch (OperationCanceledException)
        {
            Status = "Index canceled.";
        }
        catch (Exception ex)
        {
            Status = $"Index error: {ex.Message}";
        }
        finally
        {
            IsIndexing = false;
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task SaveRootEnabledAsync()
    {
        try
        {
            EnterBusy();

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            foreach (var r in Roots)
                await db.SetScanRootEnabledAsync(r.Id, r.IsEnabled);

            Status = "Enabled flags saved.";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
        finally
        {
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task RemoveSelectedRootAsync()
    {
        if (SelectedRoot is null) return;

        try
        {
            EnterBusy();

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            await db.DeleteScanRootAsync(SelectedRoot.Id);

            await LoadRootsAsync();
            Status = "Root removed.";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
        finally
        {
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task RunDupeBuildAsync()
    {
        if (IsDupeBuilding) return;

        try
        {
            EnterBusy();
            IsDupeBuilding = true;

            DupePhase = "Starting...";
            DupeTotal = 0;
            DupeDone = 0;
            DupeCurrent = "";
            DupeEta = "Estimating remaining time...";
            _dupeStopwatch.Restart();
            Status = "Dupe build running...";

            _dupeCts?.Cancel();
            _dupeCts?.Dispose();
            _dupeCts = new CancellationTokenSource();
            var ct = _dupeCts.Token;

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            var tools = new FfmpegTools();
            var extractor = new FfmpegFrameExtractor(tools);
            var dhash = new VideoDedupe.Core.DHashService();

            var svc = new DupeBuildService(db, extractor, dhash);

            var frames = ParseFrames(DupeFramesCsv);

            var opt = new DupeBuildOptions(
                ClearExisting: DupeClearExisting,
                TolSec: DupeTolSec,
                MinItems: DupeMinItems,
                MaxAvgDist: DupeMaxAvgDist,
                FramePercents: frames
            );

            Status =
    $"DupeBuild with tol={opt.TolSec}, min={opt.MinItems}, maxDist={opt.MaxAvgDist}, frames=[{string.Join(",", opt.FramePercents!)}]";

            var prog = new Progress<DupeBuildProgress>(p =>
            {
                DupePhase = p.Phase;
                if (p.OverallTotal > 0)
                {
                    DupeTotal = p.OverallTotal;
                    DupeDone = p.OverallDone;
                    UpdateDupeEta();
                }
                DupeCurrent = p.Current ?? "";
            });

            await svc.RunAsync(opt, prog, ct);

            Status = "Dupe build finished.";
            await LoadGroupsAsync();
        }
        catch (OperationCanceledException)
        {
            Status = "Dupe build canceled.";
        }
        catch (Exception ex)
        {
            Status = $"Dupe build error: {ex.Message}";
        }
        finally
        {
            _dupeStopwatch.Stop();
            IsDupeBuilding = false;
            ExitBusy();
        }
    }

    private void UpdateDupeEta()
    {
        if (DupeTotal <= 0 || DupeDone <= 0 || _dupeStopwatch.Elapsed < TimeSpan.FromSeconds(2))
        {
            DupeEta = "Estimating remaining time...";
            return;
        }

        if (DupeDone >= DupeTotal)
        {
            DupeEta = $"Completed in {FormatDuration(_dupeStopwatch.Elapsed)}";
            return;
        }

        var remaining = TimeSpan.FromTicks(_dupeStopwatch.Elapsed.Ticks * (DupeTotal - DupeDone) / DupeDone);
        DupeEta = $"Approx. {FormatDuration(remaining)} remaining ({FormatDuration(_dupeStopwatch.Elapsed)} elapsed)";
    }

    private static string FormatDuration(TimeSpan duration) => duration.TotalHours >= 1
        ? $"{(int)duration.TotalHours}:{duration.Minutes:D2}:{duration.Seconds:D2}"
        : $"{duration.Minutes}:{duration.Seconds:D2}";
    [RelayCommand]
    public void CancelDupeBuild()
    {
        _dupeCts?.Cancel();
    }
    [RelayCommand]
    public async Task SaveRootOptionsAsync()
    {
        if (SelectedRoot is null) return;

        try
        {
            EnterBusy();
            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            await db.UpdateScanRootOptionsAsync(
                SelectedRoot.Id,
                SelectedRoot.IncludeSubdirs,
                SelectedRoot.ExcludeText);

            Status = "Root options saved.";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
        finally
        {
            ExitBusy();
        }
    }

    [RelayCommand]
    public async Task AddRootAsync()
    {
        try
        {
            EnterBusy();
            Status = "AddRoot: starting...";

            var lifetime = Application.Current?.ApplicationLifetime as IClassicDesktopStyleApplicationLifetime;
            if (lifetime?.MainWindow == null)
            {
                Status = "AddRoot: MainWindow is null.";
                return;
            }

            var topLevel = TopLevel.GetTopLevel(lifetime.MainWindow);
            if (topLevel == null)
            {
                Status = "AddRoot: TopLevel is null.";
                return;
            }

            Status = "AddRoot: opening folder picker...";

            var result = await topLevel.StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
            {
                Title = "Select folder to scan",
                AllowMultiple = false
            });

            Status = $"AddRoot: picker returned {result.Count} item(s).";

            if (result.Count == 0)
                return;

            var item = result[0];

            // show raw values
            var rawPath = item.Path.ToString();
            var localPath = item.TryGetLocalPath();

            Status = $"AddRoot: raw={rawPath} local={localPath}";

            if (string.IsNullOrWhiteSpace(localPath))
                return;

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            await db.AddScanRootAsync(localPath);

            Status = $"AddRoot: added to DB. Reloading roots...";
            await LoadRootsAsync();
            Status = $"AddRoot: done. Roots={Roots.Count}";
        }
        catch (Exception ex)
        {
            Status = $"AddRoot failed: {ex}";
        }
        finally
        {
            ExitBusy();
        }
    }
    [RelayCommand]
    public async Task LoadRootsAsync()
    {
        try
        {
            EnterBusy();
            Status = "Loading roots...";

            var db = new AppDb(DbPath);
            await DbInitializer.EnsureCreatedAsync(db);

            var roots = await db.ListScanRootsAsync();

            Roots.Clear();
            foreach (var r in roots)
            {
                Roots.Add(new RootVm
                {
                    Id = r.Id,
                    Path = r.Path,
                    IsEnabled = r.IsEnabled == 1,
                    IncludeSubdirs = r.IncludeSubdirs == 1,
                    ExcludeText = r.ExcludeText
                });
            }

            SelectedRoot = Roots.FirstOrDefault();
            Status = $"Loaded {Roots.Count} roots. DB={Path.GetFullPath(DbPath)}";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
        finally
        {
            ExitBusy();
        }
    }
    private async Task LoadThumbnailsInternalAsync(CancellationToken ct)
    {
        if (SelectedGroup is null) return;

        try
        {
            EnterBusy();
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
            ExitBusy();
        }
    }

    private async Task LoadThumbsForMemberAsync(MemberVm m, int[] positions, CancellationToken ct)
    {
        // basic guards
        if (m.DurationSec is null || m.DurationSec <= 1.0) return;
        if (string.IsNullOrWhiteSpace(m.Path) || !File.Exists(m.Path)) return;

        m.ThumbsLoading = true;

        try
        {
            foreach (var pos in positions)
            {
                ct.ThrowIfCancellationRequested();

                var key = (Path.GetFullPath(DbPath), m.MediaId, m.File.ModifiedUtc, pos);

                if (_thumbCache.TryGetValue(key, out var cached))
                {
                    SetThumb(m, pos, cached);
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
                    png = await _extractor.ExtractFramePngAsync(m.Path, ts, ct, ThumbWidth);
                }
                catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
                {
                    System.Diagnostics.Trace.TraceWarning($"Thumbnail failed: {m.Path}: {ex}");
                    continue;
                }

                // Decode scaled down: a full 4K frame would cost ~33 MB per cached thumbnail.
                var bmp = await Task.Run(() =>
                {
                    using var ms = new MemoryStream(png);
                    return Bitmap.DecodeToWidth(ms, ThumbWidth);
                }, ct);

                // A concurrent load for the same member may have filled the slot meanwhile.
                if (!_thumbCache.TryAdd(key, bmp))
                {
                    bmp.Dispose();
                    bmp = _thumbCache[key];
                }
                else
                {
                    _thumbOrder.Enqueue(key);
                    EvictThumbnails();
                }

                // The group may have been deselected while decoding; it must not hold bitmaps.
                ct.ThrowIfCancellationRequested();
                SetThumb(m, pos, bmp);
            }
        }
        finally
        {
            m.ThumbsLoading = false;
        }
    }

    private void EvictThumbnails()
    {
        var shown = SelectedGroup?.Members
            .SelectMany(member => new[] { member.Thumb20, member.Thumb50, member.Thumb80 })
            .OfType<Bitmap>()
            .ToHashSet() ?? [];

        // Oldest first; bitmaps on screen are re-queued instead of disposed.
        for (var checkedCount = _thumbOrder.Count; _thumbCache.Count > ThumbCacheLimit && checkedCount > 0; checkedCount--)
        {
            var key = _thumbOrder.Dequeue();
            if (!_thumbCache.TryGetValue(key, out var bitmap))
                continue;
            if (shown.Contains(bitmap))
            {
                _thumbOrder.Enqueue(key);
                continue;
            }
            _thumbCache.Remove(key);
            bitmap.Dispose();
        }
    }

    private void RefreshGroupView()
    {
        IEnumerable<GroupVm> q = Groups;

        // sort
        q = GroupSort switch
        {
            "Newest" => q.OrderByDescending(g => g.GroupId),
            _ => q.OrderByDescending(g => g.Members.Sum(m => m.SizeBytes))
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
            EnterBusy();
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

            await db.MarkMediaFileQuarantinedAsync(m.MediaId, newPath);

            // Refresh every group: the same file can occur in more than one group.
            await LoadGroupsAsync();
            Status = $"Moved to quarantine: {newPath}";
        }
        catch (Exception ex)
        {
            Status = $"Error: {ex.Message}";
        }
        finally
        {
            ExitBusy();
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
    [RelayCommand]
    public void OpenFile(MemberVm? m)
    {
        if (m is null) return;

        try
        {
            if (!File.Exists(m.Path))
            {
                Status = "File not found.";
                return;
            }

            Process.Start(new ProcessStartInfo
            {
                FileName = m.Path,
                UseShellExecute = true
            });

            Status = "Opened file.";
        }
        catch (Exception ex)
        {
            Status = $"Open failed: {ex.Message}";
        }
    }

    [RelayCommand]
    public void ShowInExplorer(MemberVm? m)
    {
        if (m is null) return;

        try
        {
            if (!File.Exists(m.Path))
            {
                Status = "File not found.";
                return;
            }

            // Windows: highlight file in Explorer
            Process.Start(new ProcessStartInfo
            {
                FileName = "explorer.exe",
                Arguments = $"/select,\"{m.Path}\"",
                UseShellExecute = true
            });

            Status = "Opened Explorer.";
        }
        catch (Exception ex)
        {
            Status = $"Explorer failed: {ex.Message}";
        }
    }
    [RelayCommand]
    public async Task CopyPathAsync(MemberVm? m)
    {
        if (m is null) return;

        try
        {
            var lifetime = Application.Current?.ApplicationLifetime as IClassicDesktopStyleApplicationLifetime;
            var topLevel = lifetime?.MainWindow is null ? null : TopLevel.GetTopLevel(lifetime.MainWindow);

            if (topLevel?.Clipboard is null)
            {
                Status = "Clipboard not available.";
                return;
            }

            await topLevel.Clipboard.SetTextAsync(m.Path);
            Status = "Path copied to clipboard.";
        }
        catch (Exception ex)
        {
            Status = $"Copy failed: {ex.Message}";
        }
    }
    [ObservableProperty] private double dupeMinItemsDouble = 2;

    partial void OnDupeMinItemsDoubleChanged(double value)
    {
        DupeMinItems = Math.Max(2, (int)Math.Round(value));
        DupeMinItemsDouble = DupeMinItems; // snaps to integer
    }
    partial void OnDupeMinItemsChanged(int value) => DupeMinItemsDouble = value;
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
    public long MediaFileId => File.Id;
    public bool IsMissing => string.IsNullOrWhiteSpace(File.Path) || !System.IO.File.Exists(File.Path);

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
        File = file ?? throw new ArgumentNullException(nameof(file));
        AvgDist = avgDist;
    }

    public string SizeMbText
    {
        get
        {
            if (File.SizeBytes <= 0) return "-";
            var mb = File.SizeBytes / (1024.0 * 1024.0);
            return $"{mb:F2} MB";
        }
    }

    public string CreatedLocalText
    {
        get
        {
            if (string.IsNullOrWhiteSpace(File.ModifiedUtc))
                return "-";

            if (!DateTime.TryParse(File.ModifiedUtc, out var utc))
                return File.ModifiedUtc;

            var local = utc.ToLocalTime();
            return local.ToString("dd.MM.yyyy HH:mm");
        }
    }
}
public partial class RootVm : ObservableObject
{
    public long Id { get; init; }
    public string Path { get; init; } = "";

    [ObservableProperty] private bool isEnabled;
    [ObservableProperty] private bool includeSubdirs = true;
    [ObservableProperty] private string? excludeText;
}
