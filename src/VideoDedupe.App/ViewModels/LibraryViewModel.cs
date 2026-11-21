using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>Scan folders and the index run that reads their videos.</summary>
public partial class LibraryViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _indexCts;

    public ObservableCollection<RootVm> Roots { get; } = new();

    [ObservableProperty] private RootVm? selectedRoot;
    [ObservableProperty] private string newRootPath = "";

    [ObservableProperty] private bool isIndexing;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(IndexPercent))] private int indexDiscovered;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(IndexPercent))] private int indexProcessed;
    [ObservableProperty] private int indexOk;
    [ObservableProperty] private int indexFail;
    [ObservableProperty] private string indexCurrent = "";

    public double IndexPercent => IndexDiscovered <= 0 ? 0 : (double)IndexProcessed / IndexDiscovered;

    public LibraryViewModel(AppState state)
    {
        _state = state;
        state.DatabaseChanged += () =>
        {
            _indexCts?.Cancel();
            Roots.Clear();
            SelectedRoot = null;
        };
    }

    [RelayCommand]
    public async Task LoadRootsAsync()
    {
        try
        {
            using var _ = _state.Busy();
            _state.Status = "Loading roots...";
            var db = await _state.OpenDbAsync();
            Roots.Clear();
            foreach (var r in await db.ListScanRootsAsync())
                Roots.Add(new RootVm
                {
                    Id = r.Id,
                    Path = r.Path,
                    IsEnabled = r.IsEnabled == 1,
                    IncludeSubdirs = r.IncludeSubdirs == 1,
                    ExcludeText = r.ExcludeText
                });
            SelectedRoot = Roots.FirstOrDefault();
            _state.Status = $"Loaded {Roots.Count} roots. DB={Path.GetFullPath(_state.DbPath)}";
        }
        catch (Exception ex)
        {
            _state.Status = $"Error: {ex.Message}";
        }
    }

    [RelayCommand]
    public async Task AddRootAsync()
    {
        try
        {
            var path = await DesktopShell.PickFolderAsync("Select folder to scan");
            if (string.IsNullOrWhiteSpace(path))
                return;
            await AddAsync(path);
        }
        catch (Exception ex)
        {
            _state.Status = $"Add root failed: {ex.Message}";
        }
    }

    [RelayCommand]
    public async Task AddRootFromTextAsync()
    {
        var path = (NewRootPath ?? "").Trim().Trim('"');
        if (string.IsNullOrWhiteSpace(path))
        {
            _state.Status = "Please enter a folder path.";
            return;
        }
        if (!Directory.Exists(path))
        {
            _state.Status = $"Folder does not exist: {path}";
            return;
        }
        try
        {
            await AddAsync(path);
            NewRootPath = "";
        }
        catch (Exception ex)
        {
            _state.Status = $"Add root failed: {ex.Message}";
        }
    }

    private async Task AddAsync(string path)
    {
        using (_state.Busy())
        {
            var db = await _state.OpenDbAsync();
            await db.AddScanRootAsync(path);
        }
        await LoadRootsAsync();
        _state.Status = $"Added root: {path}";
    }

    [RelayCommand]
    public async Task RemoveSelectedRootAsync()
    {
        if (SelectedRoot is null) return;
        try
        {
            using (_state.Busy())
            {
                var db = await _state.OpenDbAsync();
                await db.DeleteScanRootAsync(SelectedRoot.Id);
            }
            await LoadRootsAsync();
            _state.Status = "Root removed.";
        }
        catch (Exception ex)
        {
            _state.Status = $"Error: {ex.Message}";
        }
    }

    [RelayCommand]
    public async Task SaveRootEnabledAsync()
    {
        try
        {
            using var _ = _state.Busy();
            var db = await _state.OpenDbAsync();
            foreach (var r in Roots)
                await db.SetScanRootEnabledAsync(r.Id, r.IsEnabled);
            _state.Status = "Enabled flags saved.";
        }
        catch (Exception ex)
        {
            _state.Status = $"Error: {ex.Message}";
        }
    }

    [RelayCommand]
    public async Task SaveRootOptionsAsync()
    {
        if (SelectedRoot is null) return;
        try
        {
            using var _ = _state.Busy();
            var db = await _state.OpenDbAsync();
            await db.UpdateScanRootOptionsAsync(SelectedRoot.Id, SelectedRoot.IncludeSubdirs, SelectedRoot.ExcludeText);
            _state.Status = "Root options saved.";
        }
        catch (Exception ex)
        {
            _state.Status = $"Error: {ex.Message}";
        }
    }

    [RelayCommand]
    public void CancelScanIndex() => _indexCts?.Cancel();

    [RelayCommand]
    public async Task RunScanIndexAsync()
    {
        if (IsIndexing) return;
        try
        {
            using var _ = _state.Busy();
            IsIndexing = true;
            IndexDiscovered = IndexProcessed = IndexOk = IndexFail = 0;
            IndexCurrent = "";
            _state.Status = "Indexing...";

            _indexCts?.Cancel();
            _indexCts?.Dispose();
            _indexCts = new CancellationTokenSource();

            var db = await _state.OpenDbAsync();
            var svc = new IndexService(db, new FfprobeService(new FfmpegTools()));
            var progress = new Progress<IndexProgress>(p =>
            {
                IndexDiscovered = p.Discovered;
                IndexProcessed = p.Processed;
                IndexOk = p.ProbedOk;
                IndexFail = p.ProbedFail;
                IndexCurrent = p.Error is null ? p.CurrentPath ?? "" : $"{p.CurrentPath}: {p.Error}";
            });
            await svc.RunAsync(progress, _indexCts.Token);

            _state.Status = $"Index done. Files={IndexDiscovered}, ok={IndexOk}, fail={IndexFail}";
        }
        catch (OperationCanceledException)
        {
            _state.Status = "Index canceled.";
        }
        catch (Exception ex)
        {
            _state.Status = $"Index error: {ex.Message}";
        }
        finally
        {
            IsIndexing = false;
        }
    }
}
