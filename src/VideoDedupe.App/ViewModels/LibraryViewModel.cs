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
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(IndexPercent), nameof(ProgressText))] private int indexDiscovered;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(IndexPercent), nameof(ProgressText))] private int indexProcessed;
    [ObservableProperty] private int indexOk;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(ProgressText))] private int indexFail;
    [ObservableProperty] private string indexCurrent = "";

    public double IndexPercent => IndexDiscovered <= 0 ? 0 : (double)IndexProcessed / IndexDiscovered;

    public string ProgressText => Loc.F("Scanning library · {0} of {1} videos · {2} unreadable",
        IndexProcessed, IndexDiscovered, IndexFail);

    public string TabTitle => Loc.F("Folders ({0})", Roots.Count);

    public LibraryViewModel(AppState state)
    {
        _state = state;
        Roots.CollectionChanged += (_, _) => OnPropertyChanged(nameof(TabTitle));
        Loc.Instance.LanguageChanged += () =>
        {
            OnPropertyChanged(nameof(TabTitle));
            OnPropertyChanged(nameof(ProgressText));
        };
        state.DatabaseChanged += () =>
        {
            _indexCts?.Cancel();
            Roots.Clear();
            SelectedRoot = null;
            HasRoots = false;
        };
    }

    [RelayCommand]
    public async Task LoadRootsAsync()
    {
        try
        {
            using var busy = _state.Busy();
            _state.Status = Loc.T("Loading folders...");
            var db = await _state.OpenDbAsync();
            Roots.Clear();
            foreach (var r in await db.ListScanRootsAsync())
                Roots.Add(new RootVm
                {
                    Id = r.Id,
                    Path = r.Path,
                    IsEnabled = r.IsEnabled == 1,
                    IncludeSubdirs = r.IncludeSubdirs == 1,
                    ExcludeText = r.ExcludeText,
                    Priority = RootVm.PriorityOption(r.Priority),
                    // Set last: the values above are loaded, not edited.
                    Changed = root => _ = SaveRootAsync(root)
                });
            SelectedRoot = Roots.FirstOrDefault();
            HasRoots = Roots.Count > 0;
            PrioritiesChanged?.Invoke();
            _state.Status = $"{Roots.Count} folders.";
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task AddRootAsync()
    {
        try
        {
            var path = await DesktopShell.PickFolderAsync(Loc.T("Select folder to scan"));
            if (string.IsNullOrWhiteSpace(path))
                return;
            await AddAsync(path);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Adding the folder failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task AddRootFromTextAsync()
    {
        var path = (NewRootPath ?? "").Trim().Trim('"');
        if (string.IsNullOrWhiteSpace(path))
        {
            _state.Status = Loc.T("Please enter a folder path.");
            return;
        }
        if (!Directory.Exists(path))
        {
            _state.Status = Loc.F("Folder does not exist: {0}", path);
            return;
        }
        try
        {
            await AddAsync(path);
            NewRootPath = "";
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Adding the folder failed: {0}", ex.Message);
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
        _state.Status = Loc.F("Added folder: {0}", path);
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
            _state.Status = Loc.T("Folder removed from the library (files stay where they are).");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    [ObservableProperty] private bool hasRoots;

    /// <summary>A folder's priority changed, or folders were added or removed.</summary>
    public event Action? PrioritiesChanged;

    // Saved on every change, so nothing is lost when the user forgets a save button.
    private async Task SaveRootAsync(RootVm root)
    {
        try
        {
            var db = await _state.OpenDbAsync();
            await db.SetScanRootEnabledAsync(root.Id, root.IsEnabled);
            await db.UpdateScanRootOptionsAsync(root.Id, root.IncludeSubdirs, root.ExcludeText);
            await db.SetScanRootPriorityAsync(root.Id, root.PriorityValue);
            PrioritiesChanged?.Invoke();
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Folder settings not saved: {0}", ex.Message);
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
            await _state.FlushPendingAsync();
            using var _ = _state.Busy();
            IsIndexing = true;
            IndexDiscovered = IndexProcessed = IndexOk = IndexFail = 0;
            IndexCurrent = "";
            _state.Status = Loc.T("Scanning...");

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

            _state.Status = Loc.F("Scan finished: {0} videos, {1} could not be read.", IndexDiscovered, IndexFail);
        }
        catch (OperationCanceledException)
        {
            _state.Status = Loc.T("Scan canceled.");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Scan failed: {0}", ex.Message);
        }
        finally
        {
            IsIndexing = false;
        }
    }
}
