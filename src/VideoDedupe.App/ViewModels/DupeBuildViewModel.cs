using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.App.Services;
using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>Detection settings and the run that groups duplicates.</summary>
public partial class DupeBuildViewModel : ObservableObject
{
    private readonly AppState _state;
    private readonly Stopwatch _stopwatch = new();
    private CancellationTokenSource? _cts;

    // Initial values come from AppSettings, whose defaults are those of DupeBuildOptions,
    // so UI, CLI and service agree.
    [ObservableProperty] private bool clearExisting;
    [ObservableProperty] private double tolSec;
    [ObservableProperty] private int minItems = 2;
    [ObservableProperty] private double minItemsDouble = 2;
    [ObservableProperty] private double maxAvgDist;
    [ObservableProperty] private string framesCsv = "";
    [ObservableProperty] private bool matchMirrored = true;
    [ObservableProperty] private bool useAudio = true;
    [ObservableProperty] private bool findTrimmed;
    [ObservableProperty] private bool findExcerpts;

    [ObservableProperty] private bool isBuilding;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(ProgressText))] private string phase = "";
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(Percent), nameof(ProgressText))] private int total;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(Percent), nameof(ProgressText))] private int done;
    [ObservableProperty] private string current = "";
    [ObservableProperty] private string eta = "";

    public double Percent => Total <= 0 ? 0 : (double)Done / Total;

    /// <summary>Phase names come from the service in English and are translated here.</summary>
    public string ProgressText => Loc.F("Finding duplicates · {0} · {1} of {2} videos", Loc.T(Phase), Done, Total);

    /// <summary>Raised after a successful build, so the groups can be reloaded.</summary>
    public event Func<Task>? Finished;

    public DupeBuildViewModel(AppState state, AppSettings settings)
    {
        _state = state;
        ClearExisting = settings.DupeClearExisting;
        TolSec = settings.DupeTolSec;
        MinItems = Math.Max(2, settings.DupeMinItems);
        MaxAvgDist = settings.DupeMaxAvgDist;
        FramesCsv = settings.DupeFramesCsv;
        MatchMirrored = settings.DupeMatchMirrored;
        UseAudio = settings.DupeUseAudio;
        FindTrimmed = settings.DupeFindTrimmed;
        FindExcerpts = settings.DupeFindExcerpts;
        state.DatabaseChanged += () => _cts?.Cancel();
        Loc.Instance.LanguageChanged += () => OnPropertyChanged(nameof(ProgressText));
    }

    public void SaveTo(AppSettings settings)
    {
        settings.DupeClearExisting = ClearExisting;
        settings.DupeTolSec = TolSec;
        settings.DupeMinItems = MinItems;
        settings.DupeMaxAvgDist = MaxAvgDist;
        settings.DupeFramesCsv = FramesCsv;
        settings.DupeMatchMirrored = MatchMirrored;
        settings.DupeUseAudio = UseAudio;
        settings.DupeFindTrimmed = FindTrimmed;
        settings.DupeFindExcerpts = FindExcerpts;
    }

    /// <summary>The detection positions, also used for the thumbnails.</summary>
    public IReadOnlyList<int> FramePositions => DupeBuildService.NormalizePositions((FramesCsv ?? "").Split(',')
        .Select(s => int.TryParse(s.Trim(), out var p) ? p : -1));

    partial void OnMinItemsDoubleChanged(double value)
    {
        MinItems = Math.Max(2, (int)Math.Round(value));
        MinItemsDouble = MinItems; // snaps to integer
    }

    partial void OnMinItemsChanged(int value) => MinItemsDouble = value;

    [RelayCommand]
    public void Cancel() => _cts?.Cancel();

    [RelayCommand]
    public void ResetToDefaults()
    {
        var defaults = new DupeBuildOptions();
        ClearExisting = defaults.ClearExisting;
        TolSec = defaults.TolSec;
        MinItems = defaults.MinItems;
        MaxAvgDist = defaults.MaxAvgDist;
        FramesCsv = string.Join(",", DupeBuildOptions.DefaultFramePercents);
        MatchMirrored = defaults.MatchMirrored;
        UseAudio = defaults.UseAudio;
        FindTrimmed = defaults.FindTrimmed;
        FindExcerpts = defaults.FindExcerpts;
    }

    [RelayCommand]
    public async Task RunAsync()
    {
        if (IsBuilding) return;
        var succeeded = false;
        try
        {
            await _state.FlushPendingAsync();
            using var _ = _state.Busy();
            IsBuilding = true;
            Phase = "Starting...";
            Total = Done = 0;
            Current = "";
            Eta = Loc.T("Estimating remaining time...");
            _stopwatch.Restart();

            _cts?.Cancel();
            _cts?.Dispose();
            _cts = new CancellationTokenSource();

            var db = await _state.OpenDbAsync();
            var svc = new DupeBuildService(db, new FfmpegFrameExtractor(new FfmpegTools()), new DHashService());
            var opt = new DupeBuildOptions(
                ClearExisting: ClearExisting,
                TolSec: TolSec,
                MinItems: MinItems,
                MaxAvgDist: MaxAvgDist,
                FramePercents: FramePositions,
                MatchMirrored: MatchMirrored,
                UseAudio: UseAudio,
                FindTrimmed: FindTrimmed,
                FindExcerpts: FindExcerpts);
            _state.Status = Loc.T("Finding duplicates...");

            var progress = new Progress<DupeBuildProgress>(p =>
            {
                Phase = p.Phase;
                if (p.OverallTotal > 0)
                {
                    Total = p.OverallTotal;
                    Done = p.OverallDone;
                    UpdateEta();
                }
                Current = p.Current ?? "";
            });

            var groups = await svc.RunAsync(opt, progress, _cts.Token);
            _state.Status = Loc.F("Search finished: {0} groups.", groups);
            succeeded = true;
        }
        catch (OperationCanceledException)
        {
            _state.Status = Loc.T("Search canceled.");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Search failed: {0}", ex.Message);
        }
        finally
        {
            _stopwatch.Stop();
            IsBuilding = false;
        }

        if (succeeded && Finished is { } finished)
            await finished();
    }

    private void UpdateEta()
    {
        if (Total <= 0 || Done <= 0 || _stopwatch.Elapsed < TimeSpan.FromSeconds(2))
        {
            Eta = Loc.T("Estimating remaining time...");
            return;
        }
        if (Done >= Total)
        {
            Eta = Loc.F("Completed in {0}", FormatDuration(_stopwatch.Elapsed));
            return;
        }
        var remaining = TimeSpan.FromTicks(_stopwatch.Elapsed.Ticks * (Total - Done) / Done);
        Eta = Loc.F("Approx. {0} remaining ({1} elapsed)", FormatDuration(remaining), FormatDuration(_stopwatch.Elapsed));
    }

    private static string FormatDuration(TimeSpan duration) => duration.TotalHours >= 1
        ? $"{(int)duration.TotalHours}:{duration.Minutes:D2}:{duration.Seconds:D2}"
        : $"{duration.Minutes}:{duration.Seconds:D2}";
}
