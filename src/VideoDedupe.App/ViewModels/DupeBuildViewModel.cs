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

    [ObservableProperty] private bool isBuilding;
    [ObservableProperty] private string phase = "";
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(Percent))] private int total;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(Percent))] private int done;
    [ObservableProperty] private string current = "";
    [ObservableProperty] private string eta = "Estimating remaining time...";

    public double Percent => Total <= 0 ? 0 : (double)Done / Total;

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
        state.DatabaseChanged += () => _cts?.Cancel();
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
    public async Task RunAsync()
    {
        if (IsBuilding) return;
        var succeeded = false;
        try
        {
            using var _ = _state.Busy();
            IsBuilding = true;
            Phase = "Starting...";
            Total = Done = 0;
            Current = "";
            Eta = "Estimating remaining time...";
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
                FindTrimmed: FindTrimmed);
            _state.Status =
                $"Dupe build with tol={opt.TolSec}, min={opt.MinItems}, maxDist={opt.MaxAvgDist}, frames=[{string.Join(",", opt.FramePercents!)}]";

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
            _state.Status = $"Dupe build finished: {groups} groups.";
            succeeded = true;
        }
        catch (OperationCanceledException)
        {
            _state.Status = "Dupe build canceled.";
        }
        catch (Exception ex)
        {
            _state.Status = $"Dupe build error: {ex.Message}";
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
            Eta = "Estimating remaining time...";
            return;
        }
        if (Done >= Total)
        {
            Eta = $"Completed in {FormatDuration(_stopwatch.Elapsed)}";
            return;
        }
        var remaining = TimeSpan.FromTicks(_stopwatch.Elapsed.Ticks * (Total - Done) / Done);
        Eta = $"Approx. {FormatDuration(remaining)} remaining ({FormatDuration(_stopwatch.Elapsed)} elapsed)";
    }

    private static string FormatDuration(TimeSpan duration) => duration.TotalHours >= 1
        ? $"{(int)duration.TotalHours}:{duration.Minutes:D2}:{duration.Seconds:D2}"
        : $"{duration.Minutes}:{duration.Seconds:D2}";
}
