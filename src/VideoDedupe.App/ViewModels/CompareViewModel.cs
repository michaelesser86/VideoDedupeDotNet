using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Avalonia;
using Avalonia.Media.Imaging;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>Two copies at the same moment, full resolution, to judge which one looks better.</summary>
public partial class CompareViewModel : ObservableObject, IDisposable
{
    private readonly FfmpegFrameExtractor _extractor = new(new FfmpegTools());
    private CancellationTokenSource? _loadCts;

    public CompareSide Left { get; }
    public CompareSide Right { get; }

    public IReadOnlyList<Choice> ZoomOptions { get; } =
        [new("0", "Fit"), new("1", "100 %"), new("2", "200 %"), new("4", "400 %")];

    /// <summary>Position in percent of the left file.</summary>
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(PositionText))] private double position = 50;
    [ObservableProperty] private Choice zoom;
    [ObservableProperty] private bool isLoading;
    [ObservableProperty] private string error = "";

    /// <summary>Both scroll viewers bind to this, so zoomed views stay on the same spot.</summary>
    [ObservableProperty] private Vector offset;

    public CompareViewModel(MemberVm left, MemberVm right)
    {
        Left = new CompareSide(left);
        Right = new CompareSide(right);
        zoom = ZoomOptions[0];
    }

    public double ZoomFactor => double.Parse(Zoom.Id, CultureInfo.InvariantCulture);
    public bool IsZoomed => ZoomFactor > 0;

    public string PositionText => TimeText(LeftTime);

    private double LeftTime => (Left.Member.DurationSec ?? 0) * Position / 100;

    /// <summary>
    /// The moment in the right file that shows the same picture. Copies cut at the start are
    /// aligned at their end; everything else at the start or proportionally (same length).
    /// </summary>
    internal static double RightTime(double leftTime, double leftDuration, double rightDuration, bool alignEnd)
    {
        double t;
        if (Math.Abs(leftDuration - rightDuration) <= 1)
            t = leftDuration > 0 ? leftTime / leftDuration * rightDuration : 0;
        else
            t = alignEnd ? leftTime - (leftDuration - rightDuration) : leftTime;
        return Math.Clamp(t, 0.1, Math.Max(0.1, rightDuration - 0.1));
    }

    partial void OnPositionChanged(double value) => _ = LoadAsync(debounce: true);

    partial void OnZoomChanged(Choice value)
    {
        OnPropertyChanged(nameof(ZoomFactor));
        OnPropertyChanged(nameof(IsZoomed));
        Left.Zoom = Right.Zoom = ZoomFactor;
    }

    [RelayCommand]
    public void Step(string percent) =>
        Position = Math.Clamp(Position + double.Parse(percent, CultureInfo.InvariantCulture), 0, 100);

    public async Task LoadAsync(bool debounce = false)
    {
        _loadCts?.Cancel();
        var cts = _loadCts = new CancellationTokenSource();
        try
        {
            if (debounce)
                await Task.Delay(250, cts.Token);
            IsLoading = true;
            Error = "";
            var leftTime = Math.Clamp(LeftTime, 0.1, Math.Max(0.1, (Left.Member.DurationSec ?? 0) - 0.1));
            var alignEnd = Left.Member.Evidence.TrimKind == AppDb.TrimKinds.SameEnd
                           || Right.Member.Evidence.TrimKind == AppDb.TrimKinds.SameEnd;
            var rightTime = RightTime(leftTime, Left.Member.DurationSec ?? 0, Right.Member.DurationSec ?? 0, alignEnd);

            var leftTask = _extractor.ExtractFramePngAsync(Left.Member.Path, leftTime, cts.Token);
            var rightTask = _extractor.ExtractFramePngAsync(Right.Member.Path, rightTime, cts.Token);
            var (leftPng, rightPng) = (await leftTask, await rightTask);
            var leftBitmap = await Task.Run(() => new Bitmap(new MemoryStream(leftPng)), cts.Token);
            var rightBitmap = await Task.Run(() => new Bitmap(new MemoryStream(rightPng)), cts.Token);
            if (cts.IsCancellationRequested)
            {
                leftBitmap.Dispose();
                rightBitmap.Dispose();
                return;
            }
            Left.SetFrame(leftBitmap, leftTime);
            Right.SetFrame(rightBitmap, rightTime);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            Error = Loc.F("Could not read the frame: {0}", ex.Message);
        }
        finally
        {
            if (_loadCts == cts)
                IsLoading = false;
        }
    }

    internal static string TimeText(double seconds) =>
        TimeSpan.FromSeconds(Math.Max(0, seconds)).ToString(seconds >= 3600 ? @"h\:mm\:ss" : @"m\:ss", CultureInfo.InvariantCulture);

    public void Dispose()
    {
        _loadCts?.Cancel();
        Left.SetFrame(null, 0);
        Right.SetFrame(null, 0);
    }
}

/// <summary>One file of the comparison: its frame and how large to draw it.</summary>
public partial class CompareSide(MemberVm member) : ObservableObject
{
    public MemberVm Member { get; } = member;

    public string Title => Member.FileName;
    public string Details => $"{Member.ResolutionText} · {Member.SizeText} · {Member.BitrateText} · {Member.CodecText}";

    [ObservableProperty] private Bitmap? frame;
    [ObservableProperty] private string timeText = "";
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(ImageWidth), nameof(ImageHeight))] private double zoom;

    /// <summary>NaN lets the image fit the window; otherwise its pixel size times the zoom.</summary>
    public double ImageWidth => Zoom > 0 && Frame is { } f ? f.PixelSize.Width * Zoom : double.NaN;
    public double ImageHeight => Zoom > 0 && Frame is { } f ? f.PixelSize.Height * Zoom : double.NaN;

    public void SetFrame(Bitmap? bitmap, double seconds)
    {
        var old = Frame;
        Frame = bitmap;
        TimeText = bitmap is null ? "" : CompareViewModel.TimeText(seconds);
        OnPropertyChanged(nameof(ImageWidth));
        OnPropertyChanged(nameof(ImageHeight));
        old?.Dispose();
    }
}
