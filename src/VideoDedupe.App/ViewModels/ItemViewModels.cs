using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;

using Avalonia.Media.Imaging;

using CommunityToolkit.Mvvm.ComponentModel;

using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

public partial class GroupVm : ObservableObject
{
    public required GroupsViewModel Owner { get; init; }
    public long GroupId { get; init; }
    public double MaxAvgDist { get; init; }
    public string CreatedUtc { get; init; } = "";

    public ObservableCollection<MemberVm> Members { get; } = new();

    [ObservableProperty] private string bestReason = "";

    public string Title => $"#{GroupId} · {Members.Count} files";

    /// <summary>Space freed by keeping only the best copy.</summary>
    public long ReclaimableBytes => Members.Where(m => !m.IsBest).Sum(m => m.SizeBytes);

    public string ReclaimableText => $"saves {FormatSize(ReclaimableBytes)}";

    public string BestFileName => Members.FirstOrDefault(m => m.IsBest)?.FileName ?? "";

    /// <summary>Picks the copy to keep (files marked keep win) and refreshes the summary.</summary>
    public void UpdateBest()
    {
        foreach (var m in Members)
        {
            m.IsBest = false;
            m.BestReason = "";
        }
        if (Members.Count > 0)
        {
            var decisions = Members.Where(m => m.Decision is not null).ToDictionary(m => m.MediaId, m => m.Decision!);
            var choice = BestFileSelector.Choose(Members.Select(m => m.File).ToList(), decisions);
            var best = Members.First(m => m.MediaId == choice.File.Id);
            best.IsBest = true;
            best.BestReason = choice.Reason;
            BestReason = choice.Reason;
        }
        OnPropertyChanged(nameof(Title));
        OnPropertyChanged(nameof(ReclaimableBytes));
        OnPropertyChanged(nameof(ReclaimableText));
        OnPropertyChanged(nameof(BestFileName));
    }

    public static string FormatSize(long bytes) => bytes switch
    {
        >= 1L << 30 => $"{bytes / (double)(1L << 30):F2} GB",
        >= 1L << 20 => $"{bytes / (double)(1L << 20):F1} MB",
        _ => $"{bytes / 1024.0:F0} KB"
    };
}

public partial class MemberVm : ObservableObject
{
    public GroupVm Group { get; }
    public AppDb.MediaFileRow File { get; }
    public AppDb.GroupMember Evidence { get; }

    /// <summary>The panel whose commands the member's buttons run.</summary>
    public GroupsViewModel Owner => Group.Owner;

    public long MediaId => File.Id;
    public string Path => File.Path;
    public string FileName => System.IO.Path.GetFileName(File.Path);
    public long SizeBytes => File.SizeBytes;
    public double? DurationSec => File.DurationSec;
    public bool IsMissing => string.IsNullOrWhiteSpace(File.Path) || !System.IO.File.Exists(File.Path);

    [ObservableProperty] private Bitmap? thumb20;
    [ObservableProperty] private Bitmap? thumb50;
    [ObservableProperty] private Bitmap? thumb80;
    [ObservableProperty] private bool thumbsLoading;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(BestText))] private bool isBest;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(BestText))] private string bestReason = "";
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(IsKept))] private string? decision;

    public MemberVm(GroupVm group, AppDb.GroupMember evidence)
    {
        Group = group;
        Evidence = evidence;
        File = evidence.File;
    }

    public bool IsKept => Decision == BestFileSelector.KeepDecision;

    public string BestText => IsBest ? $"BEST · {BestReason}" : "";

    /// <summary>
    /// 100 % for identical frames, 0 % at 32 differing bits, which is what unrelated
    /// pictures typically reach.
    /// </summary>
    public double PictureSimilarity => Math.Clamp(1 - Evidence.AvgDist / 32, 0, 1);

    /// <summary>Why this file is in the group, in plain words.</summary>
    public string MatchText
    {
        get
        {
            if (Evidence.IsReference)
                return "Reference: the other files were compared with this one";
            var parts = new List<string> { $"Picture {PictureSimilarity:P0} similar" };
            if (Evidence.Mirrored)
                parts.Add("mirrored");
            if (Evidence.AvgDist > Group.MaxAvgDist)
            {
                // Audio is only compared when the scene check did not confirm the match.
                parts.Add(Evidence.AudioBer is double ber
                    ? $"confirmed by the soundtrack ({1 - ber:P0} match)"
                    : "confirmed by comparing scenes");
            }
            return string.Join(" · ", parts);
        }
    }

    public string ResolutionText => File.Width is int w && File.Height is int h ? $"{w}×{h}" : "?×?";

    public string DurationText => File.DurationSec is double d
        ? TimeSpan.FromSeconds(d).ToString(d >= 3600 ? @"h\:mm\:ss" : @"m\:ss", CultureInfo.InvariantCulture)
        : "-";

    public string SizeText => File.SizeBytes > 0 ? GroupVm.FormatSize(File.SizeBytes) : "-";

    public string BitrateText => BestFileSelector.BitrateMbps(File) is double mbps ? $"{mbps:F1} Mbit/s" : "-";

    public string CodecText => File.VideoCodec ?? "-";

    public string ModifiedText =>
        DateTime.TryParse(File.ModifiedUtc, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var utc)
            ? utc.ToLocalTime().ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture)
            : "-";
}

public partial class RootVm : ObservableObject
{
    public long Id { get; init; }
    public string Path { get; init; } = "";

    [ObservableProperty] private bool isEnabled;
    [ObservableProperty] private bool includeSubdirs = true;
    [ObservableProperty] private string? excludeText;
}
