using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;

using Avalonia.Media.Imaging;

using CommunityToolkit.Mvvm.ComponentModel;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

public partial class GroupVm : ObservableObject
{
    public required GroupsViewModel Owner { get; init; }
    public long GroupId { get; init; }
    public double MaxAvgDist { get; init; }
    public string CreatedUtc { get; init; } = "";

    public ObservableCollection<MemberVm> Members { get; } = new();

    private BestFileSelector.Choice? _best;

    public string CountText => Loc.F("{0} files · #{1}", Members.Count, GroupId);

    public string ReclaimableShort => FormatSize(ReclaimableBytes);

    /// <summary>Space freed by keeping only the best copy.</summary>
    public long ReclaimableBytes => Members.Where(m => !m.IsBest).Sum(m => m.SizeBytes);

    public string ReclaimableText => Loc.F("saves {0}", FormatSize(ReclaimableBytes));

    public string BestFileName => Members.FirstOrDefault(m => m.IsBest)?.FileName ?? "";

    /// <summary>Picks the copy to keep (files marked keep win) and refreshes the summary.</summary>
    public void UpdateBest()
    {
        foreach (var m in Members)
        {
            m.IsBest = false;
            m.BestReason = "";
        }
        _best = null;
        if (Members.Count > 0)
        {
            var decisions = Members.Where(m => m.Decision is not null).ToDictionary(m => m.MediaId, m => m.Decision!);
            _best = BestFileSelector.Choose(Members.Select(m => m.File).ToList(), decisions);
            Members.First(m => m.MediaId == _best.File.Id).IsBest = true;
        }
        RefreshTexts();
        OnPropertyChanged(nameof(BestFileName));
    }

    /// <summary>Re-words everything shown, e.g. after a language switch.</summary>
    public void RefreshTexts()
    {
        if (_best is not null && Members.FirstOrDefault(m => m.MediaId == _best.File.Id) is { } best)
            best.BestReason = ReasonText(_best);
        foreach (var m in Members)
            m.RefreshTexts();
        OnPropertyChanged(nameof(CountText));
        OnPropertyChanged(nameof(ReclaimableShort));
        OnPropertyChanged(nameof(ReclaimableBytes));
        OnPropertyChanged(nameof(ReclaimableText));
    }

    private static string ReasonText(BestFileSelector.Choice c) => c.Kind switch
    {
        BestFileSelector.ReasonKind.MarkedKeep => Loc.T("marked as keep"),
        BestFileSelector.ReasonKind.OnlyFile => Loc.T("only file"),
        BestFileSelector.ReasonKind.HighestResolution => Loc.F("highest resolution ({0}×{1})", c.File.Width, c.File.Height),
        BestFileSelector.ReasonKind.LargestFile => Loc.T("same resolution, largest file (most detail)"),
        BestFileSelector.ReasonKind.Newest => Loc.T("same resolution and size, newest"),
        _ => Loc.T("same quality as the others")
    };

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
    public string FolderPath => System.IO.Path.GetDirectoryName(File.Path) ?? "";
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

    /// <summary>Lead-in of the explanation line for the copy to keep.</summary>
    public string BestText => IsBest ? Loc.F("Best copy: {0}. ", BestReason) : "";

    /// <summary>Notifies the texts computed in the current language.</summary>
    public void RefreshTexts()
    {
        OnPropertyChanged(nameof(BestText));
        OnPropertyChanged(nameof(MatchText));
    }

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
                return Loc.T("Reference: the other files were compared with this one");
            var parts = new List<string> { Loc.F("Picture {0:P0} similar", PictureSimilarity) };
            if (Evidence.Mirrored)
                parts.Add(Loc.T("mirrored"));
            if (Evidence.TrimKind is { } kind && Evidence.TrimDiffSec is double diff)
            {
                var length = TimeSpan.FromSeconds(Math.Abs(diff)).ToString(@"m\:ss", CultureInfo.InvariantCulture);
                var atEnd = kind == AppDb.TrimKinds.SameStart;
                parts.Add(Loc.F(diff < 0
                    ? atEnd ? "{0} shorter, cut at the end" : "{0} shorter, cut at the start"
                    : atEnd ? "{0} longer, extra footage at the end" : "{0} longer, extra footage at the start", length));
            }
            else if (Evidence.AvgDist > Group.MaxAvgDist)
            {
                // Audio is only compared when the scene check did not confirm the match.
                parts.Add(Evidence.AudioBer is double ber
                    ? Loc.F("confirmed by the soundtrack ({0:P0} match)", 1 - ber)
                    : Loc.T("confirmed by comparing scenes"));
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

    /// <summary>Set after loading; every edit is saved right away, there is no save button.</summary>
    public Action<RootVm>? Changed { get; set; }

    partial void OnIsEnabledChanged(bool value) => Changed?.Invoke(this);
    partial void OnIncludeSubdirsChanged(bool value) => Changed?.Invoke(this);
    partial void OnExcludeTextChanged(string? value) => Changed?.Invoke(this);
}
