using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>Everything the user deleted, quarantined or restored, newest first.</summary>
public sealed class HistoryViewModel
{
    public IReadOnlyList<HistoryItemVm> Items { get; }
    public string Summary { get; }
    public bool IsEmpty => Items.Count == 0;

    public HistoryViewModel(IReadOnlyList<AppDb.HistoryEntry> entries)
    {
        Items = entries.Select(e => new HistoryItemVm(e)).ToList();

        string Part(string format, params string[] actions)
        {
            var matching = entries.Where(e => actions.Contains(e.Action)).ToList();
            return Loc.F(format, matching.Count, GroupVm.FormatSize(matching.Sum(e => e.SizeBytes)));
        }
        Summary = string.Join(" · ",
            Part("Deleted: {0} files, {1}", AppDb.HistoryActions.Deleted, AppDb.HistoryActions.Discarded),
            Part("Quarantined: {0} files, {1}", AppDb.HistoryActions.Quarantined),
            Part("Restored: {0} files, {1}", AppDb.HistoryActions.Restored));
    }
}

public sealed class HistoryItemVm(AppDb.HistoryEntry entry)
{
    public string When { get; } =
        DateTime.TryParse(entry.Utc, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var utc)
            ? utc.ToLocalTime().ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture)
            : "";

    public string ActionText { get; } = entry.Action switch
    {
        AppDb.HistoryActions.Deleted => Loc.T("deleted"),
        AppDb.HistoryActions.Quarantined => Loc.T("quarantined"),
        AppDb.HistoryActions.Restored => Loc.T("restored"),
        AppDb.HistoryActions.Discarded => Loc.T("deleted from quarantine"),
        _ => entry.Action
    };

    public string FileName { get; } = Path.GetFileName(entry.Path);
    public string Folder { get; } = Path.GetDirectoryName(entry.Path) ?? "";
    public string FullPath { get; } = entry.Path;
    public string SizeText { get; } = GroupVm.FormatSize(entry.SizeBytes);
}
