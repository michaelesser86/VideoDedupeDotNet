using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>Files in the quarantine folder: restore them or delete them for good.</summary>
public partial class QuarantineViewModel : ObservableObject
{
    private readonly AppState _state;

    public ObservableCollection<QuarantineItemVm> Items { get; } = new();

    [ObservableProperty] private QuarantineItemVm? selectedItem;
    [ObservableProperty] private bool hasItems;
    [ObservableProperty] private string summary = "";

    public string TabTitle => Loc.F("Quarantine ({0})", Items.Count);

    /// <summary>A file is back in the library, so the duplicate groups change.</summary>
    public event Func<Task>? Restored;

    public QuarantineViewModel(AppState state)
    {
        _state = state;
        Items.CollectionChanged += (_, _) =>
        {
            OnPropertyChanged(nameof(TabTitle));
            HasItems = Items.Count > 0;
            UpdateSummary();
        };
        Loc.Instance.LanguageChanged += () =>
        {
            OnPropertyChanged(nameof(TabTitle));
            UpdateSummary();
        };
        state.DatabaseChanged += () => Items.Clear();
    }

    private void UpdateSummary() => Summary = Items.Count == 0
        ? ""
        : Loc.F("{0} files · {1}", Items.Count, GroupVm.FormatSize(Items.Sum(i => i.SizeBytes)));

    [RelayCommand]
    public async Task LoadAsync()
    {
        try
        {
            var db = await _state.OpenDbAsync();
            var rows = await db.ListQuarantinedFilesAsync();
            var selected = SelectedItem?.MediaId;
            Items.Clear();
            foreach (var row in rows)
                Items.Add(new QuarantineItemVm(row));
            SelectedItem = Items.FirstOrDefault(i => i.MediaId == selected) ?? Items.FirstOrDefault();
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task RestoreAsync(QuarantineItemVm? item)
    {
        if (item is null) return;
        try
        {
            await _state.FlushPendingAsync();
            using (_state.Busy())
            {
                await Task.Run(() => FileMover.Restore(item.QuarantinePath, item.OriginalPath));
                var db = await _state.OpenDbAsync();
                await db.MarkQuarantineRestoredAsync(item.MediaId);
            }
            Remove(item);
            _state.Status = Loc.F("Restored: {0}", item.OriginalPath);
            if (Restored is { } restored)
                await restored();
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Restore failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task DeleteAsync(QuarantineItemVm? item)
    {
        if (item is null) return;
        if (!OperatingSystem.IsWindows() && item.Exists && !await DesktopShell.ConfirmAsync(
                Loc.T("Delete file"),
                Loc.F("Permanently delete this file? This cannot be undone.\n\n{0}", item.QuarantinePath),
                Loc.T("Delete permanently")))
            return;
        try
        {
            using (_state.Busy())
            {
                if (item.Exists)
                    DesktopShell.DeleteFile(item.QuarantinePath);
                var db = await _state.OpenDbAsync();
                await db.MarkQuarantineDiscardedAsync(item.MediaId);
            }
            Remove(item);
            _state.Status = Loc.F("Deleted: {0}", item.FileName);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Delete failed: {0}", ex.Message);
        }
    }

    /// <summary>Deletes every quarantined file, after a confirmation (it is a lot at once).</summary>
    [RelayCommand]
    public async Task DeleteAllAsync()
    {
        if (Items.Count == 0) return;
        var recycle = OperatingSystem.IsWindows();
        var total = GroupVm.FormatSize(Items.Sum(i => i.SizeBytes));
        if (!await DesktopShell.ConfirmAsync(
                Loc.T("Empty quarantine"),
                recycle
                    ? Loc.F("Move all {0} files ({1}) from the quarantine folder to the Recycle Bin?", Items.Count, total)
                    : Loc.F("Permanently delete all {0} files ({1}) in the quarantine folder? This cannot be undone.", Items.Count, total),
                Loc.F("Delete {0} files", Items.Count),
                string.Join(Environment.NewLine, Items.Select(i => i.QuarantinePath))))
            return;

        var done = 0;
        var failed = 0;
        using (_state.Busy())
        {
            var db = await _state.OpenDbAsync();
            foreach (var item in Items.ToList())
            {
                try
                {
                    if (item.Exists)
                        DesktopShell.DeleteFile(item.QuarantinePath);
                    await db.MarkQuarantineDiscardedAsync(item.MediaId);
                    Remove(item);
                    done++;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or OperationCanceledException)
                {
                    failed++;
                }
            }
        }
        _state.Status = failed == 0
            ? Loc.F("Deleted {0} files from the quarantine folder.", done)
            : Loc.F("Deleted {0} files from the quarantine folder, {1} failed.", done, failed);
    }

    [RelayCommand]
    public void OpenFolder()
    {
        try
        {
            Directory.CreateDirectory(_state.QuarantineRoot);
            DesktopShell.OpenFile(_state.QuarantineRoot);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Explorer failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public void ShowInExplorer(QuarantineItemVm? item)
    {
        if (item is null) return;
        try
        {
            if (item.Exists) DesktopShell.ShowInExplorer(item.QuarantinePath);
            else _state.Status = Loc.T("File not found.");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Explorer failed: {0}", ex.Message);
        }
    }

    private void Remove(QuarantineItemVm item)
    {
        var index = Items.IndexOf(item);
        Items.Remove(item);
        if (SelectedItem is null && Items.Count > 0)
            SelectedItem = Items[Math.Clamp(index, 0, Items.Count - 1)];
    }
}

public sealed class QuarantineItemVm(AppDb.QuarantinedFile row)
{
    public long MediaId { get; } = row.MediaFileId;
    public string OriginalPath { get; } = row.OriginalPath;
    public string QuarantinePath { get; } = row.QuarantinePath;
    public long SizeBytes { get; } = row.SizeBytes;

    public string FileName => Path.GetFileName(OriginalPath);
    public string OriginalFolder => Path.GetDirectoryName(OriginalPath) ?? "";
    public string SizeText => GroupVm.FormatSize(SizeBytes);
    public bool Exists => QuarantinePath.Length > 0 && File.Exists(QuarantinePath);
    public bool IsMissing => !Exists;
}
