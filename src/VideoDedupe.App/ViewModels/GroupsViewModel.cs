using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

using VideoDedupe.App.Services;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>Duplicate groups and everything the user does with their files.</summary>
public partial class GroupsViewModel : ObservableObject
{
    private readonly AppState _state;
    private readonly Func<IReadOnlyList<int>> _thumbnailPositions;
    private readonly ThumbnailLoader _thumbnails = new();
    private CancellationTokenSource? _thumbCts;
    private CancellationTokenSource? _cleanupCts;

    public ObservableCollection<GroupVm> Groups { get; } = new();
    public ObservableCollection<GroupVm> FilteredGroups { get; } = new();

    public IReadOnlyList<Choice> SortOptions { get; } =
        [new("space", "Most space to free"), new("size", "Largest files"), new("newest", "Newest")];

    [ObservableProperty] private string filter = "";
    [ObservableProperty] private Choice sort;

    public string TabTitle => Loc.F("Duplicates ({0})", Groups.Count);
    [ObservableProperty] private GroupVm? selectedGroup;
    [ObservableProperty] private bool isCleaning;

    [ObservableProperty] private string summary = "";

    public GroupsViewModel(AppState state, Func<IReadOnlyList<int>> thumbnailPositions)
    {
        _state = state;
        _thumbnailPositions = thumbnailPositions;
        sort = SortOptions[0];
        Groups.CollectionChanged += (_, _) => OnPropertyChanged(nameof(TabTitle));
        Loc.Instance.LanguageChanged += () =>
        {
            OnPropertyChanged(nameof(TabTitle));
            UpdateSummary();
            foreach (var group in Groups)
                group.RefreshTexts();
        };
        state.DatabaseChanged += () =>
        {
            _cleanupCts?.Cancel();
            Groups.Clear();
            FilteredGroups.Clear();
            SelectedGroup = null;
            CancelThumbnails();
            _thumbnails.Clear();
            UpdateSummary();
        };
    }

    partial void OnFilterChanged(string value) => RefreshView();
    partial void OnSortChanged(Choice value) => RefreshView();

    partial void OnSelectedGroupChanged(GroupVm? oldValue, GroupVm? newValue)
    {
        CancelThumbnails();
        if (oldValue is not null)
            ThumbnailLoader.Release(oldValue);
        if (newValue is null)
            return;

        // Debounce: in case the user scrolls quickly through the list.
        _thumbCts = new CancellationTokenSource();
        _ = LoadThumbnailsAsync(newValue, delay: true, _thumbCts.Token);
    }

    private void CancelThumbnails()
    {
        _thumbCts?.Cancel();
        _thumbCts?.Dispose();
        _thumbCts = null;
    }

    private async Task LoadThumbnailsAsync(GroupVm group, bool delay, CancellationToken ct)
    {
        try
        {
            if (delay)
                await Task.Delay(200, ct);
            // Not a busy operation: previews must never block scanning or detection.
            await _thumbnails.LoadAsync(group, Path.GetFullPath(_state.DbPath), _thumbnailPositions(), ct);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Thumbnails failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task LoadGroupsAsync()
    {
        try
        {
            using var _ = _state.Busy();
            _state.Status = Loc.T("Loading groups...");
            var db = await _state.OpenDbAsync();
            var groups = await db.ListDuplicateGroupsWithMembersAsync();
            var decisions = await db.GetDecisionsAsync(groups.SelectMany(g => g.Members).Select(m => m.File.Id));

            var selectedId = SelectedGroup?.GroupId;
            SelectedGroup = null;
            Groups.Clear();
            foreach (var (g, members) in groups)
            {
                var vm = new GroupVm { Owner = this, GroupId = g.Id, MaxAvgDist = g.MaxAvgDist, CreatedUtc = g.CreatedUtc };
                foreach (var member in members)
                    vm.Members.Add(new MemberVm(vm, member) { Decision = decisions.GetValueOrDefault(member.File.Id) });
                vm.UpdateBest();
                Groups.Add(vm);
            }
            RefreshView(selectedId);
            _state.Status = Loc.F("Loaded {0} groups.", Groups.Count);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    private void RefreshView(long? preferredGroupId = null)
    {
        IEnumerable<GroupVm> q = Sort.Id switch
        {
            "newest" => Groups.OrderByDescending(g => g.GroupId),
            "size" => Groups.OrderByDescending(g => g.Members.Sum(m => m.SizeBytes)),
            _ => Groups.OrderByDescending(g => g.ReclaimableBytes)
        };

        var f = (Filter ?? "").Trim();
        if (f.Length > 0)
            q = q.Where(g => g.Members.Any(m => m.Path.Contains(f, StringComparison.OrdinalIgnoreCase)));

        var previous = preferredGroupId ?? SelectedGroup?.GroupId;
        FilteredGroups.Clear();
        foreach (var g in q)
            FilteredGroups.Add(g);

        // Keep the selection where possible.
        SelectedGroup = FilteredGroups.FirstOrDefault(g => g.GroupId == previous) ?? FilteredGroups.FirstOrDefault();
        UpdateSummary();
    }

    private void UpdateSummary()
    {
        var bytes = Groups.Sum(g => g.ReclaimableBytes);
        Summary = Groups.Count == 0
            ? Loc.T("No duplicates found yet.")
            : Loc.F("{0} groups · {1} extra copies · {2} to free", Groups.Count, Groups.Sum(g => g.Members.Count - 1), GroupVm.FormatSize(bytes));
        HasGroups = Groups.Count > 0;
        HasVisibleGroups = FilteredGroups.Count > 0;
    }

    [ObservableProperty] private bool hasGroups;
    [ObservableProperty] private bool hasVisibleGroups;

    // ----- member actions -----

    [RelayCommand]
    public void OpenFile(MemberVm? m)
    {
        if (m is null) return;
        try
        {
            if (!File.Exists(m.Path)) { _state.Status = Loc.T("File not found."); return; }
            DesktopShell.OpenFile(m.Path);
            _state.Status = Loc.T("Opened file.");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Open failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public void ShowInExplorer(MemberVm? m)
    {
        if (m is null) return;
        try
        {
            if (!File.Exists(m.Path)) { _state.Status = Loc.T("File not found."); return; }
            DesktopShell.ShowInExplorer(m.Path);
            _state.Status = Loc.T("Opened file location.");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Explorer failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task CopyPathAsync(MemberVm? m)
    {
        if (m is null) return;
        try
        {
            _state.Status = await DesktopShell.CopyTextAsync(m.Path) ? Loc.T("Path copied to clipboard.") : Loc.T("Clipboard not available.");
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Copy failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task DeleteFileAsync(MemberVm? m)
    {
        if (m is null) return;
        try
        {
            var recycle = OperatingSystem.IsWindows();
            if (File.Exists(m.Path) && !await DesktopShell.ConfirmAsync(Loc.T("Delete file"),
                    recycle
                        ? Loc.F("Move this file to the Recycle Bin?\n\n{0}", m.Path)
                        : Loc.F("Permanently delete this file? This cannot be undone.\n\n{0}", m.Path),
                    recycle ? Loc.T("Move to Recycle Bin") : Loc.T("Delete permanently")))
            {
                _state.Status = Loc.T("Delete canceled.");
                return;
            }

            using (_state.Busy())
            {
                if (File.Exists(m.Path))
                    DesktopShell.DeleteFile(m.Path);
                // Otherwise the file would stay "active" in later runs.
                var db = await _state.OpenDbAsync();
                await db.MarkMediaFileDeletedAsync(m.MediaId);
            }
            _state.Status = Loc.F("Deleted: {0}", m.FileName);
            // Refresh every group: the same file can occur in more than one group.
            await LoadGroupsAsync();
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Delete failed: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task MarkKeepAsync(MemberVm? m)
    {
        if (m is null) return;
        try
        {
            var db = await _state.OpenDbAsync();
            await db.UpsertDecisionAsync(m.MediaId, BestFileSelector.KeepDecision, note: m.Path);
            // The file may be in several groups; all of them should now keep it.
            foreach (var group in Groups)
                foreach (var member in group.Members.Where(x => x.MediaId == m.MediaId))
                    member.Decision = BestFileSelector.KeepDecision;
            foreach (var group in Groups.Where(g => g.Members.Any(x => x.MediaId == m.MediaId)))
                group.UpdateBest();
            UpdateSummary();
            _state.Status = Loc.F("Marked as keep: {0}", m.FileName);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    [RelayCommand]
    public async Task QuarantineAsync(MemberVm? m)
    {
        if (m is null) return;
        try
        {
            if (!File.Exists(m.Path)) { _state.Status = Loc.T("File missing."); return; }
            string newPath;
            using (_state.Busy())
            {
                var db = await _state.OpenDbAsync();
                newPath = new FileMover(_state.QuarantineRoot).MoveToQuarantine(m.Path);
                await db.MarkMediaFileQuarantinedAsync(m.MediaId, newPath);
            }
            // Refresh every group: the same file can occur in more than one group.
            await LoadGroupsAsync();
            _state.Status = Loc.F("Moved to quarantine: {0}", newPath);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    /// <summary>The file is not a copy of the others: remove it and never group them again.</summary>
    [RelayCommand]
    public async Task NotDuplicateAsync(MemberVm? m)
    {
        if (m is null) return;
        var group = m.Group;
        try
        {
            using var _ = _state.Busy();
            var db = await _state.OpenDbAsync();
            await db.MarkNotDuplicateOfAsync(m.MediaId, group.Members.Select(x => x.MediaId));
            await db.RemoveGroupMemberAsync(group.GroupId, m.MediaId);
            group.Members.Remove(m);
            if (group.Members.Count < 2)
                await RemoveGroupAsync(db, group);
            else
            {
                group.UpdateBest();
                UpdateSummary();
            }
            _state.Status = Loc.F("Marked as not a duplicate: {0}", m.FileName);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    /// <summary>None of the files are copies of each other.</summary>
    [RelayCommand]
    public async Task GroupNotDuplicateAsync(GroupVm? group)
    {
        if (group is null) return;
        try
        {
            using var _ = _state.Busy();
            var db = await _state.OpenDbAsync();
            await db.MarkNotDuplicateAsync(group.Members.Select(m => m.MediaId));
            await RemoveGroupAsync(db, group);
            _state.Status = Loc.F("Group #{0} marked as not duplicates.", group.GroupId);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    private async Task RemoveGroupAsync(AppDb db, GroupVm group)
    {
        await db.DeleteGroupAsync(group.GroupId);
        ThumbnailLoader.Release(group);
        Groups.Remove(group);
        RefreshView(SelectedGroup == group ? null : SelectedGroup?.GroupId);
    }

    // ----- clean-up -----

    [RelayCommand]
    public Task CleanUpGroupAsync(GroupVm? group) =>
        group is null ? Task.CompletedTask : CleanUpAsync([group], Loc.F("group #{0}", group.GroupId));

    [RelayCommand]
    public Task CleanUpAllAsync() => CleanUpAsync(Groups.ToList(), Loc.T("all groups"));

    [RelayCommand]
    public void CancelCleanUp() => _cleanupCts?.Cancel();

    /// <summary>Moves every file except the one to keep to quarantine, after a confirmation that lists them.</summary>
    private async Task CleanUpAsync(IReadOnlyList<GroupVm> groups, string scope)
    {
        if (IsCleaning) return;
        var decisions = groups.SelectMany(g => g.Members).Where(m => m.Decision is not null)
            .DistinctBy(m => m.MediaId).ToDictionary(m => m.MediaId, m => m.Decision!);
        var plan = CleanupPlanner.Create(
            groups.Select(g => new CleanupPlanner.Group(g.GroupId, g.Members.Select(m => m.File).ToList())),
            decisions, File.Exists);
        if (plan.Items.Count == 0)
        {
            _state.Status = Loc.T("Nothing to clean up.");
            return;
        }

        var details = new StringBuilder();
        foreach (var group in plan.Items.GroupBy(i => i.GroupId))
        {
            details.AppendLine($"#{group.Key}  {Loc.T("keep")}  {group.First().KeptFile.Path}");
            foreach (var item in group)
                details.AppendLine($"      {Loc.T("move")}  {item.File.Path}  ({GroupVm.FormatSize(item.File.SizeBytes)})");
        }
        var message =
            Loc.F("Clean up {0}: move {1} files from {2} groups to the quarantine folder and free {3}.",
                scope, plan.Items.Count, plan.GroupCount, GroupVm.FormatSize(plan.Bytes)) + "\n\n" +
            Loc.F("Each group keeps its best copy, or the files you marked as keep. Nothing is deleted: the files can be moved back from\n{0}",
                _state.QuarantineRoot);
        if (!await DesktopShell.ConfirmAsync(Loc.T("Clean up duplicates"), message, Loc.F("Move {0} files", plan.Items.Count),
                details.ToString().TrimEnd()))
        {
            _state.Status = Loc.T("Clean-up canceled.");
            return;
        }

        var moved = 0;
        var canceled = false;
        var failed = new List<string>();
        _cleanupCts = new CancellationTokenSource();
        try
        {
            using var _ = _state.Busy();
            IsCleaning = true;
            var db = await _state.OpenDbAsync();
            var mover = new FileMover(_state.QuarantineRoot);
            foreach (var item in plan.Items)
            {
                _cleanupCts.Token.ThrowIfCancellationRequested();
                _state.Status = Loc.F("Moving {0}/{1}: {2}", moved + failed.Count + 1, plan.Items.Count, item.File.Path);
                try
                {
                    // Moving a big file across drives takes a while; keep the window responsive.
                    var target = await Task.Run(() => mover.MoveToQuarantine(item.File.Path));
                    await db.MarkMediaFileQuarantinedAsync(item.File.Id, target);
                    moved++;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    failed.Add($"{item.File.Path}: {ex.Message}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            canceled = true;
        }
        finally
        {
            IsCleaning = false;
            _cleanupCts.Dispose();
            _cleanupCts = null;
        }

        await LoadGroupsAsync();
        var result = Loc.F(canceled
            ? "Moved {0} of {1} files to quarantine (canceled)."
            : "Moved {0} of {1} files to quarantine.", moved, plan.Items.Count);
        if (failed.Count > 0)
            result += Loc.F(" {0} failed: {1}", failed.Count, string.Join(" | ", failed.Take(3)));
        _state.Status = result;
    }
}
