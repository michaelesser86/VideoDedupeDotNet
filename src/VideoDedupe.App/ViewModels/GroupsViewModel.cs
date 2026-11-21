using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
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
    /// <summary>How long a single delete or quarantine can be undone before it really happens.</summary>
    public static TimeSpan UndoWindow { get; set; } = TimeSpan.FromSeconds(8);

    private readonly AppState _state;
    private readonly Func<IReadOnlyList<int>> _thumbnailPositions;
    private readonly ThumbnailLoader _thumbnails = new();
    private CancellationTokenSource? _thumbCts;
    private CancellationTokenSource? _cleanupCts;
    private PendingAction? _pending;

    public ObservableCollection<GroupVm> Groups { get; } = new();
    public ObservableCollection<GroupVm> FilteredGroups { get; } = new();

    public IReadOnlyList<Choice> SortOptions { get; } =
        [new("space", "Most space to free"), new("size", "Largest files"), new("newest", "Newest")];

    public IReadOnlyList<Choice> CleanupTargetOptions { get; } =
        [new("quarantine", "to the quarantine folder"), new("recycle", "to the Recycle Bin")];

    public IReadOnlyList<Choice> CodecOptions { get; } =
    [
        new(BestFileRules.Any, "No preference"),
        new(BestFileRules.ModernCodec, "HEVC, AV1 or VP9 (smaller files)"),
        new(BestFileRules.H264, "H.264 (plays everywhere)")
    ];

    public IReadOnlyList<Choice> ContainerOptions { get; } =
        [new(BestFileRules.Any, "No preference"), new(BestFileRules.Mp4, "MP4"), new(BestFileRules.Mkv, "MKV")];

    public IReadOnlyList<Choice> SizeOptions { get; } =
        [new("larger", "Larger file (more detail)"), new("smaller", "Smaller file (less space)")];

    [ObservableProperty] private Choice preferredCodec;
    [ObservableProperty] private Choice preferredContainer;
    [ObservableProperty] private Choice preferredSize;

    /// <summary>The user's rules for choosing between copies of the same resolution.</summary>
    public BestFileRules Rules => new(PreferredCodec.Id, PreferredContainer.Id, PreferredSize.Id == "smaller");

    partial void OnPreferredCodecChanged(Choice value) => RulesChanged();
    partial void OnPreferredContainerChanged(Choice value) => RulesChanged();
    partial void OnPreferredSizeChanged(Choice value) => RulesChanged();

    private void RulesChanged()
    {
        foreach (var group in Groups)
            group.UpdateBest();
        UpdateSummary();
    }

    /// <summary>Selects the saved rules (unknown ids fall back to the defaults).</summary>
    public void SetRules(string codec, string container, bool preferSmaller)
    {
        PreferredCodec = CodecOptions.FirstOrDefault(o => o.Id == codec) ?? CodecOptions[0];
        PreferredContainer = ContainerOptions.FirstOrDefault(o => o.Id == container) ?? ContainerOptions[0];
        PreferredSize = SizeOptions[preferSmaller ? 1 : 0];
    }

    [ObservableProperty] private string filter = "";
    [ObservableProperty] private Choice sort;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(CleanUpGroupText), nameof(CleanUpGroupTip), nameof(CleanUpAllTip))]
    private Choice cleanupTarget;

    public string TabTitle => Loc.F("Duplicates ({0})", Groups.Count);
    [ObservableProperty] private GroupVm? selectedGroup;
    [ObservableProperty] private MemberVm? currentMember;
    [ObservableProperty] private bool isCleaning;

    [ObservableProperty] private string summary = "";

    /// <summary>Text of the undo bar; the bar shows while an action can still be undone.</summary>
    [ObservableProperty] private string pendingText = "";
    [ObservableProperty] private bool hasPendingAction;

    /// <summary>Folder rules from the Folders tab; decide the best copy before quality does.</summary>
    public FolderPriorities Priorities { get; private set; } = FolderPriorities.None;

    /// <summary>A file was moved to the quarantine folder.</summary>
    public event Action? QuarantineChanged;

    private bool RecycleOnCleanup => CleanupTarget.Id == "recycle";

    public string CleanUpGroupText => Loc.T(RecycleOnCleanup ? "Keep best, delete others" : "Keep best, quarantine others");

    public string CleanUpGroupTip => Loc.T(RecycleOnCleanup
        ? "Moves every file except the best one (or the files marked as keep) to the Recycle Bin. Asks first."
        : "Moves every file except the best one (or the files marked as keep) to the quarantine folder. Asks first.");

    public string CleanUpAllTip => Loc.T(RecycleOnCleanup
        ? "Keep the best copy of every group and move the others to the Recycle Bin. Shows the full list first."
        : "Keep the best copy of every group and move the others to the quarantine folder. Shows the full list first.");

    public GroupsViewModel(AppState state, Func<IReadOnlyList<int>> thumbnailPositions, string? cleanupTarget = null)
    {
        _state = state;
        _thumbnailPositions = thumbnailPositions;
        sort = SortOptions[0];
        preferredCodec = CodecOptions[0];
        preferredContainer = ContainerOptions[0];
        preferredSize = SizeOptions[0];
        this.cleanupTarget = CleanupTargetOptions.FirstOrDefault(o => o.Id == cleanupTarget) ?? CleanupTargetOptions[0];
        Groups.CollectionChanged += (_, _) => OnPropertyChanged(nameof(TabTitle));
        Loc.Instance.LanguageChanged += () =>
        {
            OnPropertyChanged(nameof(TabTitle));
            OnPropertyChanged(nameof(CleanUpGroupText));
            OnPropertyChanged(nameof(CleanUpGroupTip));
            OnPropertyChanged(nameof(CleanUpAllTip));
            UpdateSummary();
            foreach (var group in Groups)
                group.RefreshTexts();
        };
        state.DatabaseChanged += () =>
        {
            // The pending action keeps its own database handle and still completes.
            _ = CommitPendingAsync();
            _cleanupCts?.Cancel();
            Groups.Clear();
            FilteredGroups.Clear();
            SelectedGroup = null;
            Priorities = FolderPriorities.None;
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
        CurrentMember = DefaultMember(newValue);
        if (newValue is null)
            return;

        // Debounce: in case the user scrolls quickly through the list.
        _thumbCts = new CancellationTokenSource();
        _ = LoadThumbnailsAsync(newValue, delay: true, _thumbCts.Token);
    }

    partial void OnCurrentMemberChanged(MemberVm? oldValue, MemberVm? newValue)
    {
        if (oldValue is not null) oldValue.IsCurrent = false;
        if (newValue is not null) newValue.IsCurrent = true;
    }

    /// <summary>The first copy that is not the one to keep: what the user most likely removes next.</summary>
    private static MemberVm? DefaultMember(GroupVm? group) =>
        group?.Members.FirstOrDefault(m => !m.IsBest) ?? group?.Members.FirstOrDefault();

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
    public Task LoadGroupsAsync() => ReloadAsync(null);

    private async Task ReloadAsync(long? selectGroupId)
    {
        try
        {
            // A pending delete must happen first, or the reload would show the file again.
            await CommitPendingAsync();
            using var _ = _state.Busy();
            _state.Status = Loc.T("Loading groups...");
            var db = await _state.OpenDbAsync();
            Priorities = await LoadPrioritiesAsync(db);
            var groups = await db.ListDuplicateGroupsWithMembersAsync();
            var decisions = await db.GetDecisionsAsync(groups.SelectMany(g => g.Members).Select(m => m.File.Id));

            var selectedId = selectGroupId ?? SelectedGroup?.GroupId;
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

    private static async Task<FolderPriorities> LoadPrioritiesAsync(AppDb db) =>
        new((await db.ListScanRootsAsync()).Select(r => (r.Path, (int)r.Priority)));

    /// <summary>Called when a folder's priority changed: the best copy may now be another one.</summary>
    public async Task ReloadPrioritiesAsync()
    {
        try
        {
            Priorities = await LoadPrioritiesAsync(await _state.OpenDbAsync());
            foreach (var group in Groups)
                group.UpdateBest();
            UpdateSummary();
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    private void RefreshView(long? preferredGroupId = null, int? fallbackIndex = null)
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

        // Keep the selection where possible; otherwise stay at the same place in the list.
        SelectedGroup = FilteredGroups.FirstOrDefault(g => g.GroupId == previous)
                        ?? (fallbackIndex is int i && FilteredGroups.Count > 0
                            ? FilteredGroups[Math.Clamp(i, 0, FilteredGroups.Count - 1)]
                            : FilteredGroups.FirstOrDefault());
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

    // ----- keyboard navigation -----

    /// <summary>Selects the previous (-1) or next (+1) group in the list.</summary>
    public void SelectAdjacentGroup(int delta)
    {
        if (FilteredGroups.Count == 0) return;
        var index = SelectedGroup is null ? -1 : FilteredGroups.IndexOf(SelectedGroup);
        SelectedGroup = FilteredGroups[Math.Clamp(index + delta, 0, FilteredGroups.Count - 1)];
    }

    /// <summary>Moves the current file within the selected group.</summary>
    public void SelectAdjacentMember(int delta)
    {
        if (SelectedGroup is not { Members.Count: > 0 } group) return;
        var index = CurrentMember is null ? -1 : group.Members.IndexOf(CurrentMember);
        CurrentMember = group.Members[Math.Clamp(index + delta, 0, group.Members.Count - 1)];
    }

    /// <summary>Makes the n-th file (0-based) of the selected group the current one.</summary>
    public void SelectMember(int index)
    {
        if (SelectedGroup is { } group && index >= 0 && index < group.Members.Count)
            CurrentMember = group.Members[index];
    }

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

    /// <summary>Opens the file next to the best copy (or, for the best copy, next to the first other one).</summary>
    [RelayCommand]
    public async Task CompareAsync(MemberVm? m)
    {
        m ??= CurrentMember;
        if (m is null || m.Group.Members.Count < 2) return;
        var other = m.IsBest ? m.Group.Members.First(x => x != m) : m.Group.Members.First(x => x.IsBest);
        var (left, right) = m.IsBest ? (m, other) : (other, m);
        if (left.IsMissing || right.IsMissing)
        {
            _state.Status = Loc.T("File not found.");
            return;
        }
        try
        {
            if (DesktopShell.MainWindow() is { } owner)
                await new Views.CompareWindow { DataContext = new CompareViewModel(left, right) }.ShowDialog(owner);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
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
        // On Windows the file goes to the Recycle Bin and can be restored, so no question.
        // Elsewhere deleting is permanent and still asks first.
        if (!OperatingSystem.IsWindows() && File.Exists(m.Path) && !await DesktopShell.ConfirmAsync(
                Loc.T("Delete file"),
                Loc.F("Permanently delete this file? This cannot be undone.\n\n{0}", m.Path),
                Loc.T("Delete permanently")))
        {
            _state.Status = Loc.T("Delete canceled.");
            return;
        }
        await StartPendingAsync(m, PendingKind.Delete);
    }

    [RelayCommand]
    public async Task QuarantineAsync(MemberVm? m)
    {
        if (m is null) return;
        if (!File.Exists(m.Path)) { _state.Status = Loc.T("File missing."); return; }
        await StartPendingAsync(m, PendingKind.Quarantine);
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
                if (CurrentMember == m)
                    CurrentMember = DefaultMember(group);
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
        var index = FilteredGroups.IndexOf(group);
        Groups.Remove(group);
        RefreshView(SelectedGroup == group ? null : SelectedGroup?.GroupId, index);
    }

    // ----- undo: a delete or quarantine only happens after the undo window -----

    private enum PendingKind { Delete, Quarantine }

    private sealed record PendingAction(
        PendingKind Kind, long MediaId, string Path, string FileName, long GroupId,
        AppDb Db, string QuarantineRoot, CancellationTokenSource Timer);

    private async Task StartPendingAsync(MemberVm m, PendingKind kind)
    {
        try
        {
            await CommitPendingAsync();
            var pending = new PendingAction(kind, m.MediaId, m.Path, m.FileName, m.Group.GroupId,
                await _state.OpenDbAsync(), _state.QuarantineRoot, new CancellationTokenSource());
            _pending = pending;
            PendingText = Loc.F(kind == PendingKind.Delete ? "Deleted: {0}" : "Moved to quarantine: {0}", m.FileName);
            HasPendingAction = true;
            HideFile(m.MediaId);
            _ = CommitAfterUndoWindowAsync(pending);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Error: {0}", ex.Message);
        }
    }

    private async Task CommitAfterUndoWindowAsync(PendingAction pending)
    {
        try
        {
            await Task.Delay(UndoWindow, pending.Timer.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        if (_pending == pending)
            await CommitPendingAsync();
    }

    /// <summary>Carries out the action waiting in the undo bar right away (before closing, scanning, ...).</summary>
    public async Task CommitPendingAsync()
    {
        if (_pending is not { } pending) return;
        _pending = null;
        HasPendingAction = false;
        pending.Timer.Cancel();
        pending.Timer.Dispose();

        try
        {
            using var _ = _state.Busy();
            if (pending.Kind == PendingKind.Quarantine)
            {
                // Moving a big file across drives takes a while; keep the window responsive.
                var mover = new FileMover(pending.QuarantineRoot);
                var target = await Task.Run(() => mover.MoveToQuarantine(pending.Path));
                await pending.Db.MarkMediaFileQuarantinedAsync(pending.MediaId, target);
                _state.Status = Loc.F("Moved to quarantine: {0}", target);
                QuarantineChanged?.Invoke();
            }
            else
            {
                if (File.Exists(pending.Path))
                    DesktopShell.DeleteFile(pending.Path);
                // Otherwise the file would stay "active" in later runs.
                await pending.Db.MarkMediaFileDeletedByUserAsync(pending.MediaId);
                _state.Status = Loc.F("Deleted: {0}", pending.FileName);
            }
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F(pending.Kind == PendingKind.Delete ? "Delete failed: {0}" : "Error: {0}", ex.Message);
            // The file is still there: show it again.
            await ReloadAsync(pending.GroupId);
        }
    }

    [RelayCommand]
    public async Task UndoAsync()
    {
        if (_pending is not { } pending) return;
        _pending = null;
        HasPendingAction = false;
        pending.Timer.Cancel();
        pending.Timer.Dispose();
        // Nothing happened on disk or in the database yet; reloading shows the file again.
        await ReloadAsync(pending.GroupId);
        _state.Status = Loc.F("Undone: {0}", pending.FileName);
    }

    /// <summary>Takes a file out of every group on screen (the database is unchanged until the commit).</summary>
    private void HideFile(long mediaId)
    {
        var selected = SelectedGroup;
        var index = selected is null ? 0 : FilteredGroups.IndexOf(selected);
        var removedSelected = false;
        foreach (var group in Groups.ToList())
        {
            var gone = group.Members.Where(m => m.MediaId == mediaId).ToList();
            if (gone.Count == 0) continue;
            foreach (var member in gone)
                group.Members.Remove(member);
            if (group.Members.Count < 2)
            {
                ThumbnailLoader.Release(group);
                Groups.Remove(group);
                removedSelected |= group == selected;
            }
            else
            {
                group.UpdateBest();
            }
        }

        if (removedSelected)
            RefreshView(null, index);
        else
        {
            if (CurrentMember?.MediaId == mediaId)
                CurrentMember = DefaultMember(selected);
            RefreshView(selected?.GroupId);
        }
    }

    // ----- clean-up -----

    [RelayCommand]
    public Task CleanUpGroupAsync(GroupVm? group) =>
        group is null ? Task.CompletedTask : CleanUpAsync([group], Loc.F("group #{0}", group.GroupId));

    [RelayCommand]
    public Task CleanUpAllAsync() => CleanUpAsync(Groups.ToList(), Loc.T("all groups"));

    [RelayCommand]
    public void CancelCleanUp() => _cleanupCts?.Cancel();

    /// <summary>Removes every file except the one to keep, after a confirmation that lists them.</summary>
    private async Task CleanUpAsync(IReadOnlyList<GroupVm> groups, string scope)
    {
        if (IsCleaning) return;
        await CommitPendingAsync();
        var recycle = RecycleOnCleanup;
        var decisions = groups.SelectMany(g => g.Members).Where(m => m.Decision is not null)
            .DistinctBy(m => m.MediaId).ToDictionary(m => m.MediaId, m => m.Decision!);
        var plan = CleanupPlanner.Create(
            groups.Select(g => new CleanupPlanner.Group(g.GroupId, g.Members.Select(m => m.File).ToList())),
            decisions, File.Exists, Priorities, Rules);
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
                details.AppendLine($"      {Loc.T(recycle ? "delete" : "move")}  {item.File.Path}  ({GroupVm.FormatSize(item.File.SizeBytes)})");
        }
        var message = recycle
            ? Loc.F("Clean up {0}: move {1} files from {2} groups to the Recycle Bin and free {3}.",
                  scope, plan.Items.Count, plan.GroupCount, GroupVm.FormatSize(plan.Bytes)) + "\n\n" +
              Loc.T("Each group keeps its best copy, or the files you marked as keep. The files can be restored from the Recycle Bin.")
            : Loc.F("Clean up {0}: move {1} files from {2} groups to the quarantine folder and free {3}.",
                  scope, plan.Items.Count, plan.GroupCount, GroupVm.FormatSize(plan.Bytes)) + "\n\n" +
              Loc.F("Each group keeps its best copy, or the files you marked as keep. Nothing is deleted: the files can be moved back from\n{0}",
                  _state.QuarantineRoot);
        if (!await DesktopShell.ConfirmAsync(Loc.T("Clean up duplicates"), message,
                Loc.F(recycle ? "Delete {0} files" : "Move {0} files", plan.Items.Count),
                details.ToString().TrimEnd()))
        {
            _state.Status = Loc.T("Clean-up canceled.");
            return;
        }

        var done = 0;
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
                _state.Status = Loc.F(recycle ? "Deleting {0}/{1}: {2}" : "Moving {0}/{1}: {2}",
                    done + failed.Count + 1, plan.Items.Count, item.File.Path);
                try
                {
                    if (recycle)
                    {
                        DesktopShell.DeleteFile(item.File.Path);
                        await db.MarkMediaFileDeletedByUserAsync(item.File.Id);
                    }
                    else
                    {
                        // Moving a big file across drives takes a while; keep the window responsive.
                        var target = await Task.Run(() => mover.MoveToQuarantine(item.File.Path));
                        await db.MarkMediaFileQuarantinedAsync(item.File.Id, target);
                    }
                    done++;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException
                                               or OperationCanceledException { CancellationToken.IsCancellationRequested: false })
                {
                    // The Recycle Bin reports a canceled error dialog as OperationCanceledException.
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
        if (!recycle && done > 0)
            QuarantineChanged?.Invoke();
        var result = recycle
            ? Loc.F(canceled ? "Moved {0} of {1} files to the Recycle Bin (canceled)." : "Moved {0} of {1} files to the Recycle Bin.",
                done, plan.Items.Count)
            : Loc.F(canceled ? "Moved {0} of {1} files to quarantine (canceled)." : "Moved {0} of {1} files to quarantine.",
                done, plan.Items.Count);
        if (failed.Count > 0)
            result += Loc.F(" {0} failed: {1}", failed.Count, string.Join(" | ", failed.Take(3)));
        _state.Status = result;
    }

    // ----- report -----

    [RelayCommand]
    public async Task ExportCsvAsync()
    {
        if (Groups.Count == 0)
        {
            _state.Status = Loc.T("No duplicates found yet.");
            return;
        }
        try
        {
            var path = await DesktopShell.PickSaveFileAsync(Loc.T("Export duplicates"),
                $"duplicates-{DateTime.Now:yyyy-MM-dd}.csv", Loc.T("CSV table"), "*.csv");
            if (path is null) return;

            var rows = FilteredGroups.Concat(Groups.Except(FilteredGroups))
                .SelectMany(g => g.Members.Select(m => new DuplicateReport.Row(g.GroupId, m.IsBest, m.Evidence)))
                .ToList();
            var culture = Loc.Instance.Language == Loc.German ? CultureInfo.GetCultureInfo("de-DE") : CultureInfo.InvariantCulture;
            await using (var writer = new StreamWriter(path, append: false, DuplicateReport.FileEncoding))
                DuplicateReport.WriteCsv(writer, rows, culture, Loc.T);
            _state.Status = Loc.F("Exported {0} groups to {1}", Groups.Count, path);
        }
        catch (Exception ex)
        {
            _state.Status = Loc.F("Export failed: {0}", ex.Message);
        }
    }
}
