using System.Globalization;

using VideoDedupe.App.ViewModels;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class CleanupFeatureTests
{
    private static AppDb.MediaFileRow File(long id, string path, int width = 1920, int height = 1080, long size = 1000,
        double? duration = null) =>
        new()
        {
            Id = id, Path = path, Width = width, Height = height, SizeBytes = size, DurationSec = duration,
            ModifiedUtc = "2024-01-01T00:00:00Z"
        };

    private static readonly string Archive = Path.Combine(Path.GetTempPath(), "Archive");
    private static readonly string Downloads = Path.Combine(Path.GetTempPath(), "Downloads");

    [Fact]
    public void FolderPriorityUsesTheDeepestLibraryFolderAndWholeFolderNames()
    {
        var priorities = new FolderPriorities(
        [
            (Archive, FolderPriorities.Prefer),
            (Path.Combine(Archive, "Unsorted"), FolderPriorities.Avoid),
            (Downloads + Path.DirectorySeparatorChar, FolderPriorities.Avoid)
        ]);

        Assert.Equal(FolderPriorities.Prefer, priorities.Of(Path.Combine(Archive, "a.mp4")));
        Assert.Equal(FolderPriorities.Avoid, priorities.Of(Path.Combine(Archive, "Unsorted", "a.mp4")));
        Assert.Equal(FolderPriorities.Avoid, priorities.Of(Path.Combine(Downloads, "x", "a.mp4")));
        // "Archive2" is not inside "Archive".
        Assert.Equal(FolderPriorities.Normal, priorities.Of(Archive + "2" + Path.DirectorySeparatorChar + "a.mp4"));
        Assert.True(priorities.HasRules);
        Assert.False(new FolderPriorities([(Archive, FolderPriorities.Normal)]).HasRules);
    }

    [Fact]
    public void PreferredFolderWinsOverResolutionButNotOverKeep()
    {
        var priorities = new FolderPriorities([(Archive, FolderPriorities.Prefer), (Downloads, FolderPriorities.Avoid)]);
        AppDb.MediaFileRow[] files =
        [
            File(1, Path.Combine(Downloads, "a.mp4"), 3840, 2160),
            File(2, Path.Combine(Archive, "a.mp4"), 1280, 720),
            File(3, Path.Combine(Path.GetTempPath(), "Other", "a.mp4"), 1920, 1080)
        ];

        var choice = BestFileSelector.Choose(files, priorities: priorities);
        Assert.Equal(2, choice.File.Id);
        Assert.Equal(BestFileSelector.ReasonKind.PreferredFolder, choice.Kind);

        // Without a preferred copy, a normal folder still beats an avoided one.
        choice = BestFileSelector.Choose([files[0], files[2]], priorities: priorities);
        Assert.Equal(3, choice.File.Id);
        Assert.Equal(BestFileSelector.ReasonKind.OthersInAvoidedFolder, choice.Kind);

        // All in the same folder class: quality decides as before.
        choice = BestFileSelector.Choose([files[0], File(4, Path.Combine(Downloads, "b.mp4"), 640, 360)], priorities: priorities);
        Assert.Equal(1, choice.File.Id);
        Assert.Equal(BestFileSelector.ReasonKind.HighestResolution, choice.Kind);

        choice = BestFileSelector.Choose(files, new Dictionary<long, string> { [1] = BestFileSelector.KeepDecision }, priorities);
        Assert.Equal(1, choice.File.Id);

        var plan = CleanupPlanner.Create([new CleanupPlanner.Group(1, files)], new Dictionary<long, string>(), _ => true, priorities);
        Assert.Equal([1L, 3L], plan.Items.Select(i => i.File.Id).Order());
    }

    [Fact]
    public void CsvReportFollowsTheCultureAndCannotRunFormulas()
    {
        var members = new[]
        {
            new AppDb.GroupMember(File(1, Path.Combine("C:", "Videos", "=cmd;x.mp4"), size: 1572864, duration: 61.5), 0),
            new AppDb.GroupMember(File(2, Path.Combine("C:", "Videos", "say \"hi\".mp4")), 16)
        };
        var rows = new[] { new DuplicateReport.Row(7, true, members[0]), new DuplicateReport.Row(7, false, members[1]) };

        var german = new StringWriter();
        DuplicateReport.WriteCsv(german, rows, CultureInfo.GetCultureInfo("de-DE"),
            text => text == "keep" ? "behalten" : text == "remove" ? "entfernen" : text);
        var lines = german.ToString().TrimEnd().Split(Environment.NewLine);
        Assert.Equal(3, lines.Length);
        Assert.StartsWith("Group;Action;File name;Folder;Size (MB)", lines[0]);
        // Separator inside a name is quoted; a leading "=" is defused; decimal comma.
        Assert.StartsWith("7;behalten;\"'=cmd;x.mp4\";", lines[1]);
        Assert.Contains(";1,5;1920x1080;61,5;", lines[1]);
        Assert.Contains(";\"say \"\"hi\"\".mp4\";", lines[2]);
        Assert.Contains(";entfernen;", lines[2]);
        Assert.Contains(";50;", lines[2]); // 16 of 32 bits differ: 50 % similar

        var english = new StringWriter();
        DuplicateReport.WriteCsv(english, rows, CultureInfo.InvariantCulture);
        Assert.Contains(",1.5,1920x1080,61.5,", english.ToString());
    }

    [Fact]
    public async Task QuarantinedFilesCanBeListedRestoredAndDiscarded()
    {
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        var original = workspace.CreateFile(Path.Combine("lib", "a.mp4"));
        await db.ExecuteAsync("INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc) VALUES (1, @P, 3, '', ''), (2, 'old.mp4', 5, '', '');",
            new { P = original });

        var target = new FileMover(Path.Combine(workspace.Root, "quarantine")).MoveToQuarantine(original);
        await db.MarkMediaFileQuarantinedAsync(1, target);
        // A row written before the QuarantinePath column existed.
        await db.ExecuteAsync(@"
INSERT INTO ReviewDecision(MediaFileId, Decision, Note, DecidedUtc) VALUES (2, 'quarantine', 'moved to: Q:\old.mp4', '2000-01-01');
UPDATE MediaFile SET IsDeleted=1 WHERE Id=2;");

        var listed = await db.ListQuarantinedFilesAsync();
        Assert.Equal([1L, 2L], listed.Select(f => f.MediaFileId));
        Assert.Equal(target, listed[0].QuarantinePath);
        Assert.Equal(original, listed[0].OriginalPath);
        Assert.Equal(@"Q:\old.mp4", listed[1].QuarantinePath);

        // Restoring never overwrites a file that took the original place.
        workspace.CreateFile(Path.Combine("lib", "a.mp4"));
        Assert.Throws<IOException>(() => FileMover.Restore(target, original));
        System.IO.File.Delete(original);

        FileMover.Restore(target, original);
        await db.MarkQuarantineRestoredAsync(1);
        Assert.True(System.IO.File.Exists(original));
        Assert.False(System.IO.File.Exists(target));
        await db.MarkQuarantineDiscardedAsync(2);
        Assert.Empty(await db.ListQuarantinedFilesAsync());
        Assert.Equal(1, (await db.ListIndexedFileStatesAsync()).Count(f => f.Value.IsDeleted == 0));
    }

    [Fact]
    public async Task QuarantineWaitsForTheUndoWindowAndCanBeUndone()
    {
        GroupsViewModel.UndoWindow = TimeSpan.FromHours(1);
        using var workspace = new TestWorkspace();
        var db = workspace.Db;
        await DbInitializer.EnsureCreatedAsync(db);
        var big = workspace.CreateFile(Path.Combine("lib", "big.mp4"));
        var small = workspace.CreateFile(Path.Combine("lib", "small.mp4"));
        var other1 = workspace.CreateFile(Path.Combine("lib", "x1.mp4"));
        var other2 = workspace.CreateFile(Path.Combine("lib", "x2.mp4"));
        await db.ExecuteAsync(@"
INSERT INTO MediaFile(Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc, Width, Height) VALUES
 (1, @Big, 3, '', '', 1920, 1080), (2, @Small, 3, '', '', 1280, 720),
 (3, @Other1, 3, '', '', 1920, 1080), (4, @Other2, 3, '', '', 1280, 720);",
            new { Big = big, Small = small, Other1 = other1, Other2 = other2 });
        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10, [new() { MediaFileId = 1 }, new() { MediaFileId = 2 }]);
        await db.InsertDuplicateGroupWithMembersAsync("dhash", "50", .25, 10, [new() { MediaFileId = 3 }, new() { MediaFileId = 4 }]);

        var state = new AppState { DbPath = Path.Combine(workspace.Root, "test.db"), QuarantineRoot = Path.Combine(workspace.Root, "quarantine") };
        var groups = new GroupsViewModel(state, () => [50]);
        var quarantine = new QuarantineViewModel(state);
        groups.QuarantineChanged += () => _ = quarantine.LoadAsync();
        quarantine.Restored += groups.LoadGroupsAsync;
        await groups.LoadGroupsAsync();
        Assert.Equal(2, groups.Groups.Count);

        // Keyboard model: the current file starts at the copy most likely to go.
        var group = groups.SelectedGroup!;
        Assert.False(groups.CurrentMember!.IsBest);
        groups.SelectMember(0);
        Assert.True(groups.CurrentMember.IsCurrent);
        groups.SelectAdjacentMember(1);
        Assert.Equal(group.Members[1], groups.CurrentMember);
        groups.SelectAdjacentGroup(1);
        Assert.NotEqual(group, groups.SelectedGroup);
        groups.SelectAdjacentGroup(-1);
        Assert.Equal(group, groups.SelectedGroup);

        var victim = group.Members.Single(m => !m.IsBest);
        await groups.QuarantineCommand.ExecuteAsync(victim);
        Assert.True(groups.HasPendingAction);
        Assert.Single(groups.Groups);                     // hidden on screen...
        Assert.True(System.IO.File.Exists(victim.Path));  // ...but nothing happened yet
        // The next group in the list is selected, so the user can go on.
        Assert.NotNull(groups.SelectedGroup);

        await groups.UndoCommand.ExecuteAsync(null);
        Assert.False(groups.HasPendingAction);
        Assert.Equal(2, groups.Groups.Count);
        Assert.Equal(group.GroupId, groups.SelectedGroup!.GroupId);

        victim = groups.SelectedGroup.Members.Single(m => !m.IsBest);
        await groups.QuarantineCommand.ExecuteAsync(victim);
        await groups.CommitPendingAsync();
        Assert.False(System.IO.File.Exists(victim.Path));
        Assert.Single(groups.Groups);
        await quarantine.LoadAsync();
        var item = Assert.Single(quarantine.Items);
        Assert.True(item.Exists);

        await quarantine.RestoreCommand.ExecuteAsync(item);
        Assert.True(System.IO.File.Exists(victim.Path));
        Assert.Empty(quarantine.Items);
        Assert.Equal(2, groups.Groups.Count);
    }
}
