namespace VideoDedupe.Infrastructure;

public static class DbInitializer
{
    public static async Task EnsureCreatedAsync(AppDb db)
    {
        await db.ExecuteAsync(@"
            PRAGMA journal_mode=WAL;

           CREATE TABLE IF NOT EXISTS ScanRoot (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Path TEXT NOT NULL UNIQUE,
    IsEnabled INTEGER NOT NULL DEFAULT 1,
    AddedUtc TEXT NOT NULL,
    IncludeSubdirs INTEGER NOT NULL DEFAULT 1,
    ExcludeText TEXT NULL
);

            CREATE TABLE IF NOT EXISTS MediaFile (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Path TEXT NOT NULL UNIQUE,
    SizeBytes INTEGER NOT NULL,
    ModifiedUtc TEXT NOT NULL,
    LastScannedUtc TEXT NOT NULL
);

            -- Path is UNIQUE, which already indexes it.
            DROP INDEX IF EXISTS IX_MediaFile_Path;

           ");

        await db.EnsureColumnAsync("ScanRoot", "IncludeSubdirs", "INTEGER NOT NULL DEFAULT 1");
        await db.EnsureColumnAsync("ScanRoot", "ExcludeText", "TEXT NULL");
        await db.EnsureColumnAsync("MediaFile", "IsDeleted", "INTEGER NOT NULL DEFAULT 0");
        await db.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS FrameHash (
            MediaFileId INTEGER NOT NULL,
            PositionPct INTEGER NOT NULL,      -- e.g. 50
            Hash64 INTEGER NOT NULL,
            PRIMARY KEY(MediaFileId, PositionPct),
            FOREIGN KEY(MediaFileId) REFERENCES MediaFile(Id) ON DELETE CASCADE
            );
            ");

        await db.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS VisualHash (
            MediaFileId INTEGER NOT NULL,
            Strategy TEXT NOT NULL,
            Slot INTEGER NOT NULL,
            Hash64 INTEGER NOT NULL,
            PRIMARY KEY(MediaFileId, Strategy, Slot),
            FOREIGN KEY(MediaFileId) REFERENCES MediaFile(Id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS IX_VisualHash_MediaFileId ON VisualHash(MediaFileId);
            ");

        await db.ExecuteAsync(@"
CREATE TABLE IF NOT EXISTS DuplicateGroup (
  Id INTEGER PRIMARY KEY AUTOINCREMENT,
  Algorithm TEXT NOT NULL,           -- e.g. 'dhash'
  Frames TEXT NOT NULL,              -- e.g. '20,50,80'
  TolSec REAL NOT NULL,
  MaxAvgDist REAL NOT NULL,
  CreatedUtc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS DuplicateMember (
  GroupId INTEGER NOT NULL,
  MediaFileId INTEGER NOT NULL,
  AvgDist REAL NOT NULL,             -- avg dist to seed (or group representative)
  PRIMARY KEY(GroupId, MediaFileId),
  FOREIGN KEY(GroupId) REFERENCES DuplicateGroup(Id) ON DELETE CASCADE,
  FOREIGN KEY(MediaFileId) REFERENCES MediaFile(Id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS IX_DuplicateMember_GroupId ON DuplicateMember(GroupId);
");

        await db.ExecuteAsync(@"
CREATE TABLE IF NOT EXISTS ReviewDecision (
  Id INTEGER PRIMARY KEY AUTOINCREMENT,
  MediaFileId INTEGER NOT NULL UNIQUE,
  Decision TEXT NOT NULL,          -- 'keep' | 'quarantine' | 'skip'
  Note TEXT NULL,
  DecidedUtc TEXT NOT NULL,
  FOREIGN KEY(MediaFileId) REFERENCES MediaFile(Id) ON DELETE CASCADE
);
");


        // Rows from before mirror detection have no mirror hash; they count as not cached.
        await db.EnsureColumnAsync("FrameHash", "MirrorHash64", "INTEGER NULL");
        // Why a member belongs to its group, shown in the app.
        await db.EnsureColumnAsync("DuplicateMember", "Mirrored", "INTEGER NOT NULL DEFAULT 0");
        await db.EnsureColumnAsync("DuplicateMember", "PHashDist", "REAL NULL");
        await db.EnsureColumnAsync("DuplicateMember", "AudioBer", "REAL NULL");
        await db.EnsureColumnAsync("DuplicateMember", "IsReference", "INTEGER NOT NULL DEFAULT 0");
        await db.EnsureColumnAsync("DuplicateMember", "TrimKind", "TEXT NULL");
        await db.EnsureColumnAsync("DuplicateMember", "TrimDiffSec", "REAL NULL");

        await db.ExecuteAsync(@"
CREATE TABLE IF NOT EXISTS AudioPrint (
  MediaFileId INTEGER PRIMARY KEY,
  Strategy TEXT NOT NULL,
  Data BLOB NOT NULL,                -- empty fingerprint = file has no usable audio
  FOREIGN KEY(MediaFileId) REFERENCES MediaFile(Id) ON DELETE CASCADE
);

-- Frames at absolute timestamps, used to verify shortened copies.
CREATE TABLE IF NOT EXISTS TimedFrameHash (
  MediaFileId INTEGER NOT NULL,
  TimeMs INTEGER NOT NULL,
  Hash64 INTEGER NOT NULL,
  MirrorHash64 INTEGER NOT NULL,
  PRIMARY KEY(MediaFileId, TimeMs),
  FOREIGN KEY(MediaFileId) REFERENCES MediaFile(Id) ON DELETE CASCADE
);

-- Pairs the user marked as different videos; a build never groups them again.
CREATE TABLE IF NOT EXISTS NotDuplicatePair (
  FileA INTEGER NOT NULL,            -- always the smaller id
  FileB INTEGER NOT NULL,
  DecidedUtc TEXT NOT NULL,
  PRIMARY KEY(FileA, FileB),
  CHECK(FileA < FileB),
  FOREIGN KEY(FileA) REFERENCES MediaFile(Id) ON DELETE CASCADE,
  FOREIGN KEY(FileB) REFERENCES MediaFile(Id) ON DELETE CASCADE
);
");

        // Keep files from preferred folders, clear avoided folders first (FolderPriorities).
        await db.EnsureColumnAsync("ScanRoot", "Priority", "INTEGER NOT NULL DEFAULT 0");
        // Where a quarantined file went, so it can be restored.
        await db.EnsureColumnAsync("ReviewDecision", "QuarantinePath", "TEXT NULL");

        await db.EnsureColumnAsync("MediaFile", "DurationSec", "REAL");
        await db.EnsureColumnAsync("MediaFile", "Width", "INTEGER");
        await db.EnsureColumnAsync("MediaFile", "Height", "INTEGER");
        await db.EnsureColumnAsync("MediaFile", "Fps", "REAL");
        await db.EnsureColumnAsync("MediaFile", "VideoCodec", "TEXT");
        await db.EnsureColumnAsync("MediaFile", "Container", "TEXT");

        // Cached hashes are only comparable with hashes from the same algorithm. After an
        // algorithm change they are recomputed once instead of silently mixing both kinds.
        await db.ExecuteAsync(@"
CREATE TABLE IF NOT EXISTS Meta (
  Key TEXT PRIMARY KEY,
  Value TEXT NOT NULL
);
INSERT INTO Meta(Key, Value) VALUES ('HashVersion', '') ON CONFLICT(Key) DO NOTHING;
DELETE FROM FrameHash WHERE (SELECT Value FROM Meta WHERE Key='HashVersion') IS NOT @Version;
DELETE FROM VisualHash WHERE (SELECT Value FROM Meta WHERE Key='HashVersion') IS NOT @Version;
DELETE FROM TimedFrameHash WHERE (SELECT Value FROM Meta WHERE Key='HashVersion') IS NOT @Version;
UPDATE Meta SET Value=@Version WHERE Key='HashVersion';", new { Version = Core.DHashService.Version });
    }
}
