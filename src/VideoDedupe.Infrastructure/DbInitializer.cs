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
    ExcludeGlob TEXT NULL
);

            CREATE TABLE IF NOT EXISTS MediaFile (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Path TEXT NOT NULL UNIQUE,
    SizeBytes INTEGER NOT NULL,
    ModifiedUtc TEXT NOT NULL,
    LastScannedUtc TEXT NOT NULL
);

            CREATE INDEX IF NOT EXISTS IX_MediaFile_Path ON MediaFile(Path);

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


        await db.EnsureColumnAsync("MediaFile", "DurationSec", "REAL");
        await db.EnsureColumnAsync("MediaFile", "Width", "INTEGER");
        await db.EnsureColumnAsync("MediaFile", "Height", "INTEGER");
        await db.EnsureColumnAsync("MediaFile", "Fps", "REAL");
        await db.EnsureColumnAsync("MediaFile", "VideoCodec", "TEXT");
        await db.EnsureColumnAsync("MediaFile", "Container", "TEXT");
    }

}
