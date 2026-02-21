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
            AddedUtc TEXT NOT NULL
            IncludeSubdirs INTEGER NOT NULL DEFAULT 1
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


        await TryAddColumn(db, "MediaFile", "DurationSec", "REAL");
        await TryAddColumn(db, "MediaFile", "Width", "INTEGER");
        await TryAddColumn(db, "MediaFile", "Height", "INTEGER");
        await TryAddColumn(db, "MediaFile", "Fps", "REAL");
        await TryAddColumn(db, "MediaFile", "VideoCodec", "TEXT");
        await TryAddColumn(db, "MediaFile", "Container", "TEXT");
    }

    private static async Task TryAddColumn(AppDb db, string table, string column, string type)
    {
        try
        {
            await db.ExecuteAsync($"ALTER TABLE {table} ADD COLUMN {column} {type};");
        }
        catch
        {
            // ignore "duplicate column name" and similar
        }
    }
}
