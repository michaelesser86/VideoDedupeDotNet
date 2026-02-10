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
