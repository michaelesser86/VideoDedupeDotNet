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
    }
}
