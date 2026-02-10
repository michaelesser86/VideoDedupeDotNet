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

");
    }
}
