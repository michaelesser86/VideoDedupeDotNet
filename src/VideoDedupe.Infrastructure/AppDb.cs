using Dapper;

using Microsoft.Data.Sqlite;

namespace VideoDedupe.Infrastructure
{
    public sealed class AppDb
    {
        private readonly string _dbPath;

        public AppDb(string dbPath)
        {
            _dbPath = dbPath;
        }

        private SqliteConnection Open()
        {
            var cn = new SqliteConnection($"Data Source={_dbPath}");
            cn.Open();
            return cn;
        }

        public async Task ExecuteAsync(string sql, object? args = null)
        {
            using var cn = Open();
            await cn.ExecuteAsync(sql, args);
        }

        public sealed class ScanRootRow
        {
            public long Id { get; set; }
            public string Path { get; set; } = "";
            public long IsEnabled { get; set; }
            public string AddedUtc { get; set; } = "";
        }

        public sealed class MediaFileRow
        {
            public long Id { get; set; }
            public string Path { get; set; } = "";
            public long SizeBytes { get; set; }
            public string ModifiedUtc { get; set; } = "";
            public string LastScannedUtc { get; set; } = "";
            public double? DurationSec { get; set; }
            public int? Width { get; set; }
            public int? Height { get; set; }
            public double? Fps { get; set; }
            public string? VideoCodec { get; set; }
            public string? Container { get; set; }
        }

        public async Task<long> AddScanRootAsync(string path)
        {
            var normalized = Normalize(path);
            using var cn = Open();

            await cn.ExecuteAsync(
                "INSERT OR IGNORE INTO ScanRoot(Path, IsEnabled, AddedUtc) VALUES (@Path, 1, @Utc)",
                new
                {
                    Path = normalized,
                    Utc = DateTime.UtcNow.ToString("O", System.Globalization.CultureInfo.InvariantCulture)
                });

            return await cn.ExecuteScalarAsync<long>(
                "SELECT Id FROM ScanRoot WHERE Path = @Path",
                new { Path = normalized });
        }

        public async Task<List<ScanRootRow>> ListScanRootsAsync()
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<ScanRootRow>(
                "SELECT Id, Path, IsEnabled, AddedUtc FROM ScanRoot ORDER BY Id");
            return rows.ToList();
        }

        public async Task ToggleScanRootAsync(long id)
        {
            using var cn = Open();
            await cn.ExecuteAsync(
                "UPDATE ScanRoot SET IsEnabled = CASE WHEN IsEnabled=1 THEN 0 ELSE 1 END WHERE Id=@Id",
                new { Id = id });
        }

        public async Task UpsertMediaFileAsync(MediaFileRow row)
        {
            using var cn = Open();
            await cn.ExecuteAsync(@"
                INSERT INTO MediaFile(Path, SizeBytes, ModifiedUtc, LastScannedUtc, DurationSec, Width, Height, Fps, VideoCodec, Container)
                VALUES (@Path, @SizeBytes, @ModifiedUtc, @LastScannedUtc, @DurationSec, @Width, @Height, @Fps, @VideoCodec, @Container)
                ON CONFLICT(Path) DO UPDATE SET
                    SizeBytes=excluded.SizeBytes,
                    ModifiedUtc=excluded.ModifiedUtc,
                    LastScannedUtc=excluded.LastScannedUtc,
                    DurationSec=excluded.DurationSec,
                    Width=excluded.Width,
                    Height=excluded.Height,
                    Fps=excluded.Fps,
                    VideoCodec=excluded.VideoCodec,
                    Container=excluded.Container;
                    ", row);
        }

        public async Task<List<MediaFileRow>> ListMediaFilesAsync(int take = 50)
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<MediaFileRow>(
                "SELECT Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc FROM MediaFile ORDER BY Id DESC LIMIT @Take",
                new { Take = take });
            return rows.ToList();
        }

        public async Task<List<MediaFileRow>> ListMediaFilesForCandidatesAsync()
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<MediaFileRow>(@"
                SELECT
                Id, Path, SizeBytes, ModifiedUtc, LastScannedUtc,
                DurationSec, Width, Height, Fps, VideoCodec, Container
                FROM MediaFile
                WHERE DurationSec IS NOT NULL AND Width IS NOT NULL AND Height IS NOT NULL
");
            return rows.ToList();
        }

        public async Task UpsertFrameHashAsync(long mediaFileId, int positionPct, ulong hash64)
        {
            using var cn = Open();
            await cn.ExecuteAsync(@"
                INSERT INTO FrameHash(MediaFileId, PositionPct, Hash64)
                VALUES (@MediaFileId, @PositionPct, @Hash64)
                ON CONFLICT(MediaFileId, PositionPct) DO UPDATE SET Hash64=excluded.Hash64;
                ", new { MediaFileId = mediaFileId, PositionPct = positionPct, Hash64 = unchecked((long)hash64) });
        }

        public async Task<ulong?> GetFrameHashAsync(long mediaFileId, int positionPct)
        {
            using var cn = Open();
            var v = await cn.ExecuteScalarAsync<long?>(
                "SELECT Hash64 FROM FrameHash WHERE MediaFileId=@Id AND PositionPct=@Pct",
                new { Id = mediaFileId, Pct = positionPct });

            return v is null ? null : unchecked((ulong)v.Value);
        }

        private static string Normalize(string path) => Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar).ToUpperInvariant();
    }
}
