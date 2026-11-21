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
            // Cascading deletes rely on this; do not depend on the native library's default.
            var cn = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = _dbPath,
                ForeignKeys = true
            }.ToString());
            cn.Open();
            return cn;
        }

        public async Task ExecuteAsync(string sql, object? args = null)
        {
            using var cn = Open();
            await cn.ExecuteAsync(sql, args);
        }

        public async Task<string?> GetMetaAsync(string key)
        {
            using var cn = Open();
            return await cn.ExecuteScalarAsync<string?>("SELECT Value FROM Meta WHERE Key=@Key", new { Key = key });
        }

        internal async Task EnsureColumnAsync(string table, string column, string type)
        {
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            var exists = await cn.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM pragma_table_info(@Table) WHERE name = @Column COLLATE NOCASE",
                new { Table = table, Column = column }, tx);
            // Identifiers and type are internal schema constants, never user input.
            if (exists == 0)
                await cn.ExecuteAsync($"ALTER TABLE {table} ADD COLUMN {column} {type};", transaction: tx);
            tx.Commit();
        }
        public async Task DeleteScanRootAsync(long id)
        {
            using var cn = Open();
            await cn.ExecuteAsync("DELETE FROM ScanRoot WHERE Id=@Id;", new { Id = id });
        }

        public async Task SetScanRootEnabledAsync(long id, bool enabled)
        {
            using var cn = Open();
            await cn.ExecuteAsync(
                "UPDATE ScanRoot SET IsEnabled=@E WHERE Id=@Id;",
                new { Id = id, E = enabled ? 1 : 0 });
        }

        public async Task MarkMediaFileDeletedAsync(long mediaFileId)
        {
            using var cn = Open();
            await cn.ExecuteAsync(
                "UPDATE MediaFile SET IsDeleted=1 WHERE Id=@Id;",
                new { Id = mediaFileId });
        }

        public sealed class ScanRootRow
        {
            public long Id { get; set; }
            public string Path { get; set; } = "";
            public long IsEnabled { get; set; }
            public string AddedUtc { get; set; } = "";
            public long IncludeSubdirs { get; init; } = 1;
            public string? ExcludeText { get; init; }
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
        public async Task UpsertMediaFileStubAsync(string path, long sizeBytes, DateTime modifiedUtc)
        {
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            var modified = modifiedUtc.ToString("O");
            await InvalidateFrameHashesAsync(cn, tx, path, sizeBytes, modified);

            await cn.ExecuteAsync(@"
INSERT INTO MediaFile(Path, SizeBytes, ModifiedUtc, LastScannedUtc)
VALUES (@Path, @SizeBytes, @ModifiedUtc, @Now)
ON CONFLICT(Path) DO UPDATE SET
    SizeBytes = excluded.SizeBytes,
    ModifiedUtc = excluded.ModifiedUtc,
    LastScannedUtc = excluded.LastScannedUtc;
", new
            {
                Path = path,
                SizeBytes = sizeBytes,
                ModifiedUtc = modified,
                Now = DateTime.UtcNow.ToString("O")
            }, tx);
            tx.Commit();
        }

        public async Task<long> AddScanRootAsync(string path)
        {
            var normalized = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
            using var cn = Open();

            return await cn.ExecuteScalarAsync<long>(@"
INSERT INTO ScanRoot(Path, IsEnabled, AddedUtc, IncludeSubdirs, ExcludeText)
VALUES (@Path, 1, @Utc, 1, NULL)
RETURNING Id;
", new
            {
                Path = normalized,
                Utc = DateTime.UtcNow.ToString("O")
            });

        }
        public async Task UpdateScanRootOptionsAsync(long id, bool includeSubdirs, string? excludeText)
        {
            using var cn = Open();
            await cn.ExecuteAsync(@"
UPDATE ScanRoot
SET IncludeSubdirs = @Inc,
    ExcludeText = @Ex
WHERE Id = @Id;
", new
            {
                Id = id,
                Inc = includeSubdirs ? 1 : 0,
                Ex = string.IsNullOrWhiteSpace(excludeText) ? null : excludeText
            });
        }
        public async Task<List<ScanRootRow>> ListScanRootsAsync()
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<ScanRootRow>("SELECT * FROM ScanRoot ORDER BY Id;");
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
            using var tx = cn.BeginTransaction();
            await InvalidateFrameHashesAsync(cn, tx, row.Path, row.SizeBytes, row.ModifiedUtc);
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
                    Container=excluded.Container,
                    IsDeleted=0;
                    ", row, tx);
            tx.Commit();
        }

        private static async Task InvalidateFrameHashesAsync(SqliteConnection cn, SqliteTransaction tx,
            string path, long sizeBytes, string modifiedUtc)
        {
            var args = new { Path = path, SizeBytes = sizeBytes, ModifiedUtc = modifiedUtc };
            await cn.ExecuteAsync(@"
DELETE FROM FrameHash
WHERE MediaFileId IN (
    SELECT Id FROM MediaFile
    WHERE Path = @Path AND (SizeBytes IS NOT @SizeBytes OR ModifiedUtc IS NOT @ModifiedUtc)
);", args, tx);
            await cn.ExecuteAsync(@"
DELETE FROM VisualHash
WHERE MediaFileId IN (
    SELECT Id FROM MediaFile
    WHERE Path = @Path AND (SizeBytes IS NOT @SizeBytes OR ModifiedUtc IS NOT @ModifiedUtc)
);
DELETE FROM AudioPrint
WHERE MediaFileId IN (
    SELECT Id FROM MediaFile
    WHERE Path = @Path AND (SizeBytes IS NOT @SizeBytes OR ModifiedUtc IS NOT @ModifiedUtc)
);", args, tx);
        }

        public sealed record IndexedFileState(string Path, long SizeBytes, string ModifiedUtc, double? DurationSec, long IsDeleted);

        public async Task<Dictionary<string, IndexedFileState>> ListIndexedFileStatesAsync()
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<IndexedFileState>(
                "SELECT Path, SizeBytes, ModifiedUtc, DurationSec, IsDeleted FROM MediaFile;");
            return rows.ToDictionary(r => r.Path, StringComparer.Ordinal);
        }

        /// <summary>Active files whose path lies below <paramref name="rootPath"/>.</summary>
        public async Task<List<(long Id, string Path)>> ListActiveFilesUnderAsync(string rootPath)
        {
            var prefix = Path.TrimEndingDirectorySeparator(rootPath) + Path.DirectorySeparatorChar;
            using var cn = Open();
            // substr avoids LIKE wildcards ('_', '%') that are legal in folder names.
            var rows = await cn.QueryAsync<(long Id, string Path)>(@"
SELECT Id, Path FROM MediaFile
WHERE IsDeleted = 0 AND substr(Path, 1, length(@Prefix)) = @Prefix COLLATE NOCASE;",
                new { Prefix = prefix });
            return rows.ToList();
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
                WHERE DurationSec IS NOT NULL AND IsDeleted = 0
                -- Deterministic seeds: ids depend on the (parallel) indexing order.
                ORDER BY DurationSec, Path
");
            return rows.ToList();
        }

        /// <param name="mirrorHash64">Hash of the mirror image; defaults to <paramref name="hash64"/>
        /// (a symmetric picture).</param>
        public Task UpsertFrameHashAsync(long mediaFileId, int positionPct, ulong hash64, ulong? mirrorHash64 = null) =>
            UpsertFrameHashesAsync(mediaFileId,
                new Dictionary<int, Core.FrameHash> { [positionPct] = new(hash64, mirrorHash64 ?? hash64) });

        /// <summary>Stores several positions of one file in a single transaction.</summary>
        public async Task UpsertFrameHashesAsync(long mediaFileId, IReadOnlyDictionary<int, Core.FrameHash> hashes)
        {
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            await cn.ExecuteAsync(@"
                INSERT INTO FrameHash(MediaFileId, PositionPct, Hash64, MirrorHash64)
                VALUES (@MediaFileId, @PositionPct, @Hash64, @MirrorHash64)
                ON CONFLICT(MediaFileId, PositionPct) DO UPDATE SET
                    Hash64=excluded.Hash64, MirrorHash64=excluded.MirrorHash64;",
                hashes.Select(h => new
                {
                    MediaFileId = mediaFileId,
                    PositionPct = h.Key,
                    Hash64 = unchecked((long)h.Value.Normal),
                    MirrorHash64 = unchecked((long)h.Value.Mirrored)
                }),
                tx);
            tx.Commit();
        }

        public async Task<ulong?> GetFrameHashAsync(long mediaFileId, int positionPct)
        {
            using var cn = Open();
            var v = await cn.ExecuteScalarAsync<long?>(
                "SELECT Hash64 FROM FrameHash WHERE MediaFileId=@Id AND PositionPct=@Pct",
                new { Id = mediaFileId, Pct = positionPct });

            return v is null ? null : unchecked((ulong)v.Value);
        }

        /// <summary>All cached frame hashes, keyed by file and position (one query instead of one per frame).</summary>
        /// <remarks>Rows without a mirror hash (older databases) are left out, so they get recomputed.</remarks>
        public async Task<Dictionary<long, Dictionary<int, Core.FrameHash>>> GetAllFrameHashesAsync()
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<(long MediaFileId, int PositionPct, long Hash64, long MirrorHash64)>(
                "SELECT MediaFileId, PositionPct, Hash64, MirrorHash64 FROM FrameHash WHERE MirrorHash64 IS NOT NULL;");
            return rows.GroupBy(r => r.MediaFileId).ToDictionary(
                g => g.Key,
                g => g.ToDictionary(r => r.PositionPct,
                    r => new Core.FrameHash(unchecked((ulong)r.Hash64), unchecked((ulong)r.MirrorHash64))));
        }

        public Task UpsertVisualHashAsync(long mediaFileId, string strategy, int slot, ulong hash64) =>
            UpsertVisualHashesAsync(mediaFileId, strategy, new Dictionary<int, ulong> { [slot] = hash64 });

        public async Task UpsertVisualHashesAsync(long mediaFileId, string strategy, IReadOnlyDictionary<int, ulong> hashes)
        {
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            await cn.ExecuteAsync(@"
INSERT INTO VisualHash(MediaFileId, Strategy, Slot, Hash64)
VALUES (@MediaFileId, @Strategy, @Slot, @Hash64)
ON CONFLICT(MediaFileId, Strategy, Slot) DO UPDATE SET Hash64=excluded.Hash64;",
                hashes.Select(h => new { MediaFileId = mediaFileId, Strategy = strategy, Slot = h.Key, Hash64 = unchecked((long)h.Value) }),
                tx);
            tx.Commit();
        }

        public async Task<byte[]?> GetAudioPrintAsync(long mediaFileId, string strategy)
        {
            using var cn = Open();
            return await cn.ExecuteScalarAsync<byte[]?>(
                "SELECT Data FROM AudioPrint WHERE MediaFileId=@Id AND Strategy=@Strategy",
                new { Id = mediaFileId, Strategy = strategy });
        }

        public async Task UpsertAudioPrintAsync(long mediaFileId, string strategy, byte[] data)
        {
            using var cn = Open();
            await cn.ExecuteAsync(@"
INSERT INTO AudioPrint(MediaFileId, Strategy, Data) VALUES (@Id, @Strategy, @Data)
ON CONFLICT(MediaFileId) DO UPDATE SET Strategy=excluded.Strategy, Data=excluded.Data;",
                new { Id = mediaFileId, Strategy = strategy, Data = data });
        }

        /// <summary>Records that every pair among <paramref name="mediaFileIds"/> shows different videos.</summary>
        public Task MarkNotDuplicateAsync(IEnumerable<long> mediaFileIds)
        {
            var ids = mediaFileIds.Distinct().Order().ToArray();
            return AddNotDuplicatePairsAsync(
                from a in ids from b in ids where a < b select (a, b));
        }

        /// <summary>Records that <paramref name="mediaFileId"/> differs from each of <paramref name="others"/>.</summary>
        public Task MarkNotDuplicateOfAsync(long mediaFileId, IEnumerable<long> others) =>
            AddNotDuplicatePairsAsync(others.Where(o => o != mediaFileId).Distinct()
                .Select(o => (Math.Min(o, mediaFileId), Math.Max(o, mediaFileId))));

        private async Task AddNotDuplicatePairsAsync(IEnumerable<(long A, long B)> pairs)
        {
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            var utc = DateTime.UtcNow.ToString("O");
            await cn.ExecuteAsync(@"
INSERT INTO NotDuplicatePair(FileA, FileB, DecidedUtc) VALUES (@A, @B, @Utc)
ON CONFLICT(FileA, FileB) DO NOTHING;",
                pairs.Select(p => new { p.A, p.B, Utc = utc }), tx);
            tx.Commit();
        }

        /// <summary>All pairs marked as different videos, smaller id first.</summary>
        public async Task<HashSet<(long A, long B)>> GetNotDuplicatePairsAsync()
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<(long A, long B)>("SELECT FileA, FileB FROM NotDuplicatePair;");
            return rows.ToHashSet();
        }

        public async Task RemoveGroupMemberAsync(long groupId, long mediaFileId)
        {
            using var cn = Open();
            await cn.ExecuteAsync("DELETE FROM DuplicateMember WHERE GroupId=@G AND MediaFileId=@F;",
                new { G = groupId, F = mediaFileId });
        }

        public async Task DeleteGroupAsync(long groupId)
        {
            using var cn = Open();
            await cn.ExecuteAsync("DELETE FROM DuplicateGroup WHERE Id=@G;", new { G = groupId });
        }

        public async Task<Dictionary<int, ulong>> GetVisualHashesAsync(long mediaFileId, string strategy)
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<(int Slot, long Hash64)>(@"
SELECT Slot, Hash64 FROM VisualHash
WHERE MediaFileId=@MediaFileId AND Strategy=@Strategy;", new { MediaFileId = mediaFileId, Strategy = strategy });
            return rows.ToDictionary(row => row.Slot, row => unchecked((ulong)row.Hash64));
        }


        public async Task ClearDuplicateGroupsAsync()
        {
            using var cn = Open();
            await cn.ExecuteAsync("DELETE FROM DuplicateGroup;");
        }

        public async Task<long> InsertDuplicateGroupWithMembersAsync(
            string algorithm, string frames, double tolSec, double maxAvgDist,
            IEnumerable<DuplicateMemberRow> members, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            var groupId = await cn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO DuplicateGroup(Algorithm, Frames, TolSec, MaxAvgDist, CreatedUtc)
VALUES (@Algorithm, @Frames, @TolSec, @MaxAvgDist, @Utc)
RETURNING Id;
", new
            {
                Algorithm = algorithm,
                Frames = frames,
                TolSec = tolSec,
                MaxAvgDist = maxAvgDist,
                Utc = DateTime.UtcNow.ToString("O")
            }, transaction: tx, cancellationToken: ct));

            foreach (var m in members)
            {
                ct.ThrowIfCancellationRequested();
                await cn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO DuplicateMember(GroupId, MediaFileId, AvgDist, Mirrored, PHashDist, AudioBer, IsReference)
VALUES (@GroupId, @MediaFileId, @AvgDist, @Mirrored, @PHashDist, @AudioBer, @IsReference);
", new
                {
                    GroupId = groupId,
                    m.MediaFileId,
                    m.AvgDist,
                    Mirrored = m.Mirrored ? 1 : 0,
                    m.PHashDist,
                    m.AudioBer,
                    IsReference = m.IsReference ? 1 : 0
                }, transaction: tx, cancellationToken: ct));
            }

            ct.ThrowIfCancellationRequested();
            tx.Commit();
            return groupId;
        }

        private sealed class MemberJoinRow
        {
            public long GroupId { get; set; }
            public double AvgDist { get; set; }
            public long Mirrored { get; set; }
            public double? PHashDist { get; set; }
            public double? AudioBer { get; set; }
            public long IsReference { get; set; }
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

        public async Task UpsertDecisionAsync(long mediaFileId, string decision, string? note = null)
        {
            using var cn = Open();
            await cn.ExecuteAsync(@"
INSERT INTO ReviewDecision(MediaFileId, Decision, Note, DecidedUtc)
VALUES (@MediaFileId, @Decision, @Note, @Utc)
ON CONFLICT(MediaFileId) DO UPDATE SET
  Decision=excluded.Decision,
  Note=excluded.Note,
  DecidedUtc=excluded.DecidedUtc;
", new
            {
                MediaFileId = mediaFileId,
                Decision = decision,
                Note = note,
                Utc = DateTime.UtcNow.ToString("O")
            });
        }

        /// <summary>Records the move and removes the file from the active library in one step.</summary>
        public async Task MarkMediaFileQuarantinedAsync(long mediaFileId, string quarantinePath)
        {
            using var cn = Open();
            using var tx = cn.BeginTransaction();
            await cn.ExecuteAsync(@"
INSERT INTO ReviewDecision(MediaFileId, Decision, Note, DecidedUtc)
VALUES (@Id, 'quarantine', @Note, @Utc)
ON CONFLICT(MediaFileId) DO UPDATE SET
  Decision=excluded.Decision,
  Note=excluded.Note,
  DecidedUtc=excluded.DecidedUtc;
UPDATE MediaFile SET IsDeleted=1 WHERE Id=@Id;",
                new { Id = mediaFileId, Note = $"moved to: {quarantinePath}", Utc = DateTime.UtcNow.ToString("O") }, tx);
            tx.Commit();
        }

        public async Task<Dictionary<long, string>> GetDecisionsAsync(IEnumerable<long> mediaFileIds)
        {
            using var cn = Open();
            var rows = await cn.QueryAsync<(long MediaFileId, string Decision)>(
                "SELECT MediaFileId, Decision FROM ReviewDecision WHERE MediaFileId IN @Ids",
                new { Ids = mediaFileIds.Distinct().ToArray() });
            return rows.ToDictionary(r => r.MediaFileId, r => r.Decision);
        }

        public async Task<(string Decision, string? Note, string DecidedUtc)?> GetDecisionAsync(long mediaFileId)
        {
            using var cn = Open();
            return await cn.QueryFirstOrDefaultAsync<(string Decision, string? Note, string DecidedUtc)?>(
                "SELECT Decision, Note, DecidedUtc FROM ReviewDecision WHERE MediaFileId=@Id",
                new { Id = mediaFileId });
        }

        public async Task<List<(DuplicateGroupRow Group, List<GroupMember> Members)>> ListDuplicateGroupsWithMembersAsync(int? take = null)
        {
            using var cn = Open();

            var groupQuery = @"
SELECT Id, Algorithm, Frames, TolSec, MaxAvgDist, CreatedUtc
FROM DuplicateGroup
WHERE 2 <= (
  SELECT COUNT(*)
  FROM DuplicateMember member
  JOIN MediaFile file ON file.Id = member.MediaFileId
  WHERE member.GroupId = DuplicateGroup.Id AND file.IsDeleted = 0
)
ORDER BY Id DESC
" + (take is null ? ";" : "LIMIT @Take;");
            var groups = (await cn.QueryAsync<DuplicateGroupRow>(groupQuery, new { Take = take })).ToList();

            // One query for all members instead of one per group.
            var membersByGroup = (await cn.QueryAsync<MemberJoinRow>(@"
SELECT
  m.GroupId as GroupId,
  m.AvgDist as AvgDist,
  m.Mirrored as Mirrored,
  m.PHashDist as PHashDist,
  m.AudioBer as AudioBer,
  m.IsReference as IsReference,
  f.Id as Id,
  f.Path as Path,
  f.SizeBytes as SizeBytes,
  f.ModifiedUtc as ModifiedUtc,
  f.LastScannedUtc as LastScannedUtc,
  f.DurationSec as DurationSec,
  f.Width as Width,
  f.Height as Height,
  f.Fps as Fps,
  f.VideoCodec as VideoCodec,
  f.Container as Container
FROM DuplicateMember m
JOIN MediaFile f ON f.Id = m.MediaFileId
WHERE m.GroupId IN @GroupIds AND f.IsDeleted = 0
ORDER BY m.AvgDist ASC;
", new { GroupIds = groups.Select(g => g.Id).ToArray() }))
                .ToLookup(r => r.GroupId);

            var result = new List<(DuplicateGroupRow, List<GroupMember>)>();

            foreach (var g in groups)
            {
                var members = membersByGroup[g.Id].Select(r => new GroupMember
                (
                    new MediaFileRow
                    {
                        Id = r.Id,
                        Path = r.Path,
                        SizeBytes = r.SizeBytes,
                        ModifiedUtc = r.ModifiedUtc,
                        LastScannedUtc = r.LastScannedUtc,
                        DurationSec = r.DurationSec,
                        Width = r.Width,
                        Height = r.Height,
                        Fps = r.Fps,
                        VideoCodec = r.VideoCodec,
                        Container = r.Container
                    },
                    r.AvgDist,
                    r.Mirrored != 0,
                    r.PHashDist,
                    r.AudioBer,
                    r.IsReference != 0
                )).ToList();

                result.Add((g, members));
            }

            return result;
        }

        public sealed class DuplicateGroupRow
        {
            public long Id { get; set; }
            public string Algorithm { get; set; } = "";
            public string Frames { get; set; } = "";
            public double TolSec { get; set; }
            public double MaxAvgDist { get; set; }
            public string CreatedUtc { get; set; } = "";
        }

        public sealed class DuplicateMemberRow
        {
            public long GroupId { get; set; }
            public long MediaFileId { get; set; }
            /// <summary>Average dHash distance to the group's reference file (0–64).</summary>
            public double AvgDist { get; set; }
            /// <summary>The file matched as a mirror image of the reference.</summary>
            public bool Mirrored { get; set; }
            /// <summary>Scene pHash distance, when a borderline match was re-checked.</summary>
            public double? PHashDist { get; set; }
            /// <summary>Audio bit error rate, when the soundtrack was compared.</summary>
            public double? AudioBer { get; set; }
            /// <summary>The file the others were compared with.</summary>
            public bool IsReference { get; set; }
        }

        /// <summary>A group member with the evidence that put it there.</summary>
        public sealed record GroupMember(MediaFileRow File, double AvgDist, bool Mirrored = false,
            double? PHashDist = null, double? AudioBer = null, bool IsReference = false);
    }
}
