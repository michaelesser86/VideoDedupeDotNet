# VideoDedupe

Finds duplicate videos – re-encodes, other resolutions, letterboxed or slightly brightened copies –
and helps you keep the best one. Desktop app (Avalonia) plus a command-line tool.

## Requirements

- .NET 10 SDK
- `ffmpeg` and `ffprobe` on `PATH`

## How it works

1. **Index** (`scan-index`): finds video files under the enabled scan roots and reads duration,
   resolution and codec with ffprobe. Unchanged files (same size and modification time) are not
   probed again; files that disappeared from a reachable root are marked as deleted.
2. **Candidates**: only files whose durations differ by at most the tolerance (`--tol`, seconds)
   can be duplicates.
3. **dHash**: for each candidate, ffmpeg extracts grayscale frames at fixed positions
   (`--frames`, percent of the duration) in one process per file. Black bars are cropped, then a
   64-bit difference hash is computed per frame. Files whose average Hamming distance to the
   group's seed is at most `--maxdist` form a group.
4. **Refinement**: matches close to the threshold (within `--margin`) are re-checked with
   perceptual hashes of scene-change frames (`--refine-frames`, `--scene-threshold`,
   `--max-phash`). Scenes are matched by best fit, so a re-encode with an extra cut still matches.
   What the scenes do not confirm is checked against the soundtrack: two minutes around the
   middle of each file are fingerprinted and compared (`--audio`, `--max-audio-ber`).
5. **Mirrored copies**: every frame hash is also computed for the mirror image, so videos
   flipped left to right match too (`--mirror`).

Pairs you mark as "not a duplicate" in the app are remembered and never grouped again.
"Clean up all groups" keeps the best copy of each group (or the files marked as keep) and moves
the others to the quarantine folder after showing the full list.

Hashes are cached in the database and recomputed only when a file changes or the hash algorithm
version changes. Hashing runs several ffmpeg processes in parallel (`--parallel`, default: half
the CPU cores, 2–8).

## Desktop app

```powershell
dotnet run --project src/VideoDedupe.App
```

Database, settings and the default quarantine folder live in `%LOCALAPPDATA%\VideoDedupe`.
A database from an older version (next to the executable) is copied there on first start.

## Command line

```powershell
dotnet run --project src/VideoDedupe.Cli -- roots-add "D:\Videos"
dotnet run --project src/VideoDedupe.Cli -- scan-index
dotnet run --project src/VideoDedupe.Cli -- dupe-build --maxdist=10
dotnet run --project src/VideoDedupe.Cli -- dupe-list --take=50
```

Run without arguments for all commands and options; `--db=<file>` selects another database.

## Development

```powershell
dotnet test
```

The tests do not need ffmpeg, except one that compares extraction modes on a generated clip and
is skipped without it. Restore fails on packages with known high-severity vulnerabilities.

## License

GPL-3.0, see [LICENSE.txt](LICENSE.txt).
