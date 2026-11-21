# VideoDedupe

Finds duplicate videos – re-encodes, other resolutions, letterboxed or slightly brightened copies –
and helps you keep the best one. Desktop app (Avalonia) plus a command-line tool.

## Install

Download `VideoDedupe-<version>-setup.exe` from the GitHub releases and run it. It installs for
the current user (no administrator rights) and includes FFmpeg. The zip contains the same files
for running without installation.

## Requirements for building

- .NET 10 SDK
- `ffmpeg` and `ffprobe` on `PATH`, or next to the executable (release builds ship them there)

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
6. **Shortened copies** (optional, `--trimmed=true` or "Find shortened copies" in the app):
   copies with a cut intro, credits or ads. Every video of at least 3 minutes gets two more
   frames, 60 s after the start and 60 s before the end; files of different length that share
   one of them are checked at the shorter file's own positions in the longer one. If such a
   frame is nearly flat (a fade, a black title card), a second one 90 s from that end is taken.
   Openings shared by many files (TV series) are ignored. This makes the first run up to ~65 %
   slower (measured on 1 GB files from a hard disk); later runs reuse the cached frames.
7. **Excerpts** (optional, `--excerpts=true` or "Find excerpts"): clips of 20 s to 10 min cut
   out of a video at least ~1.4 times as long. Every keyframe of the long videos is read, and
   short videos are sampled 5 times per second; matching pictures vote for the point where the
   clip starts, which is then checked at the clip's own frame positions. The first run reads
   every video completely once (about 3–10 s per 1 GB file); later runs only read new files.
8. **Identical files**: files of exactly the same size are compared by 18 content samples
   (first and last MB, 16 blocks in between). This needs no ffmpeg, so it also finds copies of
   files ffprobe cannot read.

Two black frames match in any two videos. When most compared positions are black in both files,
the match is decided by the soundtrack if both have one; without sound it counts as before.

Pairs you mark as "not a duplicate" in the app are remembered and never grouped again.
"Clean up all groups" keeps the best copy of each group (or the files marked as keep) and moves
the others to the quarantine folder or the Recycle Bin (Settings) after showing the full list.

The best copy is the file marked as keep; otherwise the one in the folder with the highest
priority (Folders tab: "keep files from here" or "remove copies from here first"); then the
highest resolution; then the codec and file format chosen in Settings → Best copy (e.g. prefer
HEVC/AV1, or MP4); then the largest file (or the smallest, if chosen) and the newest.

Hashes are cached in the database and recomputed only when a file changes or the hash algorithm
version changes. Hashing runs several ffmpeg processes in parallel (`--parallel`, default: half
the CPU cores, 2–8).

## Desktop app

```powershell
dotnet run --project src/VideoDedupe.App
```

Database, settings and the default quarantine folder live in `%LOCALAPPDATA%\VideoDedupe`.
A database from an older version (next to the executable) is copied there on first start.

- **Delete** sends a file to the Recycle Bin without asking. For a few seconds the action can be
  undone (Ctrl+Z or the bar at the bottom); the file is only touched after that.
- **Keyboard**: ↑↓ group, ←→ or 1–9 file, Del delete, Q quarantine, B keep, V compare,
  N not a duplicate, Enter open, E show in Explorer, Ctrl+Z undo, Ctrl+F search, F5 reload.
- **Quarantine tab**: restore files to their original folder, or delete them.
- **Export as CSV** (⋯ next to "Clean up all"): all groups as a table for Excel, in the app's
  language (German: `;` and decimal comma).
- **Compare** (V): the selected file and the best copy side by side at the same moment, at full
  resolution with zoom; shortened copies and excerpts are aligned.
- **History** (⋯ or Quarantine tab): every file deleted, quarantined or restored, with totals.
- **Updates**: at start the app asks GitHub whether a newer release exists and shows a button to
  its download page (can be switched off in Settings; development builds never ask).

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

Releases: pushing a tag `v1.2.0` runs `.github/workflows/release.yml`, which tests, publishes the
app self-contained, adds the LGPL build of FFmpeg (checksum-verified), builds the installer from
`installer/VideoDedupe.iss` with Inno Setup and attaches setup and zip to a GitHub release.
"Run workflow" builds the same files as a download without a release.

## License

GPL-3.0, see [LICENSE.txt](LICENSE.txt).
