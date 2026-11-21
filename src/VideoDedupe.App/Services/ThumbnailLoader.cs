using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Avalonia.Media.Imaging;

using VideoDedupe.App.ViewModels;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.Services;

/// <summary>
/// Extracts and caches member thumbnails. Runs on the UI thread: the cache is only touched
/// there, while ffmpeg and bitmap decoding are awaited without blocking it.
/// </summary>
public sealed class ThumbnailLoader
{
    private const int ThumbWidth = 320;
    private const int CacheLimit = 300;

    private readonly FfmpegFrameExtractor _extractor = new(new FfmpegTools());
    private readonly Dictionary<(string Database, long MediaId, string ModifiedUtc, int Pos), Bitmap> _cache = new();
    private readonly Queue<(string Database, long MediaId, string ModifiedUtc, int Pos)> _order = new();

    /// <summary>Thumbnails at the first three <paramref name="positions"/> (percent of the duration).</summary>
    public async Task LoadAsync(GroupVm group, string database, IReadOnlyList<int> positions, CancellationToken ct)
    {
        var slots = positions.Take(3).ToArray();
        // limit concurrency (disk + ffmpeg)
        using var gate = new SemaphoreSlim(2);
        await Task.WhenAll(group.Members.ToList().Select(async m =>
        {
            await gate.WaitAsync(ct);
            try { await LoadMemberAsync(group, m, database, slots, ct); }
            finally { gate.Release(); }
        }));
    }

    /// <summary>Only the selected group shows cached bitmaps, so eviction never disposes one on screen.</summary>
    public static void Release(GroupVm group)
    {
        foreach (var member in group.Members)
            member.Thumb20 = member.Thumb50 = member.Thumb80 = null;
    }

    public void Clear()
    {
        foreach (var bitmap in _cache.Values)
            bitmap.Dispose();
        _cache.Clear();
        _order.Clear();
    }

    private async Task LoadMemberAsync(GroupVm group, MemberVm m, string database, int[] slots, CancellationToken ct)
    {
        // Broken ffprobe values (or none) give no usable timestamps.
        if (m.DurationSec is not double dur || dur <= 1.0 || dur > 48 * 3600) return;
        if (string.IsNullOrWhiteSpace(m.Path) || !File.Exists(m.Path)) return;

        m.ThumbsLoading = true;
        try
        {
            for (var slot = 0; slot < slots.Length; slot++)
            {
                ct.ThrowIfCancellationRequested();
                var key = (database, m.MediaId, m.File.ModifiedUtc, slots[slot]);
                if (!_cache.TryGetValue(key, out var bitmap))
                {
                    var ts = Math.Clamp(dur * (slots[slot] / 100.0), 0.1, Math.Max(0.1, dur - 0.1));
                    byte[] png;
                    try
                    {
                        png = await _extractor.ExtractFramePngAsync(m.Path, ts, ct, ThumbWidth);
                    }
                    catch (Exception ex) when (MediaErrors.IsRecoverable(ex))
                    {
                        System.Diagnostics.Trace.TraceWarning($"Thumbnail failed: {m.Path}: {ex}");
                        continue;
                    }

                    // Decode scaled down: a full 4K frame would cost ~33 MB per cached thumbnail.
                    bitmap = await Task.Run(() =>
                    {
                        using var ms = new MemoryStream(png);
                        return Bitmap.DecodeToWidth(ms, ThumbWidth);
                    }, ct);

                    // A concurrent load for the same member may have filled the slot meanwhile.
                    if (!_cache.TryAdd(key, bitmap))
                    {
                        bitmap.Dispose();
                        bitmap = _cache[key];
                    }
                    else
                    {
                        _order.Enqueue(key);
                        Evict(group);
                    }
                }

                // The group may have been deselected while decoding; it must not hold bitmaps.
                ct.ThrowIfCancellationRequested();
                switch (slot)
                {
                    case 0: m.Thumb20 = bitmap; break;
                    case 1: m.Thumb50 = bitmap; break;
                    case 2: m.Thumb80 = bitmap; break;
                }
            }
        }
        finally
        {
            m.ThumbsLoading = false;
        }
    }

    private void Evict(GroupVm shownGroup)
    {
        var shown = shownGroup.Members
            .SelectMany(member => new[] { member.Thumb20, member.Thumb50, member.Thumb80 })
            .OfType<Bitmap>()
            .ToHashSet();

        // Oldest first; bitmaps on screen are re-queued instead of disposed.
        for (var checkedCount = _order.Count; _cache.Count > CacheLimit && checkedCount > 0; checkedCount--)
        {
            var key = _order.Dequeue();
            if (!_cache.TryGetValue(key, out var bitmap))
                continue;
            if (shown.Contains(bitmap))
            {
                _order.Enqueue(key);
                continue;
            }
            _cache.Remove(key);
            bitmap.Dispose();
        }
    }
}
