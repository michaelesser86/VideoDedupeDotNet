using System.Globalization;
using System.Text;

namespace VideoDedupe.Infrastructure;

/// <summary>The duplicate groups as a CSV table, e.g. to review or archive them in Excel.</summary>
public static class DuplicateReport
{
    public sealed record Row(long GroupId, bool Keep, AppDb.GroupMember Member);

    /// <summary>Column titles and the two action words; the app passes them translated.</summary>
    public static readonly string[] Texts =
    [
        "Group", "Action", "File name", "Folder", "Size (MB)", "Resolution", "Duration (s)",
        "Bitrate (Mbit/s)", "Codec", "Picture similarity (%)", "Modified", "keep", "remove"
    ];

    /// <summary>
    /// Uses the culture's list separator and number format, so Excel opens the file in that
    /// language with one value per column (German: ";" and decimal comma).
    /// </summary>
    public static void WriteCsv(TextWriter writer, IEnumerable<Row> rows, CultureInfo culture,
        Func<string, string>? translate = null)
    {
        string T(string text) => translate?.Invoke(text) ?? text;
        var separator = culture.TextInfo.ListSeparator;
        if (string.IsNullOrEmpty(separator)) separator = ",";

        void Line(IEnumerable<string> fields) => writer.WriteLine(string.Join(separator, fields.Select(f => Field(f, separator))));

        Line(Texts.Take(11).Select(T));
        foreach (var row in rows)
        {
            var f = row.Member.File;
            Line(
            [
                row.GroupId.ToString(culture),
                row.Keep ? T("keep") : T("remove"),
                Path.GetFileName(f.Path),
                Path.GetDirectoryName(f.Path) ?? "",
                (f.SizeBytes / (1024.0 * 1024.0)).ToString("F1", culture),
                f.Width is int w && f.Height is int h ? $"{w}x{h}" : "",
                f.DurationSec?.ToString("F1", culture) ?? "",
                BestFileSelector.BitrateMbps(f)?.ToString("F2", culture) ?? "",
                f.VideoCodec ?? "",
                (Math.Clamp(1 - row.Member.AvgDist / 32, 0, 1) * 100).ToString("F0", culture),
                DateTime.TryParse(f.ModifiedUtc, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var utc)
                    ? utc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)
                    : ""
            ]);
        }
    }

    private static string Field(string value, string separator)
    {
        // A cell starting with = + - @ would run as a formula in Excel; file names can start so.
        if (value.Length > 0 && "=+-@\t\r".Contains(value[0]))
            value = "'" + value;
        var needsQuotes = value.Contains(separator, StringComparison.Ordinal)
                          || value.IndexOfAny(['"', '\n', '\r']) >= 0;
        return needsQuotes ? "\"" + value.Replace("\"", "\"\"") + "\"" : value;
    }

    /// <summary>UTF-8 with byte order mark: without it Excel reads umlauts as ANSI.</summary>
    public static Encoding FileEncoding { get; } = new UTF8Encoding(encoderShouldEmitUTF8Identifier: true);
}
