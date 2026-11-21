using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace VideoDedupe.App.Services;

/// <summary>
/// Asks GitHub for the latest release. Sends nothing but the request itself; local and
/// development builds never ask.
/// </summary>
public static class UpdateChecker
{
    public const string LatestReleaseApi = "https://api.github.com/repos/michaelesser86/VideoDedupeDotNet/releases/latest";

    public sealed record Release(string Version, string Url);

    /// <summary>The version the build was published with (0.0.0-dev... for development builds).</summary>
    public static string CurrentVersion { get; } =
        (typeof(UpdateChecker).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "")
        .Split('+')[0];

    /// <summary>
    /// Release builds carry a plain x.y.z version; workflow test builds ("0.0.0-dev.5") never
    /// check. Local builds report 1.0.0 and do check, which at worst shows a newer release.
    /// </summary>
    public static bool IsReleaseBuild(string version) => System.Version.TryParse(version, out _);

    /// <summary>True when <paramref name="tag"/> (e.g. "v1.2.0") is newer than <paramref name="current"/>.</summary>
    public static bool IsNewer(string tag, string current) =>
        System.Version.TryParse(tag.TrimStart('v', 'V'), out var latest)
        && System.Version.TryParse(current, out var installed)
        && latest > installed;

    public static async Task<Release?> FindNewerAsync(CancellationToken ct = default)
    {
        var current = CurrentVersion;
        if (!IsReleaseBuild(current)) return null;

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("VideoDedupe", current));
        http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.github+json"));
        using var response = await http.GetAsync(LatestReleaseApi, ct);
        if (!response.IsSuccessStatusCode) return null;

        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var json = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        var tag = json.RootElement.GetProperty("tag_name").GetString() ?? "";
        var url = json.RootElement.GetProperty("html_url").GetString() ?? "";
        // Only links to the project's own release pages are ever opened.
        if (!url.StartsWith("https://github.com/michaelesser86/VideoDedupeDotNet/releases/", StringComparison.Ordinal))
            return null;
        return IsNewer(tag, current) ? new Release(tag.TrimStart('v', 'V'), url) : null;
    }
}
