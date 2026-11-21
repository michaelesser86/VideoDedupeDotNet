using System.Collections.Generic;
using System.Linq;

using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;
using Avalonia.Styling;
using Avalonia.Themes.Fluent;

namespace VideoDedupe.App.Services;

/// <summary>Surface colors of one color scheme in light or dark mode.</summary>
public sealed record SchemeSurface(string Accent, string Background, string Card, string Stroke, string Subtle,
    string AccentText, string BadgeBest);

/// <summary>An accent color with matching surfaces and a two-color gradient for highlights.</summary>
public sealed record ColorScheme(string Id, string Name, string GradientFrom, string GradientTo,
    SchemeSurface Light, SchemeSurface Dark);

public static class ColorSchemes
{
    public static IReadOnlyList<ColorScheme> All { get; } =
    [
        new("violet", "Violet", "#8B5CF6", "#6366F1",
            Light: new("#7C3AED", "#F4F4F8", "#FFFFFF", "#E2E2EC", "#ECECF3", "#6D28D9", "#EDE7FE"),
            Dark: new("#8B5CF6", "#0E0E16", "#171724", "#2A2A3E", "#202032", "#C4B5FD", "#33275A")),

        // Neon sunset over the ocean: hot pink fading into orange, on deep purple at night.
        new("sunset", "Miami Sunset", "#FF2D95", "#FF9F1C",
            Light: new("#DB2777", "#FFF4F7", "#FFFFFF", "#F6D3E1", "#FCE7F0", "#BE185D", "#FCE7F3"),
            Dark: new("#EC4899", "#130A17", "#1F1024", "#3B1C42", "#2A1530", "#FF9BC9", "#4A1838")),

        new("ocean", "Ocean", "#06B6D4", "#3B82F6",
            Light: new("#0E7490", "#F1F8FB", "#FFFFFF", "#D3E6EE", "#E3F2F7", "#0E7490", "#CFF4FC"),
            Dark: new("#0891B2", "#07111A", "#0D1B26", "#1B3242", "#12263A", "#67E8F9", "#0F3A47")),

        new("emerald", "Emerald", "#10B981", "#84CC16",
            Light: new("#047857", "#F2FAF6", "#FFFFFF", "#D3EBDD", "#E3F4EA", "#047857", "#D1FAE5"),
            Dark: new("#059669", "#08120E", "#0F1D17", "#1E3529", "#142820", "#6EE7B7", "#0F3D2C")),

        new("ruby", "Ruby", "#E11D48", "#9333EA",
            Light: new("#BE123C", "#FFF5F6", "#FFFFFF", "#F4D2D8", "#FDE4E8", "#BE123C", "#FFE4E6")
            , Dark: new("#E11D48", "#140A0D", "#21111A", "#3A1C25", "#2A141B", "#FDA4AF", "#4C1524")),
    ];

    public static ColorScheme Find(string? id) => All.FirstOrDefault(s => s.Id == id) ?? All[0];

    /// <summary>
    /// Replaces the scheme resources of both modes. Everything uses DynamicResource, so the open
    /// window recolors at once. The accent keys are the ones Fluent's controls read.
    /// </summary>
    public static void Apply(ColorScheme scheme)
    {
        if (Application.Current is not { } app) return;
        Fill(Dictionary(app, ThemeVariant.Light), scheme, scheme.Light);
        Fill(Dictionary(app, ThemeVariant.Dark), scheme, scheme.Dark);

        // Fluent builds its accent brushes from its palette. Overriding SystemAccentColor alone is
        // not enough: in light mode some brushes keep the color they were created with.
        if (app.Styles.OfType<FluentTheme>().FirstOrDefault() is { } fluent)
        {
            SetAccent(fluent, ThemeVariant.Light, scheme.Light.Accent);
            SetAccent(fluent, ThemeVariant.Dark, scheme.Dark.Accent);
        }
    }

    private static void SetAccent(FluentTheme fluent, ThemeVariant variant, string accent)
    {
        if (!fluent.Palettes.TryGetValue(variant, out var palette))
            fluent.Palettes[variant] = palette = new ColorPaletteResources();
        palette.Accent = Color.Parse(accent);
    }

    private static IResourceDictionary Dictionary(Application app, ThemeVariant variant)
    {
        if (app.Resources.ThemeDictionaries.TryGetValue(variant, out var provider) && provider is IResourceDictionary d)
            return d;
        var created = new ResourceDictionary();
        app.Resources.ThemeDictionaries[variant] = created;
        return created;
    }

    private static void Fill(IResourceDictionary d, ColorScheme scheme, SchemeSurface s)
    {
        d["AppBackgroundBrush"] = Brush(s.Background);
        d["CardBrush"] = Brush(s.Card);
        d["CardStrokeBrush"] = Brush(s.Stroke);
        d["SubtleFillBrush"] = Brush(s.Subtle);
        d["AccentTextBrush"] = Brush(s.AccentText);
        d["BadgeBestBrush"] = Brush(s.BadgeBest);
        d["AccentLineBrush"] = Brush(s.Accent);
        d["HighlightGradientBrush"] = Gradient(scheme);
    }

    /// <summary>The scheme's signature gradient, left to right.</summary>
    public static LinearGradientBrush Gradient(ColorScheme scheme) => new()
    {
        StartPoint = new RelativePoint(0, 0, RelativeUnit.Relative),
        EndPoint = new RelativePoint(1, 0, RelativeUnit.Relative),
        GradientStops =
        {
            new GradientStop(Color.Parse(scheme.GradientFrom), 0),
            new GradientStop(Color.Parse(scheme.GradientTo), 1)
        }
    };

    private static SolidColorBrush Brush(string hex) => new(Color.Parse(hex));
}
