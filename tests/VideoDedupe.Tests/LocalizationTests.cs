using System.Text.RegularExpressions;

using VideoDedupe.App.Services;

namespace VideoDedupe.Tests;

public class LocalizationTests
{
    private static string[] Placeholders(string format) =>
        Regex.Matches(format, @"\{\d+(:[^}]*)?\}").Select(m => m.Value).Order().ToArray();

    [Fact]
    public void GermanTextsKeepEveryPlaceholder()
    {
        foreach (var (english, german) in Loc.GermanTexts)
            Assert.True(Placeholders(english).SequenceEqual(Placeholders(german)),
                $"\"{german}\" must use the placeholders of \"{english}\"");
    }

    [Fact]
    public void GermanFormatsAreValid()
    {
        var args = Enumerable.Range(0, 5).Select(i => (object)i).ToArray();
        foreach (var german in Loc.GermanTexts.Values)
            _ = string.Format(german, args); // throws on a broken format
    }

    [Fact]
    public void SwitchingLanguageTranslatesAndFallsBackToEnglish()
    {
        var loc = Loc.Instance;
        var before = loc.Language;
        try
        {
            loc.Language = Loc.German;
            Assert.Equal("Duplikate finden", Loc.T("Find duplicates"));
            Assert.Equal("Duplikate (3)", Loc.F("Duplicates ({0})", 3));
            Assert.Equal("not translated", Loc.T("not translated"));

            loc.Language = Loc.English;
            Assert.Equal("Find duplicates", Loc.T("Find duplicates"));
        }
        finally
        {
            loc.Language = before;
        }
    }

    [Fact]
    public void EveryColorSchemeHasValidColors()
    {
        Assert.Equal(5, ColorSchemes.All.Count);
        Assert.Equal(ColorSchemes.All.Count, ColorSchemes.All.Select(s => s.Id).Distinct().Count());
        foreach (var scheme in ColorSchemes.All)
            foreach (var hex in new[] { scheme.GradientFrom, scheme.GradientTo }
                         .Concat(Colors(scheme.Light)).Concat(Colors(scheme.Dark)))
                Assert.True(Avalonia.Media.Color.TryParse(hex, out _), $"{scheme.Id}: {hex}");
        Assert.Equal("violet", ColorSchemes.Find("unknown").Id);
    }

    private static IEnumerable<string> Colors(SchemeSurface s) =>
        [s.Accent, s.Background, s.Card, s.Stroke, s.Subtle, s.AccentText, s.BadgeBest];
}
