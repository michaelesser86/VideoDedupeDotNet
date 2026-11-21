using System.ComponentModel;

using Avalonia.Media;

using VideoDedupe.App.Services;

namespace VideoDedupe.App.ViewModels;

/// <summary>An option in a drop-down: a fixed id for the logic and a label in the current language.</summary>
public sealed class Choice : INotifyPropertyChanged
{
    private readonly bool _translate;

    public Choice(string id, string label, bool translate = true, IBrush? swatch = null)
    {
        Id = id;
        EnglishLabel = label;
        _translate = translate;
        Swatch = swatch;
        // Choices live as long as the window, so the subscription does not leak.
        Loc.Instance.LanguageChanged += () => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Label)));
    }

    public string Id { get; }
    public string EnglishLabel { get; }
    public string Label => _translate ? Loc.T(EnglishLabel) : EnglishLabel;

    /// <summary>Color preview, for color schemes.</summary>
    public IBrush? Swatch { get; }

    public event PropertyChangedEventHandler? PropertyChanged;

    public override string ToString() => Label;
}
