using System;
using System.Globalization;

using Avalonia.Data;
using Avalonia.Data.Converters;

namespace VideoDedupe.App.Services;

/// <summary>
/// <c>{l:T 'English text'}</c> in XAML: the translated text, updated when the language changes.
/// </summary>
public sealed class TExtension
{
    public TExtension(string text) => Text = text;

    public string Text { get; set; }

    public object ProvideValue(IServiceProvider serviceProvider) =>
        // Bound to Loc.Version, which changes with every switch, so the text is re-translated.
        new ReflectionBinding(nameof(Loc.Version))
        {
            Source = Loc.Instance,
            Mode = BindingMode.OneWay,
            Converter = TranslateConverter.Instance,
            ConverterParameter = Text
        };

    private sealed class TranslateConverter : IValueConverter
    {
        public static readonly TranslateConverter Instance = new();

        public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
            Loc.T(parameter as string ?? "");

        public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
            throw new NotSupportedException();
    }
}
