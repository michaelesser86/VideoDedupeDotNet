using Avalonia.Controls.Primitives;
using Avalonia.Data.Converters;

namespace VideoDedupe.App.Services;

public static class Converters
{
    /// <summary>Scroll bars when true; otherwise the content is fitted to the view.</summary>
    public static readonly IValueConverter ScrollWhenTrue =
        new FuncValueConverter<bool, ScrollBarVisibility>(on => on ? ScrollBarVisibility.Auto : ScrollBarVisibility.Disabled);
}
