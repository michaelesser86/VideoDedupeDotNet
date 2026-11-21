using System.Threading.Tasks;

using Avalonia.Controls;
using Avalonia.Layout;
using Avalonia.Media;

namespace VideoDedupe.App.Views;

/// <summary>Minimal modal yes/no dialog; the safe answer (Cancel) is the default.</summary>
public sealed class ConfirmDialog : Window
{
    private ConfirmDialog(string title, string message, string confirmText, string? details)
    {
        Title = title;
        Width = details is null ? 520 : 760;
        SizeToContent = SizeToContent.Height;
        CanResize = false;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;

        var confirm = new Button { Content = confirmText, Classes = { "accent" } };
        var cancel = new Button { Content = Services.Loc.T("Cancel"), IsDefault = true, IsCancel = true };
        confirm.Click += (_, _) => Close(true);
        cancel.Click += (_, _) => Close(false);

        var body = new StackPanel
        {
            Margin = new Avalonia.Thickness(20),
            Spacing = 16,
            Children = { new TextBlock { Text = message, TextWrapping = TextWrapping.Wrap } }
        };
        if (details is not null)
        {
            // Long lists (every file of a clean-up) scroll instead of growing past the screen.
            body.Children.Add(new ScrollViewer
            {
                MaxHeight = 360,
                HorizontalScrollBarVisibility = Avalonia.Controls.Primitives.ScrollBarVisibility.Auto,
                Content = new SelectableTextBlock { Text = details, FontFamily = new FontFamily("Consolas, monospace"), FontSize = 12 }
            });
        }
        body.Children.Add(new StackPanel
        {
            Orientation = Orientation.Horizontal,
            HorizontalAlignment = HorizontalAlignment.Right,
            Spacing = 8,
            Children = { cancel, confirm }
        });
        Content = body;
    }

    public static async Task<bool> ShowAsync(Window owner, string title, string message, string confirmText,
        string? details = null) =>
        await new ConfirmDialog(title, message, confirmText, details).ShowDialog<bool?>(owner) == true;
}
