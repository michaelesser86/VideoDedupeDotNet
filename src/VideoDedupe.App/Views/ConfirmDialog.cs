using System.Threading.Tasks;

using Avalonia.Controls;
using Avalonia.Layout;
using Avalonia.Media;

namespace VideoDedupe.App.Views;

/// <summary>Minimal modal yes/no dialog; the safe answer (Cancel) is the default.</summary>
public sealed class ConfirmDialog : Window
{
    private ConfirmDialog(string title, string message, string confirmText)
    {
        Title = title;
        Width = 520;
        SizeToContent = SizeToContent.Height;
        CanResize = false;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;

        var confirm = new Button { Content = confirmText, Classes = { "accent" } };
        var cancel = new Button { Content = "Cancel", IsDefault = true, IsCancel = true };
        confirm.Click += (_, _) => Close(true);
        cancel.Click += (_, _) => Close(false);

        Content = new StackPanel
        {
            Margin = new Avalonia.Thickness(20),
            Spacing = 16,
            Children =
            {
                new TextBlock { Text = message, TextWrapping = TextWrapping.Wrap },
                new StackPanel
                {
                    Orientation = Orientation.Horizontal,
                    HorizontalAlignment = HorizontalAlignment.Right,
                    Spacing = 8,
                    Children = { cancel, confirm }
                }
            }
        };
    }

    public static async Task<bool> ShowAsync(Window owner, string title, string message, string confirmText) =>
        await new ConfirmDialog(title, message, confirmText).ShowDialog<bool?>(owner) == true;
}
