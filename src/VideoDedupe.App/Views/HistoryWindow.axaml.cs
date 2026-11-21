using Avalonia.Controls;
using Avalonia.Interactivity;

namespace VideoDedupe.App.Views;

public partial class HistoryWindow : Window
{
    public HistoryWindow() => InitializeComponent();

    private void Close_Click(object? sender, RoutedEventArgs e) => Close();
}
