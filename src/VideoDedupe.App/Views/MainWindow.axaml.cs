using System;

using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.VisualTree;

using VideoDedupe.App.ViewModels;

namespace VideoDedupe.App.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        // Show the library right away instead of waiting for a "load" click.
        Opened += async (_, _) =>
        {
            if (DataContext is MainViewModel vm)
                await vm.LoadAllAsync();
        };
    }

    protected override void OnKeyDown(KeyEventArgs e)
    {
        base.OnKeyDown(e);
        if (e.Key == Key.F && e.KeyModifiers.HasFlag(KeyModifiers.Control))
        {
            if (this.FindControl<TextBox>("FilterBox") is { } box)
            {
                if (box.FindAncestorOfType<TabItem>() is { } tab)
                    tab.IsSelected = true;
                box.Focus();
                box.SelectAll();
            }
            e.Handled = true;
        }
    }

    private void Member_DoubleTapped(object? sender, TappedEventArgs e)
    {
        if (sender is Control { DataContext: MemberVm m })
            m.Owner.OpenFile(m);
    }
}
