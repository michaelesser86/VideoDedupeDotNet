using System;

using Avalonia.Controls;
using Avalonia.Input;

using VideoDedupe.App.ViewModels;

namespace VideoDedupe.App.Views;

public partial class CompareWindow : Window
{
    public CompareWindow()
    {
        InitializeComponent();
        Opened += async (_, _) =>
        {
            if (DataContext is CompareViewModel vm)
                await vm.LoadAsync();
        };
        Closed += (_, _) => (DataContext as IDisposable)?.Dispose();
    }

    protected override void OnKeyDown(KeyEventArgs e)
    {
        base.OnKeyDown(e);
        if (e.Handled || DataContext is not CompareViewModel vm) return;
        switch (e.Key)
        {
            case Key.Left: vm.Step("-2"); break;
            case Key.Right: vm.Step("2"); break;
            case Key.Escape: Close(); break;
            default: return;
        }
        e.Handled = true;
    }
}
