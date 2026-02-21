using Avalonia.Controls;
using Avalonia.Input;

using VideoDedupe.App.ViewModels;

namespace VideoDedupe.App.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    private void Member_DoubleTapped(object? sender, TappedEventArgs e)
    {
        // sender is the Border inside the DataTemplate
        if (sender is not Control c) return;

        if (DataContext is not MainViewModel vm) return;
        if (c.DataContext is not MemberVm m) return;

        vm.OpenFile(m);
    }
}
