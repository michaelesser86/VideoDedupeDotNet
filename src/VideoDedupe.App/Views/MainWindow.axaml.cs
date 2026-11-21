using System;

using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.VisualTree;

using VideoDedupe.App.ViewModels;

namespace VideoDedupe.App.Views;

public partial class MainWindow : Window
{
    private bool _pendingCommitted;

    public MainWindow()
    {
        InitializeComponent();
        // Show the library right away instead of waiting for a "load" click.
        Opened += async (_, _) =>
        {
            if (DataContext is MainViewModel vm)
                await vm.LoadAllAsync();
        };
        // Tunnel: the shortcuts must win over the group list and the scroll viewer, which would
        // otherwise use the arrow keys themselves.
        AddHandler(KeyDownEvent, OnShortcutKeyDown, RoutingStrategies.Tunnel);
    }

    protected override async void OnClosing(WindowClosingEventArgs e)
    {
        base.OnClosing(e);
        if (_pendingCommitted || DataContext is not MainViewModel { Groups.HasPendingAction: true } vm)
            return;
        // The delete waiting in the undo bar is carried out before the app closes.
        e.Cancel = true;
        _pendingCommitted = true;
        await vm.Groups.CommitPendingAsync();
        Close();
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

    /// <summary>Keys for reviewing groups without the mouse (see the hint above the files).</summary>
    private void OnShortcutKeyDown(object? sender, KeyEventArgs e)
    {
        if (DataContext is not MainViewModel vm || this.FindControl<TabControl>("Tabs") is not { SelectedIndex: 0 })
            return;
        var focused = FocusManager?.GetFocusedElement();
        // Typing and drop-downs keep their keys.
        if (focused is TextBox or ComboBox or Slider || focused is Control c && c.FindAncestorOfType<ComboBox>() is not null)
            return;
        var groups = vm.Groups;

        if (e.KeyModifiers == KeyModifiers.Control && e.Key == Key.Z)
        {
            _ = groups.UndoCommand.ExecuteAsync(null);
            e.Handled = true;
            return;
        }
        if (e.KeyModifiers != KeyModifiers.None || vm.State.IsBusy && e.Key is not (Key.Up or Key.Down or Key.Left or Key.Right))
            return;

        var member = groups.CurrentMember;
        switch (e.Key)
        {
            case Key.Up: groups.SelectAdjacentGroup(-1); break;
            case Key.Down: groups.SelectAdjacentGroup(1); break;
            case Key.Left: groups.SelectAdjacentMember(-1); break;
            case Key.Right: groups.SelectAdjacentMember(1); break;
            case >= Key.D1 and <= Key.D9: groups.SelectMember(e.Key - Key.D1); break;
            case >= Key.NumPad1 and <= Key.NumPad9: groups.SelectMember(e.Key - Key.NumPad1); break;
            // A focused button handles Enter and Space itself.
            case Key.Enter or Key.Space when focused is Button: return;
            case Key.Enter or Key.Space: groups.OpenFile(member); break;
            case Key.Delete: _ = groups.DeleteFileCommand.ExecuteAsync(member); break;
            case Key.Q: _ = groups.QuarantineCommand.ExecuteAsync(member); break;
            case Key.B or Key.K: _ = groups.MarkKeepCommand.ExecuteAsync(member); break;
            case Key.N: _ = groups.NotDuplicateCommand.ExecuteAsync(member); break;
            case Key.E: groups.ShowInExplorer(member); break;
            case Key.V: _ = groups.CompareCommand.ExecuteAsync(member); break;
            default: return;
        }
        e.Handled = true;
    }

    private void Member_DoubleTapped(object? sender, TappedEventArgs e)
    {
        if (sender is Control { DataContext: MemberVm m })
            m.Owner.OpenFile(m);
    }

    private void Member_PointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (sender is Control { DataContext: MemberVm m })
            m.Owner.CurrentMember = m;
    }
}
