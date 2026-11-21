using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;

using VideoDedupe.App.ViewModels;

namespace VideoDedupe.App;

public partial class App : Application
{
    public new static App? Current => Application.Current as App;

    public override void Initialize()
    {
        AvaloniaXamlLoader.Load(this);
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            // The database is created lazily by the view model, at the path from the settings.
            var window = new Views.MainWindow();
            window.Closing += (_, _) => (window.DataContext as MainViewModel)?.SaveSettings();
            desktop.MainWindow = window;
        }

        base.OnFrameworkInitializationCompleted();
    }
}
