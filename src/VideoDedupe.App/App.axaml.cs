using System;
using System.IO;

using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;

using VideoDedupe.Infrastructure;

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
            var dbPath = Path.Combine(AppContext.BaseDirectory, "videodedupe.db");
            var db = new AppDb(dbPath);
            DbInitializer.EnsureCreatedAsync(db).GetAwaiter().GetResult();

            desktop.MainWindow = new VideoDedupe.App.Views.MainWindow();
        }

        base.OnFrameworkInitializationCompleted();
    }
}
