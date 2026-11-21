using System;
using System.IO;
using System.Threading.Tasks;

using CommunityToolkit.Mvvm.ComponentModel;

using VideoDedupe.Infrastructure;

namespace VideoDedupe.App.ViewModels;

/// <summary>What all panels share: the database, the status line and the busy indicator.</summary>
public partial class AppState : ObservableObject
{
    [ObservableProperty] private string dbPath = AppPaths.DefaultDatabase;
    [ObservableProperty] private string quarantineRoot = AppPaths.DefaultQuarantine;
    [ObservableProperty] private string status = "";
    [ObservableProperty] private bool isBusy;

    private int _busyOperations;
    private AppDb? _db;
    private string? _openPath;

    /// <summary>Raised when the user points the app at another database.</summary>
    public event Action? DatabaseChanged;

    partial void OnDbPathChanged(string value) => DatabaseChanged?.Invoke();

    /// <summary>
    /// Carries out a delete or quarantine still waiting in the undo bar. Runs before anything
    /// that reads the library, so a scan never sees a file that is about to go.
    /// </summary>
    public Func<Task> FlushPendingAsync { get; set; } = () => Task.CompletedTask;

    /// <summary>Marks an operation as running until the returned scope is disposed.</summary>
    public IDisposable Busy()
    {
        _busyOperations++;
        IsBusy = true;
        return new BusyScope(this);
    }

    private sealed class BusyScope(AppState state) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            state._busyOperations = Math.Max(0, state._busyOperations - 1);
            state.IsBusy = state._busyOperations > 0;
        }
    }

    /// <summary>The selected database; the schema check runs once per database.</summary>
    public async Task<AppDb> OpenDbAsync()
    {
        var path = Path.GetFullPath(DbPath);
        if (_db is null || _openPath != path)
        {
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);
            var db = new AppDb(path);
            await DbInitializer.EnsureCreatedAsync(db);
            (_db, _openPath) = (db, path);
        }
        return _db;
    }
}
