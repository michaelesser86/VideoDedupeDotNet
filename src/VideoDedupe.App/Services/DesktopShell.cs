using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Input.Platform;
using Avalonia.Platform.Storage;

using VideoDedupe.App.Views;

namespace VideoDedupe.App.Services;

/// <summary>Everything that talks to the desktop: windows, dialogs, Explorer, clipboard.</summary>
public static class DesktopShell
{
    public static Window? MainWindow() =>
        (Application.Current?.ApplicationLifetime as IClassicDesktopStyleApplicationLifetime)?.MainWindow;

    private static TopLevel? TopLevel() => MainWindow() is { } window ? Avalonia.Controls.TopLevel.GetTopLevel(window) : null;

    /// <summary>Asks the user; without a window (tests, shutdown) the answer is no.</summary>
    public static async Task<bool> ConfirmAsync(string title, string message, string confirmText, string? details = null) =>
        MainWindow() is { } owner && await ConfirmDialog.ShowAsync(owner, title, message, confirmText, details);

    public static async Task<string?> PickFolderAsync(string title)
    {
        if (TopLevel() is not { } topLevel) return null;
        var result = await topLevel.StorageProvider.OpenFolderPickerAsync(
            new FolderPickerOpenOptions { Title = title, AllowMultiple = false });
        return result.Count > 0 ? result[0].TryGetLocalPath() : null;
    }

    public static async Task<string?> PickDatabaseAsync()
    {
        if (TopLevel() is not { } topLevel) return null;
        var files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = Loc.T("Select SQLite Database"),
            AllowMultiple = false,
            FileTypeFilter =
            [
                new FilePickerFileType(Loc.T("SQLite Database")) { Patterns = ["*.db", "*.sqlite", "*.sqlite3"] },
                FilePickerFileTypes.All
            ]
        });
        return files.Count > 0 ? files[0].TryGetLocalPath() : null;
    }

    public static async Task<string?> PickSaveFileAsync(string title, string suggestedName, string typeName, string pattern)
    {
        if (TopLevel() is not { } topLevel) return null;
        var file = await topLevel.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
        {
            Title = title,
            SuggestedFileName = suggestedName,
            DefaultExtension = System.IO.Path.GetExtension(pattern),
            ShowOverwritePrompt = true,
            FileTypeChoices = [new FilePickerFileType(typeName) { Patterns = [pattern] }]
        });
        return file?.TryGetLocalPath();
    }

    public static async Task<bool> CopyTextAsync(string text)
    {
        if (TopLevel()?.Clipboard is not { } clipboard) return false;
        await clipboard.SetTextAsync(text);
        return true;
    }

    public static void OpenFile(string path) =>
        Process.Start(new ProcessStartInfo { FileName = path, UseShellExecute = true });

    public static void ShowInExplorer(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            // Highlight the file. Explorer parses "/select,<path>" itself, so this exact
            // quoting is required (ArgumentList would quote the whole switch).
            Process.Start(new ProcessStartInfo
            {
                FileName = "explorer.exe",
                Arguments = $"/select,\"{path}\"",
                UseShellExecute = true
            });
            return;
        }

        // macOS can reveal the file; elsewhere open its folder.
        Process.Start(OperatingSystem.IsMacOS()
            ? new ProcessStartInfo("open") { ArgumentList = { "-R", path } }
            : new ProcessStartInfo("xdg-open") { ArgumentList = { Path.GetDirectoryName(path)! } });
    }

    /// <summary>Recycle Bin on Windows, permanent elsewhere; returns whether it was recycled.</summary>
    public static bool DeleteFile(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            Microsoft.VisualBasic.FileIO.FileSystem.DeleteFile(path,
                Microsoft.VisualBasic.FileIO.UIOption.OnlyErrorDialogs,
                Microsoft.VisualBasic.FileIO.RecycleOption.SendToRecycleBin);
            return true;
        }
        File.Delete(path);
        return false;
    }
}
