using System.ComponentModel;
using System.Text.Json;

namespace VideoDedupe.Infrastructure;

public sealed class ExternalToolException(string message) : InvalidOperationException(message);

public static class MediaErrors
{
    /// <summary>Errors caused by one unreadable file; processing continues with the next one.</summary>
    public static bool IsRecoverable(Exception error) => error is
        ExternalToolException or IOException or UnauthorizedAccessException or Win32Exception
        or JsonException or InvalidDataException;
}
