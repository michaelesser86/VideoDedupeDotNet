using System.ComponentModel;
using System.Text.Json;
using SixLabors.ImageSharp;

namespace VideoDedupe.Infrastructure;

public sealed class ExternalToolException(string message) : InvalidOperationException(message);

public static class MediaErrors
{
    public static bool IsRecoverable(Exception error) => error is
        ExternalToolException or IOException or UnauthorizedAccessException or Win32Exception
        or JsonException or UnknownImageFormatException or InvalidImageContentException;
}
