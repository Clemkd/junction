using System.Text.Json;

namespace Junction.Queue.Internal;

/// <summary>Minimal helper for (de)serializing message headers to/from jsonb.</summary>
internal static class HeaderSerializer
{
    public static string? Serialize(IReadOnlyDictionary<string, string>? headers) =>
        headers is null || headers.Count == 0 ? null : JsonSerializer.Serialize(headers);

    public static IReadOnlyDictionary<string, string>? Deserialize(string? json) =>
        string.IsNullOrEmpty(json)
            ? null
            : JsonSerializer.Deserialize<Dictionary<string, string>>(json);
}
