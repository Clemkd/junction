using System.Text.Json;

namespace Junction.Stream;

/// <summary>Minimal helper for (de)serializing event headers to/from jsonb.</summary>
internal static class HeaderSerializer
{
    private static readonly IReadOnlyDictionary<string, string> Empty =
        new Dictionary<string, string>();

    public static string? Serialize(IReadOnlyDictionary<string, string>? headers) =>
        headers is null || headers.Count == 0 ? null : JsonSerializer.Serialize(headers);

    public static IReadOnlyDictionary<string, string> Deserialize(string? json) =>
        string.IsNullOrEmpty(json)
            ? Empty
            : JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? Empty;
}
