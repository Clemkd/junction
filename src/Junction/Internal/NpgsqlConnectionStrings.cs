using Npgsql;

namespace Junction.Internal;

/// <summary>Connection-string tuning shared by both modules' standalone (own-connection) registration.</summary>
internal static class NpgsqlConnectionStrings
{
    /// <summary>
    /// Enable Npgsql server-side auto-prepare, unless the caller already configured it: the hot
    /// statements (claims, polls, upserts) are the same handful of texts repeated forever, so letting
    /// the server prepare them skips repeated parse/plan. Purely a connection-string tuning, no
    /// behavioural change.
    /// </summary>
    public static string EnableAutoPrepare(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (builder.MaxAutoPrepare <= 0)
        {
            builder.MaxAutoPrepare = 32;
            builder.AutoPrepareMinUsages = 2;
        }
        return builder.ConnectionString;
    }
}
