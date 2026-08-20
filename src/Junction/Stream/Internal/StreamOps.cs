using Microsoft.EntityFrameworkCore;

namespace Junction.Stream.Internal;

/// <summary>Race-safe helpers for resolving and creating stream rows.</summary>
internal static class StreamOps
{
    /// <summary>Insert the stream row if absent. Safe under concurrency via ON CONFLICT DO NOTHING.</summary>
    public static async Task EnsureStreamAsync(JunctionDbContext ctx, string stream, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        await ctx.Database.ExecuteSqlAsync(
            $"INSERT INTO junction.streams (name, next_seq, created_at) VALUES ({stream}, 0, {now}) ON CONFLICT (name) DO NOTHING",
            ct);
    }

    public static Task<long?> TryGetStreamIdAsync(JunctionDbContext ctx, string stream, CancellationToken ct) =>
        ctx.Streams.Where(s => s.Name == stream).Select(s => (long?)s.Id).FirstOrDefaultAsync(ct);

    public static async Task<long> GetRequiredStreamIdAsync(JunctionDbContext ctx, string stream, CancellationToken ct)
        => await TryGetStreamIdAsync(ctx, stream, ct)
           ?? throw new InvalidOperationException($"Stream '{stream}' does not exist.");
}
