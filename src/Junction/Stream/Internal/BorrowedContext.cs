using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Junction.Stream.Internal;

/// <summary>
/// Builds a <see cref="JunctionDbContext"/> over a connection somebody else owns, enlisted in that
/// connection's current transaction when there is one.
/// <para>
/// This is what lets a Stream write ride the caller's transaction instead of the module's own pool:
/// the context does not open, close or own the connection, so whatever it writes commits exactly when
/// the caller commits. Shared by the append path
/// (<see cref="TransactionalEventProducer"/>) and the consumer's cursor and dead-letter writes, so the
/// two cannot drift on the detail that makes both atomic.
/// </para>
/// </summary>
internal static class BorrowedContext
{
    public static JunctionDbContext Create(DbConnection connection, DbTransaction? transaction)
    {
        var builder = new DbContextOptionsBuilder<JunctionDbContext>();
        builder.UseNpgsql(connection);
        var ctx = new JunctionDbContext(builder.Options);
        if (transaction is not null)
            ctx.Database.UseTransaction(transaction);
        return ctx;
    }
}
