using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Junction.Connectors;

/// <summary>
/// The default connector: runs queue SQL on the connection of an existing EF Core
/// <see cref="DbContext"/> resolved from the current DI scope, and joins the context's current
/// transaction when there is one.
/// <para>
/// Consequence — and the whole point: if your handler writes through the same <c>DbContext</c>,
/// acknowledging the message is part of your <c>SaveChanges</c> transaction. Either both land or
/// neither does, so a crash mid-handler re-delivers the message instead of losing the work or
/// double-applying it.
/// </para>
/// </summary>
public sealed class EfCoreConnectionSource(DbContext context) : IJunctionConnectionSource
{
    public bool HasAmbientTransaction => context.Database.CurrentTransaction is not null;

    public async ValueTask<JunctionConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        var db = context.Database;
        var connection = db.GetDbConnection();

        // Go through the facade rather than the raw DbConnection so EF's own open/close
        // reference counting stays consistent with ours.
        bool opened = false;
        if (connection.State != ConnectionState.Open)
        {
            await db.OpenConnectionAsync(cancellationToken);
            opened = true;
        }

        var transaction = db.CurrentTransaction?.GetDbTransaction();

        return new JunctionConnection(
            connection,
            transaction,
            opened && transaction is null
                ? async () => await db.CloseConnectionAsync()
                : null);
    }

    public async ValueTask<IJunctionTransaction?> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (context.Database.CurrentTransaction is not null)
            return null; // the caller owns the commit — never nest under it

        var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
        return new EfCoreJunctionTransaction(transaction);
    }

    private sealed class EfCoreJunctionTransaction(IDbContextTransaction transaction) : IJunctionTransaction
    {
        public Task CommitAsync(CancellationToken cancellationToken = default) =>
            transaction.CommitAsync(cancellationToken);

        public Task RollbackAsync(CancellationToken cancellationToken = default) =>
            transaction.RollbackAsync(cancellationToken);

        public ValueTask DisposeAsync() => transaction.DisposeAsync();
    }
}
