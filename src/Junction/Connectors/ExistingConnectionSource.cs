using System.Data;
using System.Data.Common;

namespace Junction.Connectors;

/// <summary>
/// Connector over a connection (and optionally a transaction) the caller already has in hand —
/// what <see cref="IQueueClient.Using(DbConnection, DbTransaction?)"/> builds. Nothing is pooled
/// and nothing is closed that this source did not open: the connection stays the caller's.
/// </summary>
public sealed class ExistingConnectionSource(DbConnection connection, DbTransaction? transaction = null)
    : IJunctionConnectionSource
{
    private DbTransaction? _ownTransaction;

    public bool HasAmbientTransaction => transaction is not null || _ownTransaction is not null;

    public async ValueTask<JunctionConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        bool opened = false;
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
            opened = true;
        }

        var current = transaction ?? _ownTransaction;

        return new JunctionConnection(
            connection,
            current,
            opened && current is null
                ? async () => await connection.CloseAsync()
                : null);
    }

    public async ValueTask<IJunctionTransaction?> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (HasAmbientTransaction)
            return null;

        if (connection.State != ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        _ownTransaction = await connection.BeginTransactionAsync(cancellationToken);
        return new OwnedTransaction(this, _ownTransaction);
    }

    private sealed class OwnedTransaction(ExistingConnectionSource owner, DbTransaction transaction)
        : IJunctionTransaction
    {
        public Task CommitAsync(CancellationToken cancellationToken = default) =>
            transaction.CommitAsync(cancellationToken);

        public Task RollbackAsync(CancellationToken cancellationToken = default) =>
            transaction.RollbackAsync(cancellationToken);

        public async ValueTask DisposeAsync()
        {
            owner._ownTransaction = null;
            await transaction.DisposeAsync();
        }
    }
}
