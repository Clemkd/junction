using Npgsql;

namespace Junction.Connectors;

/// <summary>
/// Connector for callers with no EF Core context in the picture: rents a connection from an
/// <see cref="NpgsqlDataSource"/> per operation. Use this when the Queue module is the only thing
/// talking to the database in that code path; prefer <see cref="EfCoreConnectionSource"/> when you want
/// the completion to ride along with your business transaction.
/// </summary>
public sealed class NpgsqlConnectionSource(NpgsqlDataSource dataSource) : IJunctionConnectionSource
{
    private NpgsqlConnection? _pinnedConnection;
    private NpgsqlTransaction? _pinnedTransaction;

    public bool HasAmbientTransaction => _pinnedTransaction is not null;

    public async ValueTask<JunctionConnection> AcquireAsync(CancellationToken cancellationToken = default)
    {
        // While a transaction is open every statement must run on the connection holding it.
        if (_pinnedConnection is not null)
            return new JunctionConnection(_pinnedConnection, _pinnedTransaction, release: null);

        var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return new JunctionConnection(connection, transaction: null, connection.DisposeAsync);
    }

    public async ValueTask<IJunctionTransaction?> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_pinnedTransaction is not null)
            return null;

        var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        _pinnedConnection = connection;
        _pinnedTransaction = transaction;
        return new PinnedTransaction(this, connection, transaction);
    }

    private void Unpin()
    {
        _pinnedConnection = null;
        _pinnedTransaction = null;
    }

    private sealed class PinnedTransaction(
        NpgsqlConnectionSource owner,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction) : IJunctionTransaction
    {
        public Task CommitAsync(CancellationToken cancellationToken = default) =>
            transaction.CommitAsync(cancellationToken);

        public Task RollbackAsync(CancellationToken cancellationToken = default) =>
            transaction.RollbackAsync(cancellationToken);

        public async ValueTask DisposeAsync()
        {
            owner.Unpin();
            await transaction.DisposeAsync();
            await connection.DisposeAsync();
        }
    }
}
