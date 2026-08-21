using System.Data.Common;

namespace Junction.Connectors;

/// <summary>
/// The <b>connector</b>: where the Queue module gets the PostgreSQL connection it runs its SQL on.
/// <para>
/// The Queue module never owns a connection pool of its own by default. It borrows the connection you
/// already have — by default the one behind your EF Core <c>DbContext</c> — which is what makes a
/// message completion and your business writes commit <b>together</b>: same connection, same
/// transaction, one <c>COMMIT</c>. That turns at-least-once delivery into effectively-once processing
/// without any distributed transaction.
/// </para>
/// <para>
/// A <see cref="DbConnection"/> cannot run two commands at once, so a source is scoped to one unit
/// of work (a DI scope, i.e. one message or one batch). The worker host creates that scope for you.
/// </para>
/// </summary>
public interface IJunctionConnectionSource
{
    /// <summary>
    /// Borrow the connection to run one queue statement on, together with the transaction it must
    /// enlist in (the caller's, if any). Dispose the result to hand the connection back — sources
    /// that do not own the connection release nothing.
    /// </summary>
    ValueTask<JunctionConnection> AcquireAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// <c>true</c> when the caller already has a transaction open on this connection, so queue
    /// operations join it and commit when the caller commits.
    /// </summary>
    bool HasAmbientTransaction { get; }

    /// <summary>
    /// Start a transaction that subsequent <see cref="AcquireAsync"/> calls on this source join —
    /// used by the worker host to commit the handler's writes and the message completion atomically.
    /// Returns <c>null</c> when a transaction is already ambient (the caller owns the commit) or when
    /// the source cannot own one.
    /// </summary>
    ValueTask<IJunctionTransaction?> BeginTransactionAsync(CancellationToken cancellationToken = default);
}

/// <summary>A transaction owned by a <see cref="IJunctionConnectionSource"/>.</summary>
public interface IJunctionTransaction : IAsyncDisposable
{
    Task CommitAsync(CancellationToken cancellationToken = default);

    Task RollbackAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// A borrowed connection plus the transaction queue SQL must run in. Disposing returns it to
/// whoever owns it (a pool, or nobody when the connection belongs to the caller).
/// </summary>
public sealed class JunctionConnection(
    DbConnection connection,
    DbTransaction? transaction,
    Func<ValueTask>? release) : IAsyncDisposable
{
    /// <summary>The open connection to run commands on.</summary>
    public DbConnection Connection { get; } = connection;

    /// <summary>The transaction to enlist commands in, or <c>null</c> for autocommit.</summary>
    public DbTransaction? Transaction { get; } = transaction;

    /// <summary>Create a command already bound to the connection and transaction.</summary>
    public DbCommand CreateCommand()
    {
        var cmd = Connection.CreateCommand();
        cmd.Transaction = Transaction;
        return cmd;
    }

    public ValueTask DisposeAsync() => release?.Invoke() ?? ValueTask.CompletedTask;
}
