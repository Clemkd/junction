using Junction;
using Junction.Connectors;
using Junction.Internal;
using Junction.Queue.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Junction.Queue;

/// <summary>DI wiring for the Queue module. Also reachable through <c>AddJunction</c>.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register the Queue module on top of an existing EF Core context — the default connector. Queue
    /// operations then run on <typeparamref name="TContext"/>'s connection and inside its current
    /// transaction, which is what lets a handler's writes and the message's completion commit as one.
    /// <para>
    /// <typeparamref name="TContext"/> must be registered (scoped, as <c>AddDbContext</c> does) and
    /// must use the Npgsql provider. Its model does not need to know about the queue tables; add
    /// <c>modelBuilder.AddJunctionModel()</c> only if you want them in your migrations.
    /// </para>
    /// </summary>
    public static IServiceCollection AddQueue<TContext>(
        this IServiceCollection services,
        Action<QueueOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IJunctionConnectionSource>(sp =>
            new EfCoreConnectionSource(sp.GetRequiredService<TContext>()));

        // The listener needs a connection of its own; borrow the context's connection string for it.
        return AddCore(services, configure, sp =>
        {
            using var scope = sp.CreateScope();
            return scope.ServiceProvider.GetRequiredService<TContext>().Database.GetConnectionString();
        });
    }

    /// <summary>
    /// Register the Queue module with its own connection pool, for callers with no EF Core context in
    /// the picture. Queue operations then run on a rented connection, so they cannot join a
    /// transaction you opened elsewhere — use <see cref="AddQueue{TContext}"/> or
    /// <see cref="IQueueClient.Using(System.Data.Common.DbConnection, System.Data.Common.DbTransaction?)"/>
    /// when you need that.
    /// </summary>
    public static IServiceCollection AddQueue(
        this IServiceCollection services,
        string connectionString,
        Action<QueueOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        string effectiveConnectionString = NpgsqlConnectionStrings.EnableAutoPrepare(connectionString);

        services.TryAddSingleton(_ => NpgsqlDataSource.Create(effectiveConnectionString));
        services.TryAddScoped<IJunctionConnectionSource>(sp =>
            new NpgsqlConnectionSource(sp.GetRequiredService<NpgsqlDataSource>()));

        return AddCore(services, configure, _ => effectiveConnectionString);
    }

    /// <summary>
    /// Run a handler class as a hosted worker. <typeparamref name="THandler"/> must implement exactly
    /// one of <see cref="IQueueMessageHandler"/>, <see cref="IQueueBatchHandler"/>,
    /// <see cref="IQueueMessageHandler{T}"/> or <see cref="IQueueBatchHandler{T}"/>. Requires a Generic
    /// Host and a prior <c>AddQueue</c>.
    /// <para>
    /// Register the same handler in as many processes as you like — that is the point of a shared
    /// queue. Each unit of work is claimed by exactly one of them.
    /// </para>
    /// </summary>
    /// <param name="lifetime">
    /// Lifetime of the handler class. Defaults to <see cref="ServiceLifetime.Scoped"/> so every unit of
    /// work gets a fresh instance with fresh scoped dependencies — including a fresh
    /// <c>DbContext</c>, which is what makes the transactional completion work.
    /// </param>
    public static IServiceCollection AddQueueWorker<THandler>(
        this IServiceCollection services,
        ServiceLifetime lifetime = ServiceLifetime.Scoped,
        Action<QueueWorkerOptions>? configure = null)
        where THandler : class, IQueueHandlerDefinition
    {
        ArgumentNullException.ThrowIfNull(services);

        var handlerType = typeof(THandler);
        var interfaces = handlerType.GetInterfaces();

        bool rawSingle = Array.Exists(interfaces, i => i == typeof(IQueueMessageHandler));
        bool rawBatch = Array.Exists(interfaces, i => i == typeof(IQueueBatchHandler));
        var typedSingle = Array.Find(interfaces,
            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueueMessageHandler<>));
        var typedBatch = Array.Find(interfaces,
            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueueBatchHandler<>));

        int implemented = (rawSingle ? 1 : 0) + (rawBatch ? 1 : 0)
            + (typedSingle is not null ? 1 : 0) + (typedBatch is not null ? 1 : 0);
        if (implemented != 1)
            throw new InvalidOperationException(
                $"{handlerType.Name} must implement exactly one handler interface: " +
                "IQueueMessageHandler, IQueueBatchHandler, IQueueMessageHandler<T> or IQueueBatchHandler<T>.");

        Type hostType =
            rawSingle ? typeof(SingleMessageWorkerHost<>).MakeGenericType(handlerType) :
            rawBatch ? typeof(BatchMessageWorkerHost<>).MakeGenericType(handlerType) :
            typedSingle is not null
                ? typeof(SingleTypedWorkerHost<,>).MakeGenericType(handlerType, typedSingle.GetGenericArguments()[0])
                : typeof(BatchTypedWorkerHost<,>).MakeGenericType(handlerType, typedBatch!.GetGenericArguments()[0]);

        services.TryAdd(new ServiceDescriptor(handlerType, handlerType, lifetime));

        var workerOptions = new QueueWorkerOptions();
        configure?.Invoke(workerOptions);

        services.AddSingleton<IHostedService>(sp =>
            (IHostedService)ActivatorUtilities.CreateInstance(sp, hostType, workerOptions));

        // A queue whose leases are never recovered leaks messages the first time a worker is killed.
        if (services.FirstOrDefault(d => d.ServiceType == typeof(QueueOptions))?.ImplementationInstance
                is not QueueOptions options || options.AutoMaintenance)
            services.AddQueueMaintenance();

        return services;
    }

    /// <summary>
    /// Run the maintenance loop: recover expired leases and prune retained rows. Idempotent, and
    /// registered automatically by <see cref="AddQueueWorker{THandler}"/> unless
    /// <see cref="QueueOptions.AutoMaintenance"/> is turned off. Safe to run on every instance.
    /// </summary>
    public static IServiceCollection AddQueueMaintenance(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(d => d.ServiceType == typeof(MaintenanceMarker)))
            return services;

        services.AddSingleton<MaintenanceMarker>();
        services.AddSingleton<IHostedService, QueueMaintenanceService>();
        return services;
    }

    /// <summary>
    /// Start filling the Queue module's <b>gauges</b>: queue depth, oldest-ready age, expired leases,
    /// dead letters, hot-table size and dead tuples, refreshed every
    /// <see cref="QueueOptions.MetricsInterval"/>. Idempotent.
    /// <para>
    /// Only the gauges need this. Counters and histograms are recorded on the hot path and are always
    /// published — an instrument with no listener costs a flag check, so there was nothing to make
    /// opt-in. The gauges are separate because their values come from queries.
    /// </para>
    /// <para>
    /// Nothing is exported until a collector subscribes to the <see cref="QueueDiagnostics.MeterName"/>
    /// meter; with OpenTelemetry that is
    /// <c>.WithMetrics(m =&gt; m.AddMeter(QueueDiagnostics.MeterName))</c>. Until one does, this service
    /// skips its queries entirely.
    /// </para>
    /// <para>
    /// Register it on one instance rather than all of them: the gauges describe the shared queue, so
    /// every replica would report the same numbers under a different series.
    /// </para>
    /// </summary>
    public static IServiceCollection AddQueueMetrics(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(d => d.ServiceType == typeof(MetricsMarker)))
            return services;

        services.AddSingleton<MetricsMarker>();
        services.AddSingleton<IHostedService, QueueMetricsCollector>();
        return services;
    }

    private static IServiceCollection AddCore(
        IServiceCollection services,
        Action<QueueOptions>? configure,
        Func<IServiceProvider, string?> listenerConnectionString)
    {
        var options = new QueueOptions();
        configure?.Invoke(options);

        services.AddSingleton(options);
        // GetService, not GetRequiredService: with nothing registered the catalog falls back to the
        // process-wide meter, which must stay out of the container — a container that owned it would
        // dispose it on teardown and silently kill every instrument for the rest of the process.
        services.AddSingleton(sp => new QueueCatalog(
            sp.GetRequiredService<QueueOptions>(), sp.GetService<QueueMetrics>()));
        services.TryAddSingleton<IPayloadSerializer>(new JsonPayloadSerializer());
        services.TryAddScoped<IQueueClient>(sp => new QueueClient(
            sp.GetRequiredService<QueueCatalog>(),
            sp.GetRequiredService<IJunctionConnectionSource>()));

        services.AddSingleton(new QueueListenerConnection(listenerConnectionString));

        if (options.EnableNotifications)
        {
            services.AddSingleton<PostgresListenerWakeup>();
            services.AddSingleton<IQueueWakeup>(sp => sp.GetRequiredService<PostgresListenerWakeup>());
            services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<PostgresListenerWakeup>());
        }

        services.TryAddSingleton<IQueueWakeup, PollingWakeup>();

        return services;
    }

    private sealed class MaintenanceMarker;

    private sealed class MetricsMarker;
}
