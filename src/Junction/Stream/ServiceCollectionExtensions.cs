using BulkForge.PostgreSql;
using Junction;
using Junction.Connectors;
using Junction.Internal;
using Junction.Stream.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Stream;

/// <summary>DI wiring for the Stream module. Also reachable through <c>AddJunction</c>.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register the Stream module with its own connection pool, for callers with no EF Core context in
    /// the picture. Appends then run on a rented connection, so they cannot join a transaction you
    /// opened elsewhere — use <see cref="AddStream{TContext}"/> when you need that.
    /// </summary>
    /// <param name="serializer">
    /// Serializer for the typed consumer API (<see cref="ISingleMessageConsumer{T}"/>,
    /// <see cref="StreamConsumer{T}"/>, <c>AppendAsync&lt;T&gt;</c>). Defaults to
    /// <see cref="JsonPayloadSerializer"/>. Applies to this stream registration only.
    /// </param>
    public static IServiceCollection AddStream(
        this IServiceCollection services,
        string connectionString,
        Action<StreamOptions>? configure = null,
        IPayloadSerializer? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        return AddCore(services, configure, serializer, _ => connectionString, hasConnector: false);
    }

    /// <summary>
    /// Register the Stream module on top of an existing EF Core context — the same connector Queue
    /// uses. Appends then run on <typeparamref name="TContext"/>'s connection and, when the caller
    /// already has one open, inside its current transaction — so an append and the caller's own writes
    /// commit together.
    /// <para>
    /// <typeparamref name="TContext"/> must be registered (scoped, as <c>AddDbContext</c> does) and
    /// must use the Npgsql provider. Its model does not need to know about the stream tables.
    /// </para>
    /// </summary>
    /// <param name="serializer">
    /// Serializer for the typed consumer API (<see cref="ISingleMessageConsumer{T}"/>,
    /// <see cref="StreamConsumer{T}"/>, <c>AppendAsync&lt;T&gt;</c>). Defaults to
    /// <see cref="JsonPayloadSerializer"/>. Applies to this stream registration only.
    /// </param>
    public static IServiceCollection AddStream<TContext>(
        this IServiceCollection services,
        Action<StreamOptions>? configure = null,
        IPayloadSerializer? serializer = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped(sp =>
            new StreamConnectionSource(new EfCoreConnectionSource(sp.GetRequiredService<TContext>())));

        return AddCore(services, configure, serializer, sp =>
        {
            using var scope = sp.CreateScope();
            return scope.ServiceProvider.GetRequiredService<TContext>().Database.GetConnectionString()!;
        }, hasConnector: true);
    }

    private static IServiceCollection AddCore(
        IServiceCollection services,
        Action<StreamOptions>? configure,
        IPayloadSerializer? serializer,
        Func<IServiceProvider, string> connectionString,
        bool hasConnector)
    {
        var options = new StreamOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);
        services.TryAddSingleton<StreamInitGate>();

        services.AddPooledDbContextFactory<JunctionDbContext>((sp, db) =>
        {
            var effectiveConnectionString = NpgsqlConnectionStrings.EnableAutoPrepare(connectionString(sp));
            db.UseNpgsql(effectiveConnectionString, npg =>
                npg.MigrationsHistoryTable("__ef_migrations", JunctionDbContext.Schema));
            db.UseBulkForge(); // enables the binary-COPY bulk append path (BulkForge.PostgreSql)
            if (options.EnableSensitiveDataLogging)
                db.EnableSensitiveDataLogging();
        });

        services.TryAddSingleton(new StreamPayloadSerializer(serializer ?? new JsonPayloadSerializer()));

        // Push delivery. Always registered, inert when disabled, and its LISTEN connection is only
        // opened once a consumer in this process actually waits for events.
        services.AddSingleton(sp => new StreamNotificationListener(
            NpgsqlConnectionStrings.EnableAutoPrepare(connectionString(sp)), options,
            sp.GetRequiredService<ILogger<StreamNotificationListener>>()));

        // Group commit defers appends to a background flusher with no caller in the picture, so it
        // can never join a caller's transaction — it always keeps its own pooled producer, regardless
        // of which overload registered the module.
        if (options.EnableGroupCommit)
        {
            services.AddSingleton<EventProducer>();
            services.AddSingleton<IEventProducer>(sp =>
                new GroupCommitProducer(
                    sp.GetRequiredService<EventProducer>(), options, sp.GetRequiredService<StreamPayloadSerializer>()));
        }
        else if (hasConnector)
        {
            services.TryAddScoped<IEventProducer, TransactionalEventProducer>();
        }
        else
        {
            services.AddSingleton<IEventProducer, EventProducer>();
        }

        services.TryAddScoped<IStreamClient, StreamClient>();

        return services;
    }

    /// <summary>
    /// Register a consumer class as a hosted background service. <typeparamref name="TConsumer"/>
    /// must implement exactly one of <see cref="ISingleMessageConsumer"/> or
    /// <see cref="IBatchMessageConsumer"/>. Requires a Generic Host and a prior <c>AddStream</c>.
    /// </summary>
    /// <param name="lifetime">
    /// Lifetime of the consumer class itself. Defaults to <see cref="ServiceLifetime.Scoped"/> so a
    /// fresh instance (with fresh scoped dependencies) handles each batch/message.
    /// </param>
    /// <remarks>
    /// <typeparamref name="TConsumer"/> must implement exactly one of the four consumer interfaces:
    /// <see cref="ISingleMessageConsumer"/>, <see cref="IBatchMessageConsumer"/> (raw
    /// <see cref="EventRecord"/>), or the typed <see cref="ISingleMessageConsumer{T}"/> /
    /// <see cref="IBatchMessageConsumer{T}"/> (deserialized business entity).
    /// </remarks>
    public static IServiceCollection AddStreamConsumer<TConsumer>(
        this IServiceCollection services,
        ServiceLifetime lifetime = ServiceLifetime.Scoped,
        Action<ConsumerHostOptions>? configure = null)
        where TConsumer : class, IStreamConsumerDefinition
    {
        var t = typeof(TConsumer);
        var interfaces = t.GetInterfaces();

        bool rawSingle = Array.Exists(interfaces, i => i == typeof(ISingleMessageConsumer));
        bool rawBatch = Array.Exists(interfaces, i => i == typeof(IBatchMessageConsumer));
        var typedSingle = Array.Find(interfaces,
            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISingleMessageConsumer<>));
        var typedBatch = Array.Find(interfaces,
            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IBatchMessageConsumer<>));

        int implemented = (rawSingle ? 1 : 0) + (rawBatch ? 1 : 0)
            + (typedSingle is not null ? 1 : 0) + (typedBatch is not null ? 1 : 0);
        if (implemented != 1)
            throw new InvalidOperationException(
                $"{t.Name} must implement exactly one consumer interface: " +
                "ISingleMessageConsumer, IBatchMessageConsumer, ISingleMessageConsumer<T> or IBatchMessageConsumer<T>.");

        Type hostType =
            rawSingle ? typeof(SingleRecordConsumerHost<>).MakeGenericType(t) :
            rawBatch ? typeof(BatchRecordConsumerHost<>).MakeGenericType(t) :
            typedSingle is not null
                ? typeof(SingleTypedConsumerHost<,>).MakeGenericType(t, typedSingle.GetGenericArguments()[0])
                : typeof(BatchTypedConsumerHost<,>).MakeGenericType(t, typedBatch!.GetGenericArguments()[0]);

        services.TryAdd(new ServiceDescriptor(t, t, lifetime));

        var options = new ConsumerHostOptions();
        configure?.Invoke(options);

        services.AddSingleton<IHostedService>(sp =>
            (IHostedService)ActivatorUtilities.CreateInstance(sp, hostType, options));

        return services;
    }
}
