using BulkForge.PostgreSql;
using Junction;
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
    /// Register the Stream module against a PostgreSQL database. Adds a pooled
    /// <see cref="JunctionDbContext"/> factory, the shared <see cref="IEventProducer"/>
    /// and the <see cref="IStreamClient"/> entry point.
    /// </summary>
    public static IServiceCollection AddStream(
        this IServiceCollection services,
        string connectionString,
        Action<StreamOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var options = new StreamOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);

        var effectiveConnectionString = NpgsqlConnectionStrings.EnableAutoPrepare(connectionString);

        services.AddPooledDbContextFactory<JunctionDbContext>(db =>
        {
            db.UseNpgsql(effectiveConnectionString, npg =>
                npg.MigrationsHistoryTable("__ef_migrations", JunctionDbContext.Schema));
            db.UseBulkForge(); // enables the binary-COPY bulk append path (BulkForge.PostgreSql)
            if (options.EnableSensitiveDataLogging)
                db.EnableSensitiveDataLogging();
        });

        services.TryAddSingleton<IPayloadSerializer>(new JsonPayloadSerializer());

        // Push delivery. Always registered, inert when disabled, and its LISTEN connection is only
        // opened once a consumer in this process actually waits for events.
        services.AddSingleton(sp => new StreamNotificationListener(
            effectiveConnectionString, options,
            sp.GetRequiredService<ILogger<StreamNotificationListener>>()));

        if (options.EnableGroupCommit)
        {
            services.AddSingleton<EventProducer>();
            services.AddSingleton<IEventProducer>(sp =>
                new GroupCommitProducer(sp.GetRequiredService<EventProducer>(), options));
        }
        else
        {
            services.AddSingleton<IEventProducer, EventProducer>();
        }

        services.AddSingleton<IStreamClient, StreamClient>();

        return services;
    }

    /// <summary>
    /// Register a consumer class as a hosted background service. <typeparamref name="TConsumer"/>
    /// must implement exactly one of <see cref="ISingleMessageConsumer"/> or
    /// <see cref="IBatchMessageConsumer"/>. Requires a Generic Host and a prior
    /// <see cref="AddStream(IServiceCollection, string, Action{StreamOptions})"/>.
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
