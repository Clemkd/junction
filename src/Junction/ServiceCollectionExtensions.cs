using System.Reflection;
using Junction.Queue;
using Junction.Stream;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Junction;

/// <summary>
/// Single entry point that wires up both modules with one coherent registration. Reach for
/// <c>AddQueue</c> / <c>AddStream</c> directly (see <c>Junction.Queue.ServiceCollectionExtensions</c>
/// / <c>Junction.Stream.ServiceCollectionExtensions</c>) instead when a process only needs one module.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register both modules with their own connection pools, for callers with no EF Core context in
    /// the picture. The Queue module's hot path runs on a rented connection (see <c>AddQueue</c>); the
    /// Stream module owns a pooled <see cref="Stream.JunctionDbContext"/> factory (see
    /// <c>AddStream</c>). Use <see cref="AddJunction{TContext}"/> instead when you want Queue
    /// completions to ride along with your own EF Core transaction.
    /// </summary>
    /// <param name="queueSerializer">Serializer for the Queue module's typed handler API. Defaults to
    /// <see cref="JsonPayloadSerializer"/>.</param>
    /// <param name="streamSerializer">Serializer for the Stream module's typed consumer API. Defaults
    /// to <see cref="JsonPayloadSerializer"/>.</param>
    public static IServiceCollection AddJunction(
        this IServiceCollection services,
        string connectionString,
        Action<JunctionOptions>? configure = null,
        IPayloadSerializer? queueSerializer = null,
        IPayloadSerializer? streamSerializer = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var options = new JunctionOptions();
        configure?.Invoke(options);

        services.AddQueue(connectionString, o => CopyQueueOptions(options.Queue, o), queueSerializer);
        services.AddStream(connectionString, o => CopyStreamOptions(options.Stream, o), streamSerializer);
        services.TryAddScoped<IJunctionClient, JunctionClient>();

        return services;
    }

    /// <summary>
    /// Register the Queue module on top of an existing EF Core context — so a handler's writes and a
    /// message's completion commit together (see <c>AddQueue&lt;TContext&gt;</c>) — while the Stream
    /// module gets its own pooled connection factory built from <paramref name="connectionString"/>.
    /// <para>
    /// The two modules cannot yet share one connection-acquisition strategy: the Stream module builds
    /// its <see cref="Microsoft.EntityFrameworkCore.IDbContextFactory{TContext}"/> eagerly at
    /// registration time, so its connection string must be supplied explicitly even though
    /// <typeparamref name="TContext"/> already has one — a known asymmetry, tracked as a follow-up
    /// (see <c>docs/DESIGN.md</c>), not silently papered over here.
    /// </para>
    /// </summary>
    /// <param name="queueSerializer">Serializer for the Queue module's typed handler API. Defaults to
    /// <see cref="JsonPayloadSerializer"/>.</param>
    /// <param name="streamSerializer">Serializer for the Stream module's typed consumer API. Defaults
    /// to <see cref="JsonPayloadSerializer"/>.</param>
    public static IServiceCollection AddJunction<TContext>(
        this IServiceCollection services,
        string connectionString,
        Action<JunctionOptions>? configure = null,
        IPayloadSerializer? queueSerializer = null,
        IPayloadSerializer? streamSerializer = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var options = new JunctionOptions();
        configure?.Invoke(options);

        services.AddQueue<TContext>(o => CopyQueueOptions(options.Queue, o), queueSerializer);
        services.AddStream(connectionString, o => CopyStreamOptions(options.Stream, o), streamSerializer);
        services.TryAddScoped<IJunctionClient, JunctionClient>();

        return services;
    }

    /// <summary>Run a Queue handler class as a hosted worker. See <c>AddQueueWorker&lt;THandler&gt;</c>.</summary>
    public static IServiceCollection AddJunctionQueueWorker<THandler>(
        this IServiceCollection services,
        ServiceLifetime lifetime = ServiceLifetime.Scoped,
        Action<QueueWorkerOptions>? configure = null)
        where THandler : class, IQueueHandlerDefinition =>
        services.AddQueueWorker<THandler>(lifetime, configure);

    /// <summary>Run a Stream consumer class as a hosted worker. See <c>AddStreamConsumer&lt;TConsumer&gt;</c>.</summary>
    public static IServiceCollection AddJunctionStreamConsumer<TConsumer>(
        this IServiceCollection services,
        ServiceLifetime lifetime = ServiceLifetime.Scoped,
        Action<ConsumerHostOptions>? configure = null)
        where TConsumer : class, IStreamConsumerDefinition =>
        services.AddStreamConsumer<TConsumer>(lifetime, configure);

    /// <summary>Run the Queue module's maintenance loop. See <c>AddQueueMaintenance</c>.</summary>
    public static IServiceCollection AddJunctionMaintenance(this IServiceCollection services) =>
        services.AddQueueMaintenance();

    /// <summary>Start filling the Queue module's gauges. See <c>AddQueueMetrics</c>.</summary>
    public static IServiceCollection AddJunctionMetrics(this IServiceCollection services) =>
        services.AddQueueMetrics();

    private static void CopyQueueOptions(QueueOptions source, QueueOptions target) => CopyProperties(source, target);

    private static void CopyStreamOptions(StreamOptions source, StreamOptions target) => CopyProperties(source, target);

    /// <summary>
    /// <c>AddQueue</c>/<c>AddStream</c> each build their own fresh options instance from the
    /// <c>Action&lt;TOptions&gt;</c> callback passed in, so the already-configured
    /// <see cref="JunctionOptions.Queue"/>/<see cref="JunctionOptions.Stream"/> built here has to be
    /// copied onto it. Reflected over every public settable property rather than listed by hand, so a
    /// property added to either options type later is carried across automatically instead of being
    /// silently dropped by a copy list nobody remembered to update.
    /// </summary>
    private static void CopyProperties<T>(T source, T target) where T : class
    {
        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.CanRead && property.CanWrite && property.GetIndexParameters().Length == 0)
                property.SetValue(target, property.GetValue(source));
        }
    }
}
