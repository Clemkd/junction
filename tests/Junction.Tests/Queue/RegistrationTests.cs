using Junction.Queue.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>DI wiring: what gets registered, and what is refused early.</summary>
public sealed class RegistrationTests
{
    private const string ConnectionString = "Host=localhost;Database=junction;Username=postgres;Password=postgres";

    private sealed class GoodHandler : IQueueMessageHandler
    {
        public string Queue => "q";

        public Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed class BatchAndSingleHandler : IQueueMessageHandler, IQueueBatchHandler
    {
        public string Queue => "q";

        public int BatchSize => 10;

        public Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task HandleAsync(IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed class NoInterfaceHandler : IQueueHandlerDefinition
    {
        public string Queue => "q";
    }

    private static ServiceProvider BuildStandalone(Action<QueueOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(ConnectionString, configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    [Fact]
    public void The_client_and_its_collaborators_resolve()
    {
        using var provider = BuildStandalone();
        using var scope = provider.CreateScope();

        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

        Assert.NotNull(client.Producer);
        Assert.NotNull(client.GetConsumer("q"));
        Assert.NotNull(provider.GetRequiredService<IMessageSerializer>());
        Assert.NotNull(provider.GetRequiredService<IQueueWakeup>());
    }

    [Fact]
    public void The_client_is_scoped_so_each_unit_of_work_gets_its_own_connection()
    {
        using var provider = BuildStandalone();
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        Assert.NotSame(
            first.ServiceProvider.GetRequiredService<IQueueClient>(),
            second.ServiceProvider.GetRequiredService<IQueueClient>());
        Assert.Same(
            first.ServiceProvider.GetRequiredService<IQueueClient>(),
            first.ServiceProvider.GetRequiredService<IQueueClient>());
    }

    [Fact]
    public void Options_are_applied()
    {
        using var provider = BuildStandalone(o =>
        {
            o.MaxAttempts = 9;
            o.LeaseDuration = TimeSpan.FromMinutes(2);
            o.Completion = CompletionMode.Archive;
        });

        var options = provider.GetRequiredService<QueueOptions>();

        Assert.Equal(9, options.MaxAttempts);
        Assert.Equal(TimeSpan.FromMinutes(2), options.LeaseDuration);
        Assert.Equal(CompletionMode.Archive, options.Completion);
    }

    [Fact]
    public void A_worker_registration_also_brings_the_maintenance_loop()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(ConnectionString);
        services.AddQueueWorker<GoodHandler>();
        using var provider = services.BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().ToList();

        Assert.Equal(2, hosted.Count);   // the worker and the janitor
        Assert.Contains(hosted, h => h is QueueMaintenanceService);
    }

    [Fact]
    public void Automatic_maintenance_can_be_declined()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(ConnectionString, o => o.AutoMaintenance = false);
        services.AddQueueWorker<GoodHandler>();
        using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetServices<IHostedService>());
    }

    [Fact]
    public void Registering_the_maintenance_loop_twice_starts_one_loop()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(ConnectionString);
        services.AddQueueMaintenance();
        services.AddQueueMaintenance();
        using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetServices<IHostedService>());
    }

    [Fact]
    public void A_handler_implementing_two_shapes_is_refused()
    {
        var services = new ServiceCollection();
        services.AddQueue(ConnectionString);

        var thrown = Assert.Throws<InvalidOperationException>(
            () => services.AddQueueWorker<BatchAndSingleHandler>());

        Assert.Contains("exactly one handler interface", thrown.Message);
    }

    [Fact]
    public void A_handler_implementing_no_shape_is_refused()
    {
        var services = new ServiceCollection();
        services.AddQueue(ConnectionString);

        Assert.Throws<InvalidOperationException>(() => services.AddQueueWorker<NoInterfaceHandler>());
    }

    [Fact]
    public void An_invalid_schema_name_is_refused_before_any_sql_runs()
    {
        Assert.Throws<ArgumentException>(() => new QueueOptions { Schema = "public; DROP TABLE users" });
        Assert.Throws<ArgumentException>(() => new QueueOptions { Schema = "Mixed_Case" });
        Assert.Equal("my_queues", new QueueOptions { Schema = "my_queues" }.Schema);
    }

    [Fact]
    public void Notifications_add_the_listener_service()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(ConnectionString, o => o.EnableNotifications = true);
        using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetServices<IHostedService>());   // the listener
        Assert.IsType<PostgresListenerWakeup>(provider.GetRequiredService<IQueueWakeup>());
    }

    [Fact]
    public void Without_notifications_workers_fall_back_to_polling()
    {
        using var provider = BuildStandalone();

        Assert.IsType<PollingWakeup>(provider.GetRequiredService<IQueueWakeup>());
    }

    [Fact]
    public void The_ef_connector_requires_the_context_to_be_registered()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue<TestDbContext>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        // No AddDbContext: the failure must surface at resolution, not as a silent no-op.
        Assert.Throws<InvalidOperationException>(
            () => scope.ServiceProvider.GetRequiredService<IQueueClient>());
    }
}
