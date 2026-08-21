using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

using Junction.Stream;
using Junction.Stream.Internal;

namespace Junction.Tests.Stream;

public sealed class RegistrationTests
{
    private const string FakeConnectionString = "Host=localhost;Database=fake;Username=x;Password=x";

    [Fact]
    public void AddStream_with_a_connection_string_registers_the_pooled_producer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStream(FakeConnectionString);
        var provider = services.BuildServiceProvider();

        Assert.IsType<EventProducer>(provider.GetRequiredService<IEventProducer>());
    }

    [Fact]
    public void AddStream_of_TContext_registers_the_ambient_transaction_producer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(FakeConnectionString));
        services.AddStream<TestDbContext>();
        var provider = services.BuildServiceProvider();

        Assert.IsType<TransactionalEventProducer>(provider.GetRequiredService<IEventProducer>());
    }

    [Fact]
    public void AddStream_of_TContext_with_group_commit_still_uses_the_pooled_producer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(FakeConnectionString));
        services.AddStream<TestDbContext>(o => o.EnableGroupCommit = true);
        var provider = services.BuildServiceProvider();

        // Group commit defers appends to a background flusher with no caller in the picture, so it
        // never rides an ambient transaction — even when AddStream<TContext> is what registered it.
        Assert.IsType<GroupCommitProducer>(provider.GetRequiredService<IEventProducer>());
    }

    private sealed class NoInterface : IStreamConsumerDefinition
    {
        public string Stream => "s";
        public string ConsumerName => "c";
    }

    private sealed class Dual : ISingleMessageConsumer, IBatchMessageConsumer
    {
        public string Stream => "s";
        public string ConsumerName => "c";
        public int BatchSize => 1;
        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default) => Task.CompletedTask;
        public Task ConsumeAsync(IReadOnlyList<EventRecord> messages, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class RawSingle : ISingleMessageConsumer
    {
        public string Stream => "s";
        public string ConsumerName => "c";
        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed record Msg(int Id);

    private sealed class TypedBatch : IBatchMessageConsumer<Msg>
    {
        public string Stream => "s";
        public string ConsumerName => "c";
        public int BatchSize => 10;
        public Task ConsumeAsync(IReadOnlyList<Msg> messages, CancellationToken ct = default) => Task.CompletedTask;
    }

    [Fact]
    public void Registering_a_type_with_no_consumer_interface_throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddStreamConsumer<NoInterface>());
    }

    [Fact]
    public void Registering_a_type_with_two_consumer_interfaces_throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddStreamConsumer<Dual>());
    }

    [Fact]
    public void Registering_a_raw_single_consumer_adds_hosted_service_and_the_consumer()
    {
        var services = new ServiceCollection();
        services.AddStreamConsumer<RawSingle>();

        Assert.Contains(services, d => d.ServiceType == typeof(IHostedService));
        Assert.Contains(services, d => d.ServiceType == typeof(RawSingle));
    }

    [Fact]
    public void Registering_a_typed_batch_consumer_adds_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddStreamConsumer<TypedBatch>();

        Assert.Contains(services, d => d.ServiceType == typeof(IHostedService));
    }
}
