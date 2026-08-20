using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Junction.Tests.Stream;

public sealed class RegistrationTests
{
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
