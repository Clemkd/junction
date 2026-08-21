using Junction;
using Junction.Queue;
using Junction.Stream;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// Registration mistakes that used to be accepted in silence and produce a half-configured module.
/// None of these touch a database — they are all decided while the container is being built, which is
/// the point: the failure should happen at startup, next to the call that caused it, not as behaviour
/// somebody has to explain in production.
/// </summary>
public sealed class RegistrationGuardTests
{
    private const string ConnectionString = "Host=localhost;Database=unused;Username=u;Password=p";

    private sealed class Ctx(DbContextOptions<Ctx> options) : DbContext(options);

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<Ctx>(o => o.UseNpgsql(ConnectionString));
        return services;
    }

    /// <summary>
    /// The trap: options go in with <c>AddSingleton</c> (last wins) and the connector with
    /// <c>TryAddScoped</c> (first wins), so a second registration used to keep the first call's
    /// connector and the second call's options — a module configured half one way, half the other.
    /// </summary>
    [Fact]
    public void Registering_the_queue_module_twice_is_refused()
    {
        var services = Services();
        services.AddQueue<Ctx>();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddQueue(ConnectionString));
        Assert.Contains("already registered", error.Message);
    }

    [Fact]
    public void Registering_the_stream_module_twice_is_refused()
    {
        var services = Services();
        services.AddStream<Ctx>();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddStream(ConnectionString));
        Assert.Contains("already registered", error.Message);
    }

    /// <summary>AddJunction registers both, so a stray AddQueue afterwards is the same mistake.</summary>
    [Fact]
    public void AddJunction_followed_by_a_stray_module_registration_is_refused()
    {
        var services = Services();
        services.AddJunction<Ctx>();

        Assert.Throws<InvalidOperationException>(() => services.AddQueue<Ctx>());
        Assert.Throws<InvalidOperationException>(() => services.AddStream<Ctx>());
    }

    /// <summary>
    /// Registering the two modules in either order stays legal — they are separate modules, and
    /// MixedRegistrationTests covers that each keeps its own connector.
    /// </summary>
    [Fact]
    public void Registering_both_modules_in_either_order_is_allowed()
    {
        var queueFirst = Services();
        queueFirst.AddQueue<Ctx>();
        queueFirst.AddStream<Ctx>();

        var streamFirst = Services();
        streamFirst.AddStream<Ctx>();
        streamFirst.AddQueue<Ctx>();

        using var a = queueFirst.BuildServiceProvider();
        using var b = streamFirst.BuildServiceProvider();
        Assert.NotNull(a.GetRequiredService<QueueOptions>());
        Assert.NotNull(b.GetRequiredService<QueueOptions>());
    }
}
