using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// The consumer half of the shared-transaction guarantee. Producing already commits with the caller's
/// writes (see <see cref="TransactionalTests"/>); this pins the other direction — a hosted consumer's
/// own writes and the cursor advance past the event that caused them are one commit, so a crash can
/// neither apply the effect without recording it nor record it without applying the effect.
/// <para>
/// The interesting case is the rollback: a consumer that throws after writing must leave <b>both</b>
/// its write and the cursor untouched, so the event comes back. An implementation that advanced the
/// in-memory cursor before the commit would pass the happy path and silently skip events here.
/// </para>
/// </summary>
[Collection("postgres-stream")]
public sealed class TransactionalConsumerTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static long _idSeq = DateTime.UtcNow.Ticks;

    private static long NextId() => Interlocked.Increment(ref _idSeq);

    private sealed class Config
    {
        public required string Stream { get; init; }
        public string Consumer { get; init; } = "tx-consumer";
        public long FailUntilAttempt { get; set; }
        public int Attempts;
        public readonly List<long> Handled = [];
    }

    /// <summary>
    /// Writes a business row, then optionally throws. The row is written through the scope's own
    /// <c>TestDbContext</c> — which is the whole point: that is the context the host opened the
    /// transaction on.
    /// </summary>
    private sealed class WritingConsumer(Config cfg, TestDbContext db) : ISingleMessageConsumer
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;

        public async Task ConsumeAsync(EventRecord message, CancellationToken ct = default)
        {
            int attempt = Interlocked.Increment(ref cfg.Attempts);

            db.Records.Add(new BusinessRecord { Id = NextId(), Value = $"{cfg.Stream}:{message.Offset}" });
            await db.SaveChangesAsync(ct);

            if (attempt <= cfg.FailUntilAttempt)
                throw new InvalidOperationException($"attempt {attempt} fails after writing");

            lock (cfg.Handled)
                cfg.Handled.Add(message.Offset);
        }
    }

    private async Task<(ServiceProvider sp, List<IHostedService> hosted)> StartAsync(
        Config cfg, Action<ConsumerHostOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddSingleton(cfg);
        services.AddStream<TestDbContext>();
        services.AddStreamConsumer<WritingConsumer>(configure: o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(30);
            o.ErrorRetryDelay = TimeSpan.FromMilliseconds(30);
            o.MaxAttempts = 5;
            configure?.Invoke(o);
        });

        var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var h in hosted)
            await h.StartAsync(CancellationToken.None);
        return (sp, hosted);
    }

    private async Task<long> AppendAsync(string stream, int count)
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();
        var result = await client.Producer.AppendAsync(
            stream, Enumerable.Range(0, count).Select(i => EventData.FromText("T", $"e{i}")).ToList());
        return result.FirstOffset;
    }

    /// <summary>
    /// Business rows written by this test. Matched on the test's own stream name: the table is shared
    /// by the whole collection, so an unqualified count picks up other tests' rows.
    /// </summary>
    private async Task<int> CountBusinessRowsAsync(string valuePrefix)
    {
        await using var sp = fixture.BuildProvider();
        await using var conn = new Npgsql.NpgsqlConnection(fixture.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count(*) FROM public.business_records WHERE value LIKE @p";
        var p = cmd.CreateParameter();
        p.ParameterName = "p";
        p.Value = valuePrefix + "%";
        cmd.Parameters.Add(p);
        return (int)(long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<long> CursorAsync(string stream, string consumer)
    {
        await using var sp = fixture.BuildProvider();
        var lag = await sp.GetRequiredService<IStreamClient>().GetConsumerLagAsync(stream, consumer);
        return lag.Position;
    }

    [Fact]
    public async Task A_consumers_write_and_its_cursor_commit_together()
    {
        var cfg = new Config { Stream = PostgresFixture.NewName("txc-ok") };
        await AppendAsync(cfg.Stream, 3);

        var (sp, hosted) = await StartAsync(cfg);
        await using (sp)
        {
            bool caughtUp = await WaitAsync(async () => await CursorAsync(cfg.Stream, cfg.Consumer) == 3);
            await StopAsync(hosted);

            Assert.True(caughtUp, "the cursor never reached the end of the stream");
            Assert.Equal(3, cfg.Handled.Count);
            Assert.Equal(3, await CountBusinessRowsAsync(cfg.Stream + ":"));
        }
    }

    /// <summary>
    /// The load-bearing test. The consumer writes a row and then throws, twice. Each failed attempt
    /// must roll back the row <i>and</i> leave the cursor where it was; the third attempt then commits
    /// both. So the stream is fully consumed, and exactly one business row per event survives — not
    /// three rolled-back ones on top.
    /// </summary>
    [Fact]
    public async Task A_failed_attempt_rolls_back_the_write_and_leaves_the_cursor_put()
    {
        var cfg = new Config { Stream = PostgresFixture.NewName("txc-rollback"), FailUntilAttempt = 2 };
        await AppendAsync(cfg.Stream, 1);

        var (sp, hosted) = await StartAsync(cfg);
        await using (sp)
        {
            bool caughtUp = await WaitAsync(async () => await CursorAsync(cfg.Stream, cfg.Consumer) == 1);
            await StopAsync(hosted);

            Assert.True(caughtUp, "the event was never committed");
            Assert.Equal(3, Volatile.Read(ref cfg.Attempts));   // two rolled back, the third committed

            // One row, not three: the two failed attempts' writes went away with their transactions.
            Assert.Equal(1, await CountBusinessRowsAsync(cfg.Stream + ":"));
        }
    }

    /// <summary>
    /// With the option off, the handler's write is committed on its own and survives a later failure —
    /// the pre-existing at-least-once behaviour. Worth pinning so the default cannot be flipped without
    /// someone noticing that this is the semantics it replaces.
    /// </summary>
    [Fact]
    public async Task Without_transactional_commit_a_failed_attempts_write_survives()
    {
        var cfg = new Config { Stream = PostgresFixture.NewName("txc-off"), FailUntilAttempt = 2 };
        await AppendAsync(cfg.Stream, 1);

        var (sp, hosted) = await StartAsync(cfg, o => o.TransactionalCommit = false);
        await using (sp)
        {
            bool caughtUp = await WaitAsync(async () => await CursorAsync(cfg.Stream, cfg.Consumer) == 1);
            await StopAsync(hosted);

            Assert.True(caughtUp);
            // Three attempts, three committed rows: nothing rolled anything back.
            Assert.Equal(3, await CountBusinessRowsAsync(cfg.Stream + ":"));
        }
    }

    /// <summary>
    /// The connection-string registration has no caller connection to join, so the option is inert
    /// rather than an error: consumers still run, still commit their cursor, just not transactionally.
    /// </summary>
    [Fact]
    public async Task Transactional_commit_is_inert_without_a_connector()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStream(fixture.ConnectionString);
        await using var sp = services.BuildServiceProvider();

        var client = sp.GetRequiredService<IStreamClient>();
        Assert.Null(await client.BeginTransactionAsync());
    }

    private static async Task<bool> WaitAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return true;
            await Task.Delay(50);
        }
        return await condition();
    }

    private static async Task StopAsync(List<IHostedService> hosted)
    {
        foreach (var h in hosted)
            await h.StopAsync(CancellationToken.None);
    }
}
