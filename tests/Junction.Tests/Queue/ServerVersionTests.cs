using Junction.Connectors;
using Junction.Queue;
using Junction.Queue.Internal;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// The version gate: Junction claims PostgreSQL 13+ and uses <c>uuidv7()</c> lease tokens only from
/// 18. These tests pin both halves of that claim against whichever server the suite is pointed at
/// (see <c>PostgresFixture.Image</c>), so running on <c>postgres:13-alpine</c> exercises the portable
/// branch and on 18 the uuidv7 one — with the same assertions either way.
/// </summary>
[Collection("postgres-queue")]
public sealed class ServerVersionTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task The_server_is_at_or_above_the_documented_floor()
    {
        var catalog = new QueueCatalog(new QueueOptions());
        await using var source = await SourceAsync();

        await catalog.DetectServerAsync(source.Value, CancellationToken.None);

        Assert.True(
            catalog.ServerVersion >= QueueSql.MinimumServerVersion,
            $"server_version_num {catalog.ServerVersion} is below the documented floor " +
            $"{QueueSql.MinimumServerVersion}.");
    }

    /// <summary>
    /// The dialect comes from the server, not from a build-time constant — and it stays portable until
    /// the server has actually been asked, which is what stops a claim from ever reaching a
    /// PostgreSQL 17 carrying a function that release does not have.
    /// </summary>
    [Fact]
    public async Task The_lease_token_dialect_follows_the_server_version()
    {
        var catalog = new QueueCatalog(new QueueOptions());
        Assert.False(catalog.Sql.UsesUuidV7);   // nothing asked yet: portable

        await using var source = await SourceAsync();
        await catalog.DetectServerAsync(source.Value, CancellationToken.None);

        Assert.Equal(catalog.ServerVersion >= QueueSql.UuidV7ServerVersion, catalog.Sql.UsesUuidV7);
    }

    /// <summary>
    /// End-to-end proof that whichever dialect was chosen actually parses and runs here: a claim comes
    /// back with a lease token, and that token still fences the acknowledge. A wrong choice fails this
    /// test on the claim itself rather than silently degrading.
    /// </summary>
    [Fact]
    public async Task A_claim_produces_a_lease_token_that_fences_the_acknowledge()
    {
        string queue = PostgresFixture.NewQueue("version");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w1").ClaimAsync());

        Assert.NotNull(claimed);
        Assert.NotEqual(Guid.Empty, claimed.LeaseToken);

        // The version nibble of the token the *server* generated, not the flag we set locally: this is
        // what distinguishes "we decided to use uuidv7" from "uuidv7 actually ran". Position 14 of the
        // canonical form is the RFC 9562 version digit.
        var catalog = new QueueCatalog(new QueueOptions());
        await using var source = await SourceAsync();
        await catalog.DetectServerAsync(source.Value, CancellationToken.None);

        char expected = catalog.ServerVersion >= QueueSql.UuidV7ServerVersion ? '7' : '4';
        Assert.Equal(expected, claimed.LeaseToken.ToString("D")[14]);

        bool acknowledged = await TestHelpers.WithClientAsync(
            _sp, c => c.GetConsumer(queue, "w1").TryAcknowledgeAsync(claimed));

        Assert.True(acknowledged);
    }

    /// <summary>A server below the floor is refused up front, with the version in the message.</summary>
    [Fact]
    public void The_supported_floor_is_the_release_that_made_gen_random_uuid_a_core_function()
    {
        Assert.Equal(130000, QueueSql.MinimumServerVersion);
        Assert.Equal(180000, QueueSql.UuidV7ServerVersion);
    }

    /// <summary>An opened connection wrapped as a connector, disposed with the test.</summary>
    private async Task<OwnedSource> SourceAsync()
    {
        var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        return new OwnedSource(connection, new ExistingConnectionSource(connection));
    }

    private sealed record OwnedSource(NpgsqlConnection Connection, IJunctionConnectionSource Value)
        : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => Connection.DisposeAsync();
    }
}
