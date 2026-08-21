using Junction.Stream.Model;
using Microsoft.EntityFrameworkCore;

namespace Junction.Stream;

/// <summary>
/// EF Core context backing the Stream module. All objects live under the <see cref="Schema"/>
/// PostgreSQL schema — shared with the Queue module (table names do not collide). Column names
/// are pinned to snake_case so the library's hand-written SQL (offset allocation, upserts,
/// storage statistics) stays predictable.
/// </summary>
public sealed class JunctionDbContext(DbContextOptions<JunctionDbContext> options)
    : DbContext(options)
{
    public const string Schema = "junction";

    public DbSet<StreamDefinition> Streams => Set<StreamDefinition>();
    public DbSet<StreamRecordEntity> Records => Set<StreamRecordEntity>();
    public DbSet<ConsumerCursor> Cursors => Set<ConsumerCursor>();
    public DbSet<StreamDeadLetterEntity> DeadLetters => Set<StreamDeadLetterEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(Schema);

        modelBuilder.Entity<StreamDefinition>(e =>
        {
            e.ToTable("streams");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").UseIdentityByDefaultColumn();
            e.Property(x => x.Name).HasColumnName("name").IsRequired();
            e.Property(x => x.NextSequence).HasColumnName("next_seq");
            e.Property(x => x.CreatedAt).HasColumnName("created_at").HasColumnType("timestamp with time zone");
            e.HasIndex(x => x.Name).IsUnique().HasDatabaseName("ux_streams_name");
        });

        modelBuilder.Entity<StreamRecordEntity>(e =>
        {
            e.ToTable("stream_events");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").UseIdentityByDefaultColumn();
            e.Property(x => x.StreamId).HasColumnName("stream_id");
            e.Property(x => x.Sequence).HasColumnName("seq");
            e.Property(x => x.EventKey).HasColumnName("event_key");
            e.Property(x => x.EventType).HasColumnName("event_type").IsRequired();
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("bytea");
            e.Property(x => x.Headers).HasColumnName("headers").HasColumnType("jsonb");
            e.Property(x => x.CreatedAt).HasColumnName("created_at").HasColumnType("timestamp with time zone");
            // Primary (and only) access path: range scan by (stream, seq) for a consumer poll.
            // No index on event_key: nothing queries by key in v1, so an extra index would only
            // add write amplification and storage. Add one if you introduce key-based lookups.
            e.HasIndex(x => new { x.StreamId, x.Sequence })
                .IsUnique()
                .HasDatabaseName("ux_events_stream_seq");
        });

        modelBuilder.Entity<ConsumerCursor>(e =>
        {
            e.ToTable("consumer_cursors");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").UseIdentityByDefaultColumn();
            e.Property(x => x.StreamId).HasColumnName("stream_id");
            e.Property(x => x.ConsumerName).HasColumnName("consumer_name").IsRequired();
            e.Property(x => x.Position).HasColumnName("position");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasColumnType("timestamp with time zone");
            e.HasIndex(x => new { x.StreamId, x.ConsumerName })
                .IsUnique()
                .HasDatabaseName("ux_cursor_stream_consumer");
        });

        modelBuilder.Entity<StreamDeadLetterEntity>(e =>
        {
            e.ToTable("stream_dead_letters");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").UseIdentityByDefaultColumn();
            e.Property(x => x.StreamId).HasColumnName("stream_id");
            e.Property(x => x.ConsumerName).HasColumnName("consumer_name").IsRequired();
            e.Property(x => x.Sequence).HasColumnName("seq");
            e.Property(x => x.EventKey).HasColumnName("event_key");
            e.Property(x => x.EventType).HasColumnName("event_type").IsRequired();
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("bytea");
            e.Property(x => x.Headers).HasColumnName("headers").HasColumnType("jsonb");
            e.Property(x => x.Attempts).HasColumnName("attempts");
            e.Property(x => x.FailedAt).HasColumnName("failed_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.Error).HasColumnName("error");
            e.HasIndex(x => new { x.StreamId, x.ConsumerName, x.FailedAt })
                .HasDatabaseName("ix_dead_letters_stream_consumer");
        });
    }
}
