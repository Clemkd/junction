using Microsoft.EntityFrameworkCore;

namespace Junction.Queue.Model;

/// <summary>
/// Adds the queue tables to <i>your</i> <c>DbContext</c> model, so they are created and versioned by
/// your own EF migrations instead of by <see cref="QueueOptions.AutoCreateSchema"/>. The production
/// path: schema changes go through the same review and deployment as the rest of your database.
/// </summary>
public static class JunctionModelExtensions
{
    /// <summary>
    /// Map the queue tables into this model. Call from <c>OnModelCreating</c> and turn
    /// <see cref="QueueOptions.AutoCreateSchema"/> off:
    /// <code>
    /// protected override void OnModelCreating(ModelBuilder modelBuilder)
    /// {
    ///     base.OnModelCreating(modelBuilder);
    ///     modelBuilder.AddJunctionModel();
    /// }
    /// </code>
    /// The generated migration will not carry the storage tuning from
    /// <see cref="QueueSchema.TuningScript"/> (fillfactor, per-table autovacuum) — EF has no model
    /// concept for those. Add them to the migration by hand with <c>migrationBuilder.Sql(...)</c>;
    /// on a busy queue they matter.
    /// </summary>
    /// <param name="includeStarvationIndex">
    /// Also map the index the starvation-free claim needs (see
    /// <see cref="QueueOptions.StarvationThreshold"/>). Off by default, to match the DDL path: it is a
    /// second index on the hot table and only pays for itself when the option is set.
    /// </param>
    public static ModelBuilder AddJunctionModel(
        this ModelBuilder modelBuilder,
        string schema = QueueSchema.DefaultSchema,
        bool includeStarvationIndex = false)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);

        modelBuilder.Entity<QueueDefinition>(e =>
        {
            e.ToTable("queues", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            e.Property(x => x.Name).HasColumnName("name").IsRequired();
            // The library's SQL inserts a queue row with its name alone and lets the database stamp the
            // rest, so the defaults are part of the contract — not decoration.
            e.Property(x => x.CreatedAt).HasColumnName("created_at")
                .HasColumnType("timestamp with time zone").HasDefaultValueSql("now()");
            e.HasIndex(x => x.Name).IsUnique().HasDatabaseName("ux_queues_name");
        });

        modelBuilder.Entity<QueueMessageEntity>(e =>
        {
            e.ToTable("messages", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            e.Property(x => x.QueueId).HasColumnName("queue_id");
            e.Property(x => x.State).HasColumnName("state").HasColumnType("smallint")
                .HasDefaultValue(QueueMessageState.Ready);
            e.Property(x => x.Priority).HasColumnName("priority").HasDefaultValue(0);
            e.Property(x => x.VisibleAt).HasColumnName("visible_at")
                .HasColumnType("timestamp with time zone").HasDefaultValueSql("now()");
            e.Property(x => x.EnqueuedAt).HasColumnName("enqueued_at")
                .HasColumnType("timestamp with time zone").HasDefaultValueSql("now()");
            e.Property(x => x.Attempts).HasColumnName("attempts").HasDefaultValue(0);
            e.Property(x => x.MaxAttempts).HasColumnName("max_attempts").HasDefaultValue(5);
            e.Property(x => x.LeaseToken).HasColumnName("lease_token");
            e.Property(x => x.LeaseExpiresAt).HasColumnName("lease_expires_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.WorkerId).HasColumnName("worker_id");
            e.Property(x => x.MessageType).HasColumnName("message_type").IsRequired();
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("bytea").IsRequired();
            e.Property(x => x.Headers).HasColumnName("headers").HasColumnType("jsonb");
            e.Property(x => x.DedupKey).HasColumnName("dedup_key");
            e.Property(x => x.LastError).HasColumnName("last_error");

            // Partial indexes, exactly as QueueSchema.CreateScript builds them: the filters are the
            // whole point — they keep each index scoped to the rows that path actually reads.
            e.HasIndex(x => new { x.QueueId, x.Priority, x.VisibleAt, x.Id })
                .HasDatabaseName("ix_messages_ready")
                .IsDescending(false, true, false, false)
                .HasFilter("state = 0");

            e.HasIndex(x => x.LeaseExpiresAt)
                .HasDatabaseName("ix_messages_lease")
                .HasFilter("state = 1");

            e.HasIndex(x => new { x.QueueId, x.DedupKey })
                .IsUnique()
                .HasDatabaseName("ux_messages_dedup")
                .HasFilter("dedup_key IS NOT NULL");

            // Claimable rows by how long they have been claimable, priority ignored — the branch that
            // makes StarvationThreshold cheap.
            if (includeStarvationIndex)
            {
                e.HasIndex(x => new { x.QueueId, x.VisibleAt, x.Id })
                    .HasDatabaseName("ix_messages_age")
                    .HasFilter("state = 0");
            }
        });

        modelBuilder.Entity<DeadLetterEntity>(e =>
        {
            e.ToTable("dead_letters", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            e.Property(x => x.MessageId).HasColumnName("message_id");
            e.Property(x => x.QueueId).HasColumnName("queue_id");
            e.Property(x => x.MessageType).HasColumnName("message_type").IsRequired();
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("bytea").IsRequired();
            e.Property(x => x.Headers).HasColumnName("headers").HasColumnType("jsonb");
            e.Property(x => x.Priority).HasColumnName("priority").HasDefaultValue(0);
            e.Property(x => x.Attempts).HasColumnName("attempts");
            e.Property(x => x.MaxAttempts).HasColumnName("max_attempts");
            e.Property(x => x.EnqueuedAt).HasColumnName("enqueued_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.FailedAt).HasColumnName("failed_at")
                .HasColumnType("timestamp with time zone").HasDefaultValueSql("now()");
            e.Property(x => x.Error).HasColumnName("error");
            e.Property(x => x.DedupKey).HasColumnName("dedup_key");
            e.HasIndex(x => new { x.QueueId, x.FailedAt })
                .IsDescending(false, true)
                .HasDatabaseName("ix_dead_letters_queue");
        });

        modelBuilder.Entity<CompletedMessageEntity>(e =>
        {
            e.ToTable("completed", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            e.Property(x => x.MessageId).HasColumnName("message_id");
            e.Property(x => x.QueueId).HasColumnName("queue_id");
            e.Property(x => x.MessageType).HasColumnName("message_type").IsRequired();
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("bytea").IsRequired();
            e.Property(x => x.Headers).HasColumnName("headers").HasColumnType("jsonb");
            e.Property(x => x.Attempts).HasColumnName("attempts");
            e.Property(x => x.EnqueuedAt).HasColumnName("enqueued_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.CompletedAt).HasColumnName("completed_at")
                .HasColumnType("timestamp with time zone").HasDefaultValueSql("now()");
            e.Property(x => x.WorkerId).HasColumnName("worker_id");
            e.HasIndex(x => x.CompletedAt).HasDatabaseName("ix_completed_at");
        });

        return modelBuilder;
    }
}
