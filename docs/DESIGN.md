# Design: merging LiteQueue and LiteStream into Junction

## Why merge

`LiteQueue` (competing-consumer work queue, fan-in) and `LiteStream` (durable event stream, fan-out)
were two sibling libraries: same stack (.NET 10, EF Core 10, PostgreSQL), same author, same project
shape, cross-referencing each other in their own docs. Their names were already taken elsewhere, and
rather than just picking two new names, the call was made to ship them as one product — most
applications that need a work queue eventually want an event log too, and installing, configuring and
learning one coherent API beats bolting two libraries together at the application layer.

## What "unified" means here — and what it deliberately doesn't

The two engines are **not** merged at the data-model level. Queue correctness rests on
`FOR UPDATE SKIP LOCKED` + fenced lease tokens + a partial index (single delivery, competing
consumers). Stream correctness rests on an append-only log with per-consumer durable offsets
(fan-out, replayable). Modelling the queue as "a stream with one consumer group" would force a
mutable-state column onto an immutable log (breaking replay) or a second physical table anyway — the
complexity of (b) without eliminating any code. The unification happens **above** the engines:

- one schema (`junction`, shared — Queue's and Stream's table names do not collide, so no prefixing
  was needed: `queues`, `messages`, `dead_letters`, `completed` vs. `streams`, `stream_events`,
  `consumer_cursors`)
- one connector abstraction (`Junction.Connectors.IJunctionConnectionSource`, generalized from
  LiteQueue's `IQueueConnectionSource` — Queue already supported both "borrow the caller's EF Core
  connection" and "own a connection pool"; Stream did not have this at all before)
- one registration surface (`AddJunction`) and one facade (`IJunctionClient` → `.Queue` / `.Stream`)
- consistent naming: `IQueueClient`/`QueueClient`, `IStreamClient`/`StreamClient` (both dropped the
  `Lite*` prefix now that the namespace already says which module they're in)

## What was evaluated and deliberately deferred

This repository was assembled by porting both libraries' source into `Junction.Queue` and
`Junction.Stream` and writing the composition layer on top — **without a .NET SDK available to
compile or test anything in the environment that did the port**. Given that constraint, two
deeper-unification ideas from the original plan were assessed as too risky to hand-write blind and
were scaled back:

- **A shared LISTEN/NOTIFY connection.** `Junction.Queue.Internal.PostgresListenerWakeup` is a
  `BackgroundService` started eagerly when `QueueOptions.EnableNotifications` is on, signaling by
  queue name. `Junction.Stream.Internal.StreamNotificationListener` is a plain class that lazily
  starts its own listen loop on first subscribe, and additionally does advisory-lock-based
  duplicate-consumer diagnostics that Queue has no equivalent of. The two lifecycles and signaling
  models diverge enough that a blind merge risked breaking the notification path in both modules for
  a payoff (one fewer long-lived connection per process) that's real but secondary. **Left as two
  separate classes**, co-located under each module's `Internal/` folder. Revisit once the repo builds
  in CI and the merge can be done with a compiler and the existing test suites checking the result.

- **A shared worker-host base class.** `Junction.Queue.QueueWorkerHostBase<T>` runs a
  claim → bounded-channel → N-processor pipeline with lease heartbeats, dead-lettering and
  shutdown-abandon semantics. `Junction.Stream.ConsumerHostBase<T>` runs a much simpler
  poll → process → commit loop with no channel or concurrency dispatch. The shared surface is
  genuinely thin (`BackgroundService`, resolve identity from a probe instance, log start/stop, an
  error-retry-delay loop) relative to how much of each implementation is irreducibly different —
  and both are the correctness-critical files in their respective modules (delivery guarantees).
  **Left as two separate class hierarchies**, ported mechanically. Same follow-up condition as above.

- **`AddJunction<TContext>()` and the Stream connection string.** Queue's EF-context registration
  defers resolving a connection string until first use (`sp => scope.ServiceProvider
  .GetRequiredService<TContext>().Database.GetConnectionString()`). Stream's registration needs the
  actual connection string *at registration time* to build a pooled `IDbContextFactory` eagerly —
  there's no lazy-resolution path for it today. So `AddJunction<TContext>(connectionString, ...)`
  takes the connection string explicitly even though `TContext` already has one internally: Queue
  borrows `TContext`'s connection (so its completions commit inside your transaction), Stream gets
  its own pooled factory built from the string you pass. Fixing this for real means giving Stream a
  deferred-resolution registration path (EF Core's `AddPooledDbContextFactory` has an
  `Action<IServiceProvider, DbContextOptionsBuilder>` overload that would do it) — a real, moderate
  code change that wasn't made blind without a compiler to check it.

None of this was silently dropped — each is called out at the point a user would notice it (XML doc
on `AddJunction<TContext>`, this file) rather than only discovered by reading a diff.

## What still needs verification

This build has not been compiled. Before trusting it:

1. `dotnet build` — the highest-risk files for a first compile error are the ones listed above
   (they're new composition code, not mechanically-ported code) plus
   `src/Junction/ServiceCollectionExtensions.cs`'s reflection-based `CopyProperties<T>` helper.
2. `dotnet test tests/Junction.Tests` (needs Docker for `Testcontainers.PostgreSql`) — both suites
   were ported into `tests/Junction.Tests/Queue` and `tests/Junction.Tests/Stream` (subfolders, to
   avoid the filename collisions between the two original suites — both had a `ProducerTests.cs` and
   a `RegistrationTests.cs`). Each subfolder keeps its own `PostgresFixture`; their xUnit
   `[CollectionDefinition]` names were changed from the identical `"postgres"` in both source repos
   to `"postgres-queue"` / `"postgres-stream"` since a shared assembly can't register two collection
   definitions under the same name.
3. Everything else — the bulk of `src/Junction/Queue/*` and `src/Junction/Stream/*` — is a mechanical
   namespace/identifier port of two working, tested libraries. The default schema changed from
   `litequeue`/`litestream` to the shared `junction` (see `QueueOptions.Schema`,
   `QueueSchema.DefaultSchema`, `JunctionDbContext.Schema`, and the hardcoded schema-qualified SQL
   literals in the Stream module, which — unlike Queue's — were not runtime-configurable before the
   port and still aren't).

## Deferred to a later phase (not started)

- Samples (`samples/Junction.Console`) and Aspire orchestration (`aspire/Junction.AppHost` /
  `ServiceDefaults`) — both original repos had them; neither was ported here.
- Benchmarks (`benchmarks/Junction.Benchmarks`).
- Retrofitting Queue's bulk-enqueue path onto BulkForge for consistency with Stream (Queue's raw
  binary-COPY, dependency-free bulk path was a deliberate original design choice — see the git
  history of `litequeue`'s own `docs/DESIGN.md` §7 — not revisited here).
- Deduplicating the two near-identical `HeaderSerializer.cs` copies (one under
  `Junction.Queue.Internal`, one under `Junction.Stream`, both public in Stream's case).

## The old repositories

`clemkd/litequeue` and `clemkd/litestream` are not deleted. Both carry a deprecation notice pointing
here. Archiving them on GitHub is a manual step (or a follow-up with the right API access) — not done
automatically as part of this merge.
