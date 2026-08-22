# Benchmarks

## Scale tests (10M+ rows)

`tests/Junction.Tests/Queue/ScaleTests.cs` and `tests/Junction.Tests/Stream/ScaleTests.cs` are
**correctness** tests at a size `VolumeTests` deliberately stays below — a backlog/stream past ten
million rows. Each one writes the full volume in large batches, checks the `EXPLAIN` plan of the
claim/poll query still uses its index (never a scan of ten million rows), then drains it back out and
checks nothing was lost, duplicated, or reordered. They log throughput (`msg/s` / `ev/s`) as they go,
so a run also doubles as a rough load number for your own hardware — but the assertions are all
correctness, not a wall-clock threshold, for the same reason as `VolumeTests`.

They are tagged `[Trait("Category", "Scale")]` and excluded from CI's default run
(`--filter "Category!=Scale"` in `ci.yml`) because writing and draining 10M+ rows takes minutes, not
seconds. Run them explicitly:

```bash
dotnet test tests/Junction.Tests --filter Category=Scale
```

Override the size with an environment variable (e.g. to go past 10M, or to shrink it for a quick local
smoke run):

```bash
JUNCTION_QUEUE_SCALE_MESSAGES=50000000 dotnet test tests/Junction.Tests --filter "FullyQualifiedName~Queue.ScaleTests"
JUNCTION_STREAM_SCALE_EVENTS=50000000 dotnet test tests/Junction.Tests --filter "FullyQualifiedName~Stream.ScaleTests"
```

`benchmarks/Junction.Benchmarks` is a [BenchmarkDotNet](https://benchmarkdotnet.org) harness (warmup,
multiple iterations, `MemoryDiagnoser`) covering both modules. It starts a throwaway PostgreSQL 18
container automatically, or targets an existing database via `JUNCTION_BENCH_CONNECTION`:

```bash
dotnet run -c Release --project benchmarks/Junction.Benchmarks                        # everything
dotnet run -c Release --project benchmarks/Junction.Benchmarks -- --filter *Claim*    # a subset
JUNCTION_BENCH_CONNECTION="Host=...;..." dotnet run -c Release --project benchmarks/Junction.Benchmarks
```

Database benchmarks are I/O-bound, so timings are noisier than CPU micro-benchmarks and depend
entirely on your Postgres, disk and host — treat every number below as a relative signal, not a
guarantee, and reproduce on your own hardware before sizing anything against it.

> These numbers have not been re-run against the current codebase in this environment. Run the suites
> above to get numbers for your own hardware.

## Queue

Three suites, in `benchmarks/Junction.Benchmarks/Queue/`:

- **`EnqueueBenchmarks`** — one batched `unnest` insert vs. one statement per message, at batch sizes
  1/10/100/1000.
- **`ClaimBenchmarks`** — claim/acknowledge cost at several batch sizes **and two backlog sizes**
  (1,000 vs 50,000 messages). The numbers should barely move between them: the ready index is partial
  and completed rows leave the table, so claim cost shouldn't scale with backlog size.
- **`ContentionBenchmarks`** — 1 → 16 workers draining one queue: does throughput scale with workers?

### Reference numbers

Laptop, PostgreSQL 18 in Docker, 20,000 messages × 256 B, claims of 20:

| Workers | Enqueue (msg/s) | Claim + ack, per message (msg/s) | Claim + ack, batched (msg/s) | Gain from batching |
|---:|---:|---:|---:|---:|
| 1 | 22,850 | 577 | 4,696 | 8.1× |
| 2 | 23,646 | 879 | 8,597 | 9.8× |
| 4 | 23,024 | 1,738 | 12,021 | 6.9× |
| 8 | 24,001 | 2,936 | 18,317 | 6.2× |
| 16 | 24,015 | 4,019 | **23,297** | 5.8× |

Two things to read out of it. **Consumption scales with workers** — 5.0× from 1 to 16 on a single
queue, `FOR UPDATE SKIP LOCKED` earning its keep; enqueue is already saturated at one producer because
a batch enqueue is one statement. And **completing in batches matters as much as claiming in
batches**: acknowledging one message per statement costs one WAL flush per message, capping a single
worker at a few hundred messages a second regardless of the claim path. `AcknowledgeBatchAsync` removes
the whole batch in one statement — 6–10× here — and the hosted worker does it for you on batch
handlers.

## Stream

Three suites, in `benchmarks/Junction.Benchmarks/Stream/`:

- **`AppendBenchmarks`** — append throughput across batch sizes, including the allocation profile
  (`MemoryDiagnoser`) of the row-by-row path vs. the bulk-insert path.
- **`GroupCommitBenchmarks`** — many concurrent single-event appends, group commit on vs. off.
- **`PollBenchmarks`** — read-path cost over a pre-seeded stream at two batch sizes.

### Reference numbers

Same environment as above. Write/read throughput, 50,000 events × 256 B, batches of 500:

| | events/sec | throughput |
|---|---:|---:|
| Write | 37,973 | 9.27 MB/s |
| Read | 88,514 | 21.6 MB/s |

**Bulk append.** Batches at or above `StreamOptions.BulkInsertThreshold` (default 100) switch to the
bulk-insert path — measured **~2.5–3.3× faster** writes (e.g. batch 2000 × 256 B: 11k → 37k events/s)
in the same offset-allocation transaction, so ordering and the no-loss guarantee are unchanged. Smaller
batches use the row-by-row path, which has lower fixed cost.

**Group commit.** For many small, concurrent single-event appends, `StreamOptions.EnableGroupCommit`
coalesces them into grouped transactions:

| Concurrent producers (256 B, 1 event/call) | Direct | Group commit |
|---:|---:|---:|
| 8 | 306 ev/s | 502 ev/s |
| 32 | ~300 ev/s | 1,984 ev/s |
| 128 | ~300 ev/s | 6,497 ev/s |
| 256 | ~300 ev/s | **10,603 ev/s (~35×)** |

**Push delivery.** End-to-end latency, append to handler invocation, 20 events, 500 ms poll interval:

| | median | p95 |
|---|---:|---:|
| Polling | 513 ms | 517 ms |
| Push delivery (`EnablePushDelivery`, on by default) | **2.6 ms** | **5.9 ms** |
