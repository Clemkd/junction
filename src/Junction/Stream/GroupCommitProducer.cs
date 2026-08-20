using System.Threading.Channels;
using Junction.Stream.Internal;

namespace Junction.Stream;

/// <summary>
/// Wraps an inner <see cref="IEventProducer"/> with opportunistic <b>group commit</b>: single-event
/// appends are queued and a single background flusher drains everything currently available, groups
/// it by stream, and writes each stream's events in one transaction. Many small concurrent appends
/// therefore collapse into few transactions (few fsyncs) and never contend on the stream row lock,
/// since only the flusher writes. Batching is load-driven — no artificial delay is added.
/// </summary>
internal sealed class GroupCommitProducer : IEventProducer, IAsyncDisposable
{
    private readonly record struct Request(string Stream, EventData Event, TaskCompletionSource<long> Completion);

    private readonly IEventProducer _inner;
    private readonly StreamPayloadSerializer _serializer;
    private readonly int _maxBatch;
    private readonly TimeSpan _linger;
    private readonly Channel<Request> _channel;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _flushLoop;

    public GroupCommitProducer(IEventProducer inner, StreamOptions options, StreamPayloadSerializer serializer)
    {
        _inner = inner;
        _serializer = serializer;
        _maxBatch = Math.Max(1, options.GroupCommitMaxBatch);
        _linger = options.GroupCommitLinger;
        _channel = Channel.CreateUnbounded<Request>(new UnboundedChannelOptions
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
        _flushLoop = Task.Run(FlushLoopAsync);
    }

    public Task<long> AppendAsync<T>(
        T value,
        string? stream = null,
        string? key = null,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        string type = typeof(T).Name;
        byte[] payload = _serializer.Value.Serialize(value);
        return AppendAsync(stream ?? type, EventData.FromBytes(type, payload, key, headers), cancellationToken);
    }

    public async Task<long> AppendAsync(string stream, EventData evt, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(evt);

        var completion = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        await _channel.Writer.WriteAsync(new Request(stream, evt, completion), ct);
        return await completion.Task.WaitAsync(ct);
    }

    // Batch appends already amortize the transaction — pass straight through, no coalescing needed.
    public Task<AppendResult> AppendAsync(string stream, IReadOnlyList<EventData> events, CancellationToken ct = default)
        => _inner.AppendAsync(stream, events, ct);

    private async Task FlushLoopAsync()
    {
        var reader = _channel.Reader;
        var buffer = new List<Request>(_maxBatch);
        try
        {
            while (await reader.WaitToReadAsync(_shutdown.Token).ConfigureAwait(false))
            {
                buffer.Clear();
                while (buffer.Count < _maxBatch && reader.TryRead(out var request))
                    buffer.Add(request);

                // Linger: let more events accumulate so the flush coalesces a real batch instead of
                // racing ahead with one item. Flushes on whichever fires first — buffer full or the
                // linger window elapsing — so a low arrival rate never waits for a full batch.
                if (buffer.Count < _maxBatch && _linger > TimeSpan.Zero)
                    await LingerAsync(reader, buffer).ConfigureAwait(false);

                await FlushAsync(buffer).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }

    private async Task LingerAsync(ChannelReader<Request> reader, List<Request> buffer)
    {
        using var window = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
        window.CancelAfter(_linger);
        try
        {
            while (buffer.Count < _maxBatch && await reader.WaitToReadAsync(window.Token).ConfigureAwait(false))
                while (buffer.Count < _maxBatch && reader.TryRead(out var request))
                    buffer.Add(request);
        }
        catch (OperationCanceledException) when (!_shutdown.IsCancellationRequested)
        {
            // linger window elapsed — flush what we have
        }
    }

    private async Task FlushAsync(List<Request> batch)
    {
        // One transaction per distinct stream. GroupBy is stable, so events keep their arrival
        // order within a stream and receive the contiguous offsets the inner producer reserves.
        foreach (var group in batch.GroupBy(r => r.Stream, StringComparer.Ordinal))
        {
            var requests = group.ToList();
            try
            {
                var events = new List<EventData>(requests.Count);
                foreach (var r in requests)
                    events.Add(r.Event);

                var result = await _inner.AppendAsync(group.Key, events, CancellationToken.None).ConfigureAwait(false);

                long offset = result.FirstOffset;
                foreach (var r in requests)
                    r.Completion.TrySetResult(offset++);
            }
            catch (Exception ex)
            {
                foreach (var r in requests)
                    r.Completion.TrySetException(ex);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();      // no more appends; loop drains what's left, then exits
        _shutdown.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            await _flushLoop.ConfigureAwait(false);
        }
        catch
        {
            // ignore shutdown races
        }
        _shutdown.Dispose();
    }
}
