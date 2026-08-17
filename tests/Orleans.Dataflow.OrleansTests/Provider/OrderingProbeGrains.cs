namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// The callee half of the message-ordering probe: a grain that records the order in which calls reached it.
/// </summary>
/// <remarks>
/// <para>
/// Non-reentrant, which is what makes the recording meaningful: one turn runs at a time, so the sequence
/// this grain writes down is the order its mailbox handed the calls over rather than the order some
/// interleaving happened to produce. Each turn yields before returning so that the mailbox really does
/// accumulate — a probe whose callee answered synchronously would measure a queue that never had two
/// messages in it.
/// </para>
/// <para>
/// This exists because Orleans documents no pairwise ordering guarantee between two activations, and a
/// keyed adapter that promised per-key ordering while pipelining calls would be promising something nobody
/// undertook to provide. The question is asked of the cluster rather than of the documentation.
/// </para>
/// </remarks>
public interface IOrderingProbeCalleeGrain : IGrainWithStringKey
{
    /// <summary>Records that one sequenced call reached this grain.</summary>
    /// <param name="sequence">The number the caller sent, counting from zero.</param>
    /// <returns>The number, so a caller can pair a reply with its call.</returns>
    Task<int> ReceiveAsync(int sequence);

    /// <summary>Reports the sequence numbers in the order they arrived.</summary>
    /// <returns>The arrivals.</returns>
    Task<List<int>> ArrivalsAsync();
}

/// <summary>The callee.</summary>
internal sealed class OrderingProbeCalleeGrain : Grain, IOrderingProbeCalleeGrain
{
    private readonly List<int> _arrivals = [];

    /// <inheritdoc/>
    public async Task<int> ReceiveAsync(int sequence)
    {
        _arrivals.Add(sequence);

        // The yield is the load. Without it every call would be answered inside its own turn before the
        // next one was even dequeued, and a queue with at most one message in it cannot reorder anything.
        await Task.Yield();

        return sequence;
    }

    /// <inheritdoc/>
    public Task<List<int>> ArrivalsAsync() => Task.FromResult(new List<int>(_arrivals));
}

/// <summary>
/// The caller half of the message-ordering probe: a grain that pumps sequenced calls without awaiting them.
/// </summary>
/// <remarks>
/// Pipelining is the whole point. A caller that awaited each reply before sending the next could not
/// observe a reordering even if the transport performed one, because there would never be two messages in
/// flight between the pair. This is therefore the sharpest form of the question a keyed adapter needs
/// answered: with several calls outstanding at once between one caller and one callee, does the callee see
/// them in the order they were sent?
/// </remarks>
public interface IOrderingProbeCallerGrain : IGrainWithStringKey
{
    /// <summary>Sends sequenced calls at one callee without awaiting between them.</summary>
    /// <param name="callee">The callee grain's key.</param>
    /// <param name="count">How many calls to send.</param>
    /// <returns>The sequence numbers the replies carried, in reply order.</returns>
    Task<List<int>> PumpAsync(string callee, int count);
}

/// <summary>The caller.</summary>
internal sealed class OrderingProbeCallerGrain(IGrainFactory grains) : Grain, IOrderingProbeCallerGrain
{
    /// <inheritdoc/>
    public async Task<List<int>> PumpAsync(string callee, int count)
    {
        ArgumentNullException.ThrowIfNull(callee);

        IOrderingProbeCalleeGrain target = grains.GetGrain<IOrderingProbeCalleeGrain>(callee);
        List<Task<int>> pending = [];

        for (int sequence = 0; sequence < count; sequence++)
        {
            pending.Add(target.ReceiveAsync(sequence));
        }

        return [.. await Task.WhenAll(pending)];
    }
}
