using System.Collections.Concurrent;
using Orleans.Dataflow.Adapters;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// The keyed stage's credit accounting on its own, without a cluster: one call in flight per key, several
/// keys at once, and a table that empties.
/// </summary>
/// <remarks>
/// <para>
/// The cluster tests prove the stage behaves correctly end to end; these prove the accounting behaves
/// correctly when nothing else could be making it look that way. A run has a bounded engine, non-reentrant
/// executors, and grain scheduling around it, any of which could serialize a key by accident — so a defect
/// in this class would be invisible there and would surface the day one of those changed.
/// </para>
/// <para>
/// No cluster and therefore no collection: this is the one keyed test file that is pure accounting, and it
/// runs in milliseconds beside the ones that deploy silos.
/// </para>
/// </remarks>
public sealed class KeyedCallWindowTests
{
    /// <summary>Gets the token that cancels a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task OneKeysCallsRunOneAtATimeAndInSubmissionOrder()
    {
        KeyedCallWindow window = new();
        ConcurrentQueue<int> started = [];
        TaskCompletionSource[] entered = [Gate(), Gate(), Gate()];
        TaskCompletionSource[] released = [Gate(), Gate(), Gate()];
        Task<object?>[] pending =
        [
            .. Enumerable.Range(0, 3).Select(index => window.SubmitAsync("one", async () =>
            {
                started.Enqueue(index);
                entered[index].SetResult();

                await released[index].Task;

                return (object?)index;
            })),
        ];

        // Waited for rather than asserted, because a submission starts its call on the thread pool: the
        // window will not run an author's call on the thread that submitted it, which is what keeps its own
        // lock out from under whatever that call does.
        await entered[0].Task.WaitAsync(Token);

        // Only the first has begun, and the other two cannot begin until it replies — that is the whole of
        // the per-key credit. Asserting it is safe rather than a race: nothing releases the first yet, so
        // "not started" here is a state the window is holding rather than a moment that has not arrived.
        Assert.Equal([0], started);

        for (int index = 0; index < 3; index++)
        {
            released[index].SetResult();

            _ = await pending[index].WaitAsync(Token);
        }

        Assert.Equal([0, 1, 2], started);
    }

    [Fact]
    public async Task DifferentKeysRunAtTheSameTime()
    {
        KeyedCallWindow window = new();
        TaskCompletionSource release = Gate();
        TaskCompletionSource[] entered = [Gate(), Gate(), Gate()];
        Task<object?>[] pending =
        [
            .. Enumerable.Range(0, 3).Select(index => window.SubmitAsync($"key-{index}", async () =>
            {
                entered[index].SetResult();

                await release.Task;

                return (object?)index;
            })),
        ];

        // All three are inside their calls at once, which is what makes the per-key bound a per-key bound
        // rather than a bound on the stage. Awaiting them is the assertion: a window that serialized across
        // keys would never let the third report that it had entered.
        await Task.WhenAll(entered.Select(static gate => gate.Task)).WaitAsync(Token);

        release.SetResult();

        _ = await Task.WhenAll(pending).WaitAsync(Token);
    }

    [Fact]
    public async Task AKeyIsForgottenOnceItsLastCallHasSettled()
    {
        KeyedCallWindow window = new();
        TaskCompletionSource release = Gate();
        Task<object?> pending = window.SubmitAsync("one", async () =>
        {
            await release.Task;

            return null;
        });

        Assert.Equal(1, window.Tracked);

        release.SetResult();

        _ = await pending.WaitAsync(Token);

        // The table holds keys with work in flight and not keys the run has seen, which is what keeps a long
        // run over many keys from growing without bound. The release is scheduled rather than run inline, so
        // this is the one place the test waits for something rather than asserting it outright.
        while (window.Tracked > 0)
        {
            Token.ThrowIfCancellationRequested();

            await Task.Delay(5, Token);
        }
    }

    [Fact]
    public async Task AFailedCallDoesNotFailTheNextCallOfItsKey()
    {
        KeyedCallWindow window = new();
        Task<object?> failed = window.SubmitAsync(
            "one",
            static () => Task.FromException<object?>(new InvalidOperationException("the first one threw")));

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => failed);

        // The run's engine faults the run on the first failure; what this asserts is that the accounting
        // does not report that failure a second time on an element that never reached the grain. Reporting a
        // predecessor's exception here would attribute one call's fault to a different element.
        object? second = await window.SubmitAsync("one", static () => Task.FromResult<object?>(2)).WaitAsync(Token);

        Assert.Equal(2, second);
    }

    [Fact]
    public async Task ACallChainedBehindAFailureStillRuns()
    {
        KeyedCallWindow window = new();
        TaskCompletionSource release = Gate();
        Task<object?> first = window.SubmitAsync("one", async () =>
        {
            await release.Task;

            throw new InvalidOperationException("the first one threw");
        });
        Task<object?> second = window.SubmitAsync("one", static () => Task.FromResult<object?>(2));

        release.SetResult();

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => first);
        Assert.Equal(2, await second.WaitAsync(Token));
    }

    /// <summary>Creates a latch whose continuations never run on the thread that released it.</summary>
    /// <returns>The latch.</returns>
    private static TaskCompletionSource Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
