using Orleans.Dataflow.Runtime;
using Xunit;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The ingress queue read the way a registered source reads it: asynchronously, under the two tokens the
/// runtime-factory seam hands over instead of a run context.
/// </summary>
/// <remarks>
/// <para>
/// The queue has two readers now and they have to agree about everything that is a contract: what ends the
/// sequence, what a failure does to it, what a shutdown does, and what a cancellation does. The synchronous
/// reader belongs to the local vocabulary's own queue source and reads a <see cref="LocalRunContext"/>; this
/// one belongs to a provider on the far side of the seam, which is given two tokens and nothing else. These
/// tests are what keeps the second one honest, because the runs that exercise it live in the Orleans test
/// project and could not tell a subtle difference from a correct answer.
/// </para>
/// <para>
/// Nothing here waits on a length of time. Every case either ends on its own or is ended by a token, and the
/// two waits that would otherwise hang are the ones the tokens exist to release.
/// </para>
/// </remarks>
public sealed class AsyncIngressReaderTests
{
    /// <summary>Gets the token that cancels a hung test rather than letting it block the suite.</summary>
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ACompletedQueueDeliversWhatItHeldAndThenEnds()
    {
        LocalIngressQueue queue = new(4, OverflowPolicy.Backpressure);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));

        queue.Complete();

        Assert.Equal([1, 2], await ReadAsync(queue, CancellationToken.None, CancellationToken.None));
    }

    [Fact]
    public async Task AFailedQueueRaisesInsteadOfDeliveringWhatItHeld()
    {
        LocalIngressQueue queue = new(4, OverflowPolicy.Backpressure);

        _ = await queue.OfferAsync(1, TestToken);

        queue.Fail(new InvalidOperationException("the producer gave up"));

        InvalidOperationException raised = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ReadAsync(queue, CancellationToken.None, CancellationToken.None));

        Assert.Equal("the producer gave up", raised.Message);
    }

    [Fact]
    public async Task AShutdownEndsTheSequenceAsRunningOutWould()
    {
        LocalIngressQueue queue = new(4, OverflowPolicy.Backpressure);

        using CancellationTokenSource stopping = new();

        _ = await queue.OfferAsync(1, TestToken);

        // Read one element and then stop: the reader is parked waiting for a second one that never comes,
        // and the stop token releases it as the end of the sequence rather than as a failure.
        Task<List<object?>> reading = ReadAsync(queue, CancellationToken.None, stopping.Token, () => stopping.Cancel());

        Assert.Equal([1], await reading);
    }

    [Fact]
    public async Task ACancellationAbandonsTheSequence()
    {
        LocalIngressQueue queue = new(4, OverflowPolicy.Backpressure);

        using CancellationTokenSource running = new();

        _ = await queue.OfferAsync(1, TestToken);

        Task<List<object?>> reading = ReadAsync(queue, running.Token, running.Token, () => running.Cancel());

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading);
    }

    [Fact]
    public async Task TheEndOfTheSequenceRefusesEveryLaterOffer()
    {
        LocalIngressQueue queue = new(4, OverflowPolicy.Backpressure);

        queue.Complete();

        Assert.Empty(await ReadAsync(queue, CancellationToken.None, CancellationToken.None));
        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(1, TestToken));
    }

    [Fact]
    public async Task ADroppingQueueCountsWhatItDiscardedAndDeliversTheRest()
    {
        LocalIngressQueue queue = new(1, OverflowPolicy.DropNewest);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        Assert.Equal(QueueOfferOutcome.Dropped, await queue.OfferAsync(2, TestToken));
        Assert.Equal(QueueOfferOutcome.Dropped, await queue.OfferAsync(3, TestToken));

        queue.Complete();

        Assert.Equal([1], await ReadAsync(queue, CancellationToken.None, CancellationToken.None));
        Assert.Equal(2L, queue.Dropped);
    }

    [Fact]
    public async Task EveryPullIsCountedWhetherOrNotAnElementWasThere()
    {
        LocalIngressQueue queue = new(4, OverflowPolicy.Backpressure);

        _ = await queue.OfferAsync(1, TestToken);

        queue.Complete();

        _ = await ReadAsync(queue, CancellationToken.None, CancellationToken.None);

        // Two pulls for one element: the one that took it and the one that found the queue ended. The demand
        // meter counts what the run asked for, which is what makes a credit of one readable.
        Assert.Equal(2L, queue.Pulls);
    }

    /// <summary>Reads a queue to its end, optionally doing something once the first element is in hand.</summary>
    /// <param name="queue">The queue.</param>
    /// <param name="runToken">The run's own token.</param>
    /// <param name="stopToken">The run's stop token.</param>
    /// <param name="afterFirst">What to do once one element has been read.</param>
    /// <returns>The elements read.</returns>
    private static async Task<List<object?>> ReadAsync(
        LocalIngressQueue queue,
        CancellationToken runToken,
        CancellationToken stopToken,
        Action? afterFirst = null)
    {
        List<object?> read = [];

        await foreach (object? element in queue.ElementsAsync(runToken, stopToken))
        {
            read.Add(element);

            if (read.Count == 1)
            {
                afterFirst?.Invoke();
            }
        }

        return read;
    }
}
