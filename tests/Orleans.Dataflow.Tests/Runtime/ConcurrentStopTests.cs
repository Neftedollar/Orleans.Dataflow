using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What holds however the three ways of stopping a run interleave.
/// </summary>
/// <remarks>
/// <para>
/// Every other runtime test pins one interleaving down with a gate and then asserts a particular outcome.
/// These two do the opposite: they let shutdown, cancellation, and disposal race for real, many times, and
/// assert only what has to be true under every interleaving. Which of the three wins is not one of those
/// things, and is deliberately not asserted.
/// </para>
/// <para>
/// The invariants are re-derived from the run rather than recorded from it: a run reaches exactly one
/// terminal state and reaches it once, it never fails when nothing failed, its results are settled by the
/// time it is, and it releases exactly as many enumerators as it obtained. A test that asserted a
/// particular result instead could only ever catch a change, never an incompleteness.
/// </para>
/// </remarks>
public sealed class ConcurrentStopTests
{
    /// <summary>The number of races each test runs.</summary>
    /// <remarks>
    /// Enough that a window of a few instructions is hit repeatedly on a warm machine, and few enough that
    /// the pair costs a fraction of a second. The bug this guards against was found at a rate of one in
    /// five, so a few hundred is generous.
    /// </remarks>
    private const int Races = 250;

    [Fact]
    public async Task StoppingEveryWayAtOnceLeavesExactlyOneTerminalState()
    {
        for (int race = 0; race < Races; race++)
        {
            using CancellationTokenSource cancellation = new();
            RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);
            RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

            RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

            Task shutdown = Task.Run(async () => await run.ShutdownAsync(), TestToken);
            Task cancelling = Task.Run(cancellation.Cancel, TestToken);
            Task disposal = Task.Run(async () => await run.DisposeAsync(), TestToken);

            await Task.WhenAll(shutdown, cancelling, disposal);

            // Whoever won, the run has one answer, it is not a failure, its result is settled with it, and
            // it let go of exactly what it took hold of.
            Assert.True(run.Completion.IsCompleted);
            Assert.NotEqual(TaskStatus.Faulted, run.Completion.Status);
            Assert.True(run.GetValueAsync(total, TestToken).IsCompleted);
            Assert.Equal(elements.Enumerations, elements.Releases);
        }
    }

    [Fact]
    public async Task StoppingAMultiSegmentRunEveryWayAtOnceLeavesExactlyOneTerminalState()
    {
        // The same race across a graph that has boundaries in it, where the ways to get it wrong are
        // different in kind: a segment that settled the run before another had released what it held, a
        // drain that raced a cancellation into two answers, or a channel whose writer was completed twice.
        // Which of the three requests wins is still not asserted; that every segment stopped and the run
        // has one answer is.
        for (int race = 0; race < Races; race++)
        {
            using CancellationTokenSource cancellation = new();
            RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8);

            RunnableGraph graph = Source.From(elements)
                .Buffer(new BufferOptions { Capacity = 2 })
                .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => Task.FromResult((long)value))
                .Buffer(new BufferOptions { Capacity = 2, OverflowPolicy = OverflowPolicy.DropOldest })
                .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

            RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

            Task shutdown = Task.Run(async () => await run.ShutdownAsync(), TestToken);
            Task cancelling = Task.Run(cancellation.Cancel, TestToken);
            Task disposal = Task.Run(async () => await run.DisposeAsync(), TestToken);

            await Task.WhenAll(shutdown, cancelling, disposal);

            Assert.True(run.Completion.IsCompleted);
            Assert.NotEqual(TaskStatus.Faulted, run.Completion.Status);
            Assert.True(run.GetValueAsync(total, TestToken).IsCompleted);
            Assert.Equal(elements.Enumerations, elements.Releases);

            // Whatever a drop policy did or did not have time to do, it counted it.
            Assert.InRange(run.DroppedElements, 0L, 8L);
        }
    }

    [Fact]
    public async Task CancellingWhileARunIsAlreadyStoppingIsNeverAnError()
    {
        // The run releases its link to the caller's token when it ends, and the caller may cancel that
        // token at the same moment. Neither side may throw for the other's timing.
        for (int race = 0; race < Races; race++)
        {
            using CancellationTokenSource cancellation = new();
            RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

            RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

            Task cancelling = Task.Run(cancellation.Cancel, TestToken);

            await run.DisposeAsync();
            await cancelling;

            Assert.True(run.Completion.IsCompleted);
            Assert.True(run.GetValueAsync(total, TestToken).IsCompleted);
        }
    }
}
