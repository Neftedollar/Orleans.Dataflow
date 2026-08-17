using System.Threading.Channels;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The two channel adapters: a run that drains a reader the author owns, and a run that fills a writer the
/// author owns.
/// </summary>
/// <remarks>
/// <para>
/// Both are bridges to state a run did not create and does not own, and both are asserted for exactly that.
/// A reader is not reset per run, so two runs of one graph compete for its elements — tested rather than
/// merely documented, because a reader that looked re-enumerable would be the one honest mistake this
/// adapter invites. A writer is completed by the run because a consumer on the other side has to learn that
/// the stream ended, and with the run's failure when it had one.
/// </para>
/// <para>
/// Write acceptance is not consumption, and the last test says so with a run that finishes while its
/// elements are still sitting unread in the channel.
/// </para>
/// </remarks>
public sealed class ChannelAdapterTests
{
    [Fact]
    public async Task AChannelSourceDrainsTheReaderAndCompletesWhenTheChannelDoes()
    {
        Channel<int> channel = Channel.CreateUnbounded<int>();

        await channel.Writer.WriteAsync(1, TestToken);
        await channel.Writer.WriteAsync(2, TestToken);
        channel.Writer.Complete();

        RunnableGraph graph = Source.FromChannel(channel.Reader)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task AChannelCompletedWithAFailureFaultsTheRunWithThatExceptionUnwrapped()
    {
        InvalidOperationException failure = new("the producer of this channel failed");
        Channel<int> channel = Channel.CreateUnbounded<int>();

        await channel.Writer.WriteAsync(1, TestToken);
        channel.Writer.Complete(failure);

        RunnableGraph graph = Source.FromChannel(channel.Reader)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(count, TestToken)));
    }

    [Fact]
    public async Task AChannelSourceWaitingForElementsIsCompletedByAShutdownAndCancelledByACancellation()
    {
        foreach (bool graceful in new[] { true, false })
        {
            Channel<int> channel = Channel.CreateUnbounded<int>();

            await channel.Writer.WriteAsync(3, TestToken);

            using CancellationTokenSource cancellation = new();

            TaskCompletionSource delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);

            RunnableGraph graph = Source.FromChannel(channel.Reader)
                .Select(value =>
                {
                    delivered.TrySetResult();

                    return value;
                })
                .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

            await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

            // The element the channel already held has to be past the run's first pull before the run is
            // stopped: shutdown means "stop pulling", so a shutdown that arrived first would legitimately
            // leave the element unread and this test would be asserting a race rather than a rule.
            await delivered.Task;

            if (graceful)
            {
                await run.ShutdownAsync();
                await run.Completion;

                Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
                Assert.Equal(3L, await run.GetValueAsync(total, TestToken));
            }
            else
            {
                await cancellation.CancelAsync();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
            }
        }
    }

    [Fact]
    public async Task TwoRunsOfOneChannelSourceCompeteForItsElements()
    {
        // The honest consequence of a source that is not fresh per run. A reader is external state the
        // author owns and is not re-enumerable, so the two runs split the elements between them: the union
        // is exactly the input and nothing is duplicated, but which run saw which element is not defined
        // and this test deliberately asserts nothing about it.
        Channel<int> channel = Channel.CreateUnbounded<int>();

        for (int value = 0; value < 16; value++)
        {
            await channel.Writer.WriteAsync(value, TestToken);
        }

        channel.Writer.Complete();

        RunnableGraph graph = Source.FromChannel(channel.Reader)
            .To(s => s.Collect(new CollectOptions { MaxElements = 32 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        await first.Completion;
        await second.Completion;

        IReadOnlyList<int> left = await first.GetValueAsync(seen, TestToken);
        IReadOnlyList<int> right = await second.GetValueAsync(seen, TestToken);

        Assert.Equal(Enumerable.Range(0, 16), left.Concat(right).Order());
    }

    [Fact]
    public async Task AChannelSinkWritesEveryElementAndCompletesTheWriterWhenTheRunSucceeds()
    {
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], await Drain(channel.Reader));
        Assert.True(channel.Reader.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task AChannelSinkCompletesTheWriterWithTheRunsFailure()
    {
        InvalidOperationException failure = new("the second element cannot be mapped");
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Select(value => value == 2 ? throw failure : value)
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));

        // Read through the channel rather than waiting on its completion task: a channel completed with an
        // error still hands its consumer the elements that were accepted before it, and only then raises.
        // That is the failure reaching the other side in the right order, and the run's own exception is
        // the very instance it raises with.
        List<int> read = [];

        Assert.Same(
            failure,
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (int value in channel.Reader.ReadAllAsync(TestToken))
                {
                    read.Add(value);
                }
            }));

        Assert.Equal([1], read);
    }

    [Fact]
    public async Task AChannelSinkCompletesTheWriterWithACancellationWhenTheRunIsCancelled()
    {
        Channel<int> channel = Channel.CreateUnbounded<int>();
        Gate gate = new();

        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Select(value =>
            {
                gate.Wait();

                return value;
            })
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;
        await cancellation.CancelAsync();
        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await channel.Reader.Completion);
    }

    [Fact]
    public async Task AnEarlyCompletionUpstreamOfAChannelSinkStillCompletesTheWriterWithoutAFailure()
    {
        // A take that reaches its bound ends the run successfully, and a consumer reading the other side of
        // the channel has to see exactly that: the elements the take passed, and then a clean end.
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .Take(2)
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2], await Drain(channel.Reader));
        Assert.True(channel.Reader.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task AFirstElementSinkIsNotTheOnlyWayToEndEarlyAndAChannelSinkStillCloses()
    {
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .TakeWhile(value => value < 2)
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1], await Drain(channel.Reader));
        Assert.True(channel.Reader.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task TheWritersOwnBoundIsTheSinksBackpressure()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);
        Channel<int> channel = Channel.CreateBounded<int>(1);

        RunnableGraph graph = Source.From(elements).To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // One element in the channel and one held in the write that has nowhere to go: the run cannot get
        // further ahead than the channel the author declared, which is the whole of the sink's policy.
        Assert.Equal(1, await channel.Reader.ReadAsync(TestToken));
        Assert.False(run.Completion.IsCompleted);

        List<int> read = [1];

        while (read.Count < 5)
        {
            read.Add(await channel.Reader.ReadAsync(TestToken));
        }

        await run.Completion;

        Assert.Equal([1, 2, 3, 4, 5], read);
        Assert.Equal(5, elements.Pulls);
    }

    [Fact]
    public async Task WriteAcceptanceIsNotConsumption()
    {
        // The distinction every bounded egress adapter has to state. The run has ended successfully and
        // nothing has read the channel: the elements are accepted, and whether anything ever processes them
        // is on the other side of a boundary this run does not own.
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(3, channel.Reader.Count);
    }

    [Fact]
    public async Task AWriterCompletedByTheAuthorMidRunFaultsTheRun()
    {
        Channel<int> channel = Channel.CreateUnbounded<int>();
        Gate gate = new();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Select(value =>
            {
                if (value == 2)
                {
                    gate.Wait();
                }

                return value;
            })
            .To(Sink.ToChannel(channel.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        channel.Writer.Complete();
        gate.Open();

        await Assert.ThrowsAsync<ChannelClosedException>(() => run.Completion);
    }

    [Fact]
    public async Task AReaderThatWrapsItsFailureStillFaultsTheRunWithTheFailureItself()
    {
        // A reader is an author's own type and may be anything, including one that reports its end the way
        // some paths of the framework's own channels do — wrapped in a ChannelClosedException. The run
        // faults with what the author actually failed with, because a runtime type between an author's
        // exception and the run that reports it is exactly what this codebase does not do.
        InvalidOperationException failure = new("the producer of this channel failed");

        RunnableGraph graph = Source.FromChannel(new WrappingReader(failure))
            .To(s => s.Count(), "count", out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    /// <summary>Reads everything a completed channel holds.</summary>
    /// <param name="reader">The reader to drain.</param>
    /// <returns>The elements, in the order they were written.</returns>
    private static async Task<List<int>> Drain(ChannelReader<int> reader)
    {
        List<int> read = [];

        await foreach (int value in reader.ReadAllAsync(TestToken))
        {
            read.Add(value);
        }

        return read;
    }

    /// <summary>A reader that reports its failure wrapped, the way some channel paths do.</summary>
    /// <param name="failure">The failure to wrap.</param>
    private sealed class WrappingReader(Exception failure) : ChannelReader<int>
    {
        /// <inheritdoc/>
        public override bool TryRead(out int item)
        {
            item = 0;

            return false;
        }

        /// <inheritdoc/>
        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) =>
            throw new ChannelClosedException("the channel is closed", failure);
    }
}
