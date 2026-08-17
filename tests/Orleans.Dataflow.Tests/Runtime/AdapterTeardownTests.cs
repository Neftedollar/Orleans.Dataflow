using System.Threading.Channels;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the adapters of this checkpoint do to the one place a run has left to go wrong: the moment it
/// settles.
/// </summary>
/// <remarks>
/// <para>
/// Settling is the last thing a run does and it happens on a segment's own thread with nobody left to catch
/// anything. Two of the things it touches are not this runtime's code — an author's channel writer, and a
/// projection that arrives in a binding table — so either of them throwing has to become the run's failure
/// rather than a run that never answers. A hang is the one outcome worse than any exception, and these are
/// the tests that say it cannot happen.
/// </para>
/// <para>
/// The rest of the file is teardown proper: disposal and shutdown never throw for any of the new sources,
/// and a source that waits is released by both.
/// </para>
/// </remarks>
public sealed class AdapterTeardownTests
{
    [Fact]
    public async Task AWriterWhoseCompletionThrowsFaultsTheRunRatherThanStrandingIt()
    {
        InvalidOperationException failure = new("this writer refuses to be completed");
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2))
            .To(Sink.ToChannel(new AwkwardWriter(channel.Writer, failure)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal(2, channel.Reader.Count);
    }

    [Fact]
    public async Task AWriterWhoseCompletionThrowsNeverReplacesAFailureTheRunAlreadyHad()
    {
        InvalidOperationException stage = new("the second element cannot be mapped");
        InvalidOperationException teardown = new("this writer refuses to be completed");
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Select(value => value == 2 ? throw stage : value)
            .To(Sink.ToChannel(new AwkwardWriter(channel.Writer, teardown)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(stage, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task ACollectingProjectionThatThrowsFaultsTheRunRatherThanStrandingIt()
    {
        // Unreachable through the authoring API, whose projection is a cast the element type already
        // guarantees. It is reachable from a binding table this process did not write, which is the case
        // every defense in the run planner exists for.
        InvalidOperationException failure = new("this projection refuses to run");

        RunnableGraph graph = Graph(
            GraphDocument.Create(
                GraphId.Create("anonymous"),
                GraphRevision.Create(GraphRevision.FirstRevisionNumber),
                [CapabilityToken.Nondeployable, CapabilityToken.EphemeralIdentity],
                [Node("stage-1", "from-enumerable"), Node("stage-2", "collect", "local-collect-parameters", """{"maxElements":4}""")],
                [Edge("stage-1", "stage-2")],
                [
                    ResultSlotDefinition.Create(
                        ResultSlotId.Create("seen"),
                        ContractReference.Create(ContractId.Create("local-result"), 1),
                        PortAddress.Create(NodeId.Create("stage-2"), PortId.Create("result"))),
                ]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                (
                    "stage-2",
                    LocalStageDescriptor.Collect(
                        new CollectOptions { MaxElements = 4 },
                        (Func<object?, object?>)(_ => throw failure)))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task DisposalAndShutdownNeverThrowForAnyOfTheWaitingSources()
    {
        foreach ((string name, RunnableGraph graph) in Waiting())
        {
            RunHandle disposed = await Host.MaterializeAsync(graph, TestToken);

            await disposed.DisposeAsync();

            Assert.Equal(TaskStatus.Canceled, disposed.Completion.Status);

            await using RunHandle stopped = await Host.MaterializeAsync(graph, TestToken);

            await stopped.ShutdownAsync();
            await stopped.Completion;

            Assert.Equal(TaskStatus.RanToCompletion, stopped.Completion.Status);
            Assert.Equal(name, name);
        }
    }

    [Fact]
    public async Task ASourceThatNeverEndsIsStoppedThroughABoundaryToo()
    {
        // The stop reaches the source, the source ends its sequence, and the boundary below it drains as a
        // source running out would: shutdown is the same thing at every depth.
        RunnableGraph graph = Source.Never<int>()
            .Buffer(new BufferOptions { Capacity = 2 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, token) => Task.FromResult(value))
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.False(run.Completion.IsCompleted);

        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal(0L, await run.GetValueAsync(count, TestToken));
    }

    [Fact]
    public async Task AQueueChainWithCallbacksInFlightIsDrainedByAShutdown()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "ingress")
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                async (value, token) =>
                {
                    entered.TrySetResult();

                    await release.Task;

                    return value * 2;
                })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("ingress"), TestToken);

        _ = await queue.OfferAsync(3, TestToken);
        await entered.Task;

        ValueTask stopping = run.ShutdownAsync();

        release.SetResult();

        await stopping;
        await run.Completion;

        // Drained, not cancelled: the callback that was in flight when the stop arrived produced its
        // element and the element reached the sink.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([6], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task ACycleWhoseLaterLapProducesNothingFaultsTheRun()
    {
        // The sharp edge of a per-lap check, stated rather than hidden: the sequence is the author's, and
        // nothing obliges its second enumeration to hold what its first did. A lap that produces nothing is
        // an endless loop that emits nothing, whichever lap it is.
        OneShotSequence elements = new(1, 2);

        RunnableGraph graph = Source.Cycle(elements).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("cycle over nothing", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AGeneratorOrFactoryThatReturnsNoTaskIsReportedAsASentence()
    {
        RunnableGraph unfolding = Source
            .UnfoldAsync<int, int>(0, (state, token) => null!)
            .To(Sink.Ignore<int>());

        await using (RunHandle run = await Host.MaterializeAsync(unfolding, TestToken))
        {
            InvalidOperationException failure =
                await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

            Assert.Contains("returned no task", failure.Message, StringComparison.Ordinal);
        }

        RunnableGraph deferred = Source.FromAsyncFactory<int>(token => null!).To(Sink.Ignore<int>());

        await using (RunHandle run = await Host.MaterializeAsync(deferred, TestToken))
        {
            InvalidOperationException failure =
                await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

            Assert.Contains("returned no task", failure.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task AControlSlotOfAnotherGraphInstanceIsRefusedByThisRun()
    {
        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 2 }, "ingress").To(Sink.Ignore<int>());
        RunnableGraph twin = Source.Queue<int>(new BufferOptions { Capacity = 2 }, "ingress").To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            async () => await run.GetValueAsync(twin.Control<IIngressQueue<int>>("ingress"), TestToken));

        Assert.Equal("slot", refused.ParamName);
        Assert.Contains("another built instance", refused.Message, StringComparison.Ordinal);

        await run.DisposeAsync();
    }

    [Fact]
    public async Task AChannelReadIntoAChannelWriteCarriesEverythingAcross()
    {
        Channel<int> input = Channel.CreateUnbounded<int>();
        Channel<int> output = Channel.CreateBounded<int>(2);

        for (int value = 1; value <= 5; value++)
        {
            await input.Writer.WriteAsync(value, TestToken);
        }

        input.Writer.Complete();

        RunnableGraph graph = Source.FromChannel(input.Reader)
            .Select(value => value * 10)
            .To(Sink.ToChannel(output.Writer));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        List<int> read = [];

        await foreach (int value in output.Reader.ReadAllAsync(TestToken))
        {
            read.Add(value);
        }

        await run.Completion;

        Assert.Equal([10, 20, 30, 40, 50], read);
    }

    /// <summary>Builds one graph of every source that waits rather than ending on its own.</summary>
    /// <returns>The named graphs.</returns>
    private static IEnumerable<(string Name, RunnableGraph Graph)> Waiting()
    {
        yield return ("never", Source.Never<int>().To(Sink.Ignore<int>()));
        yield return (
            "queue",
            Source.Queue<int>(new BufferOptions { Capacity = 2 }, "ingress").To(Sink.Ignore<int>()));
        yield return ("channel", Source.FromChannel(Channel.CreateUnbounded<int>().Reader).To(Sink.Ignore<int>()));
    }

    /// <summary>A channel writer that refuses to be completed.</summary>
    /// <param name="writer">The writer every other member defers to.</param>
    /// <param name="failure">The exception completion raises.</param>
    /// <remarks>
    /// A writer is an author's own type and may be anything, including a subclass of
    /// <see cref="ChannelWriter{T}"/> whose completion throws. That makes this the reachable half of the
    /// teardown defense rather than a hypothetical one.
    /// </remarks>
    private sealed class AwkwardWriter(ChannelWriter<int> writer, Exception failure) : ChannelWriter<int>
    {
        /// <inheritdoc/>
        public override bool TryWrite(int item) => writer.TryWrite(item);

        /// <inheritdoc/>
        public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default) =>
            writer.WaitToWriteAsync(cancellationToken);

        /// <inheritdoc/>
        public override bool TryComplete(Exception? error = null) => throw failure;
    }

    /// <summary>A sequence that hands out its elements once and is empty every time after that.</summary>
    /// <param name="elements">The elements of the first enumeration.</param>
    private sealed class OneShotSequence(params int[] elements) : IEnumerable<int>
    {
        private int _enumerations;

        /// <inheritdoc/>
        public IEnumerator<int> GetEnumerator() =>
            (Interlocked.Increment(ref _enumerations) == 1 ? elements : []).AsEnumerable().GetEnumerator();

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
