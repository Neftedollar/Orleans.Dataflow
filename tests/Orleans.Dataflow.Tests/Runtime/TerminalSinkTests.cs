using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The result-bearing sinks that need the stream to end: the two last-element sinks and the bounded
/// collecting one.
/// </summary>
/// <remarks>
/// <para>
/// A last-element sink is the mirror of a first-element sink and its opposite in lifetime, which is the one
/// claim worth proving twice: it completes no run early, so the source is pulled to its end, and it holds
/// one element rather than accumulating. The honest variant is asserted for both a reference and a value
/// element type, because the default it resolves is a value the authoring surface computed and handed over,
/// and a runtime that had lost it would fail on a value type and pass on a reference one.
/// </para>
/// <para>
/// Collecting is the only sink whose state is mutable, and that is what its per-run test is about: a seed
/// two runs shared would make the second run's result the first run's elements as well.
/// </para>
/// </remarks>
public sealed class TerminalSinkTests
{
    [Fact]
    public async Task LastExposesTheLastElementAndPullsTheSourceToItsEnd()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements).To(s => s.Last(), "last", out ResultSlot<int> last);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(3, await run.GetValueAsync(last, TestToken));
        Assert.Equal(3, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task LastOnAnEmptyStreamFaultsAndTheMessageNamesTheLastElement()
    {
        RunnableGraph graph = Source.Empty<int>().To(s => s.Last(), "last", out ResultSlot<int> last);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("Sequence contains no elements", failure.Message, StringComparison.Ordinal);
        Assert.Contains("its last element", failure.Message, StringComparison.Ordinal);
        Assert.Contains("last-or-default", failure.Message, StringComparison.Ordinal);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(last, TestToken)));
    }

    [Fact]
    public async Task FirstOnAnEmptyStreamStillNamesTheFirstElement()
    {
        // The counterpart of the test above, and the reason the wording is a property of the terminal
        // rather than a constant: one sentence for two sinks would have to be wrong about one of them.
        RunnableGraph graph = Source.Empty<int>().To(s => s.First(), "first", out ResultSlot<int> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("its first element", failure.Message, StringComparison.Ordinal);
        Assert.Contains("first-or-default", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LastOrDefaultResolvesTheElementTypesDefaultOnAnEmptyStream()
    {
        RunnableGraph values = Source.Empty<int>()
            .To(s => s.LastOrDefault(), "last", out ResultSlot<int> number);

        await using (RunHandle run = await Host.MaterializeAsync(values, TestToken))
        {
            await run.Completion;

            Assert.Equal(0, await run.GetValueAsync(number, TestToken));
        }

        RunnableGraph references = Source.Empty<string>()
            .To(s => s.LastOrDefault(), "last", out ResultSlot<string?> text);

        await using (RunHandle run = await Host.MaterializeAsync(references, TestToken))
        {
            await run.Completion;

            Assert.Null(await run.GetValueAsync(text, TestToken));
        }
    }

    [Fact]
    public async Task LastOrDefaultResolvesTheLastElementWhenThereWasOne()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<string>("a", "b"))
            .To(s => s.LastOrDefault(), "last", out ResultSlot<string?> last);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal("b", await run.GetValueAsync(last, TestToken));
    }

    [Fact]
    public async Task AShutdownResolvesTheLastElementSeenSoFar()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        Gate gate = new();

        RunnableGraph graph = Source.From(elements)
            .Select(value =>
            {
                if (value == 2)
                {
                    gate.Wait();
                }

                return value;
            })
            .To(s => s.LastOrDefault(), "last", out ResultSlot<int> last);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        ValueTask stopping = run.ShutdownAsync();

        gate.Open();

        await stopping;
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(2, await run.GetValueAsync(last, TestToken));
    }

    [Fact]
    public async Task CollectExposesTheElementsInOrder()
    {
        RunnableGraph graph = Source.Range(1, 4)
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 4], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task CollectSucceedsAtExactlyItsBoundAndFaultsOnTheElementAfterIt()
    {
        RunnableGraph exact = Source.Range(1, 3)
            .To(s => s.Collect(new CollectOptions { MaxElements = 3 }), "seen", out ResultSlot<IReadOnlyList<int>> filled);

        await using (RunHandle run = await Host.MaterializeAsync(exact, TestToken))
        {
            await run.Completion;

            Assert.Equal([1, 2, 3], await run.GetValueAsync(filled, TestToken));
        }

        RecordingEnumerable<int> elements = new(1, 2, 3, 4);

        RunnableGraph overflowing = Source.From(elements)
            .To(s => s.Collect(new CollectOptions { MaxElements = 3 }), "seen", out ResultSlot<IReadOnlyList<int>> overflow);

        await using (RunHandle run = await Host.MaterializeAsync(overflowing, TestToken))
        {
            CollectOverflowException failure =
                await Assert.ThrowsAsync<CollectOverflowException>(() => run.Completion);

            Assert.Contains("bounded at 3 elements", failure.Message, StringComparison.Ordinal);
            Assert.Same(failure, await Assert.ThrowsAsync<CollectOverflowException>(() => run.GetValueAsync(overflow, TestToken)));
            Assert.Equal(1, elements.Releases);
        }
    }

    [Fact]
    public async Task CollectStartsFromAnEmptyListInEveryRun()
    {
        // The one sink whose state is mutable, and therefore the one whose seed cannot be a value the plan
        // holds: two runs sharing a list would make the second run's result contain the first run's
        // elements as well.
        RunnableGraph graph = Source.Range(1, 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;

            Assert.Equal([1, 2], await first.GetValueAsync(seen, TestToken));
        }

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            await second.Completion;

            Assert.Equal([1, 2], await second.GetValueAsync(seen, TestToken));
        }
    }

    [Fact]
    public async Task CollectOnAnEmptyStreamResolvesAnEmptyList()
    {
        RunnableGraph graph = Source.Empty<string>()
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "seen", out ResultSlot<IReadOnlyList<string>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Empty(await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task AShutdownResolvesTheElementsCollectedSoFar()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        Gate gate = new();

        RunnableGraph graph = Source.From(elements)
            .Select(value =>
            {
                if (value == 2)
                {
                    gate.Wait();
                }

                return value;
            })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        ValueTask stopping = run.ShutdownAsync();

        gate.Open();

        await stopping;
        await run.Completion;

        Assert.Equal([1, 2], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task ACollectedResultIsASnapshotRatherThanTheListTheRunAppendedTo()
    {
        RunnableGraph graph = Source.Range(1, 3)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await first.Completion;

        IReadOnlyList<int> result = await first.GetValueAsync(seen, TestToken);

        // Not the accumulator: nothing an author can do to the result can reach a run, and asking twice
        // hands back the same settled value.
        Assert.IsNotType<List<int>>(result);
        Assert.Same(result, await first.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public void CollectRejectsABoundBelowOneAgainstItsOwnArgument()
    {
        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => Sink.Collect<int>(new CollectOptions { MaxElements = 0 }));

        Assert.Equal("options", failure.ParamName);
        Assert.Contains("MaxElements", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectRejectsAbsentOptions() =>
        Assert.Throws<ArgumentNullException>(() => Sink.Collect<int>(null!));

    [Fact]
    public void ACollectingSinkWritesItsBoundIntoTheDocumentUnderAContractOfItsOwn()
    {
        RunnableGraph graph = Source.Range(1, 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 7 }), "seen", out ResultSlot<IReadOnlyList<int>> _);

        StageNode collect = graph.Document.Nodes.Single(
            node => node.Stage.Stage == StageId.Create("collect"));

        Assert.Equal("local-collect-parameters", collect.ParameterContract.Contract.Value);
        Assert.Equal("""{"maxElements":7}""", collect.Parameters.ToString());
    }

    [Fact]
    public void TwoCollectingGraphsThatDifferOnlyInTheirBoundHaveDifferentFingerprints()
    {
        RunnableGraph small = Source.Range(1, 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "seen", out ResultSlot<IReadOnlyList<int>> _);
        RunnableGraph large = Source.Range(1, 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 5 }), "seen", out ResultSlot<IReadOnlyList<int>> _);
        RunnableGraph same = Source.Range(1, 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "seen", out ResultSlot<IReadOnlyList<int>> _);

        Assert.NotEqual(small.Fingerprint, large.Fingerprint);
        Assert.Equal(small.Fingerprint, same.Fingerprint);
    }
}
