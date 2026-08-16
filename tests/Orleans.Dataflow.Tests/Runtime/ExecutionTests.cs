using System.Globalization;
using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a run does with elements: the chain it applies, the bound it keeps, and the state it does not
/// share.
/// </summary>
/// <remarks>
/// The bound is the claim worth the most here. One element in flight is the strongest a stream can be, and
/// it is only worth asserting against a sink that is actually held: a runtime that read ahead would look
/// identical in its results and differ only in what the source saw.
/// </remarks>
public sealed class ExecutionTests
{
    [Fact]
    public async Task AChainOfSelectAndWhereResolvesItsAggregate()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Source.From(elements)
            .Where(value => value % 2 == 1)
            .Select(value => value * 10)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(90L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(5, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AFilterThatDropsAnElementStopsItReachingTheStagesBelow()
    {
        List<int> mapped = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4))
            .Where(value => value > 2)
            .Select(value =>
            {
                mapped.Add(value);

                return value;
            })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([3, 4], mapped);
        Assert.Equal(7L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task IgnoreDrainsEveryElementAndCompletes()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        List<int> drained = [];

        RunnableGraph graph = Source.From(elements)
            .Select(value =>
            {
                drained.Add(value);

                return value;
            })
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], drained);
        Assert.Empty(graph.ResultSlots);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AFoldWhoseResultWasDiscardedStillFoldsEveryElement()
    {
        // Converting a result-bearing sink drops the declaration, not the fold: the graph still folds and
        // simply exposes nothing to ask for.
        List<int> folded = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(Sink.Aggregate<int, long>(
                0L,
                (sum, value) =>
                {
                    folded.Add(value);

                    return sum + value;
                }).ToSink());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], folded);
        Assert.Empty(graph.ResultSlots);
    }

    [Fact]
    public async Task TheRunHoldsExactlyOneElementInFlight()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        Gate gate = new();

        RunnableGraph graph = Source.From(elements)
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        // The run is inside the fold holding the first element, and the source has not been asked for a
        // second one. A runtime that buffered would already have read ahead.
        Assert.Equal(1, elements.Pulls);
        Assert.Equal(1, elements.PeakInFlight);

        gate.Open();
        await run.Completion;

        Assert.Equal(10L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(4, elements.Pulls);
        Assert.Equal(1, elements.PeakInFlight);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphKeepIndependentState()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;

            Assert.Equal(6L, await first.GetValueAsync(total, TestToken));
        }

        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);
        await second.Completion;

        // Six and not twelve: the second run started from the seed the author wrote, not from where the
        // first one left off.
        Assert.Equal(6L, await second.GetValueAsync(total, TestToken));
        Assert.Equal(2, elements.Enumerations);
        Assert.Equal(6, elements.Pulls);
        Assert.Equal(2, elements.Releases);
    }

    [Fact]
    public async Task TwoRunsStillShareWhateverTheAuthorsDelegatesCaptured()
    {
        // The other side of the independence claim, pinned so that it is not overstated later. A run
        // isolates the state it owns — its enumerator, its fold state — and cannot isolate state the
        // author put outside the graph.
        int captured = 0;

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Select(value =>
            {
                captured++;

                return value;
            })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;

            Assert.Equal(6L, await first.GetValueAsync(total, TestToken));
        }

        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);
        await second.Completion;

        // The fold started from the seed again; the captured counter did not.
        Assert.Equal(6L, await second.GetValueAsync(total, TestToken));
        Assert.Equal(6, captured);
    }

    [Fact]
    public async Task TwoConcurrentRunsOfOneGraphBothCompleteWithTheirOwnResult()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(first.Completion, second.Completion);

        Assert.Equal(6L, await first.GetValueAsync(total, TestToken));
        Assert.Equal(6L, await second.GetValueAsync(total, TestToken));
        Assert.Equal(2, elements.Enumerations);
        Assert.Equal(2, elements.Releases);
    }

    [Fact]
    public async Task AResultIsSettledBeforeCompletionIs()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Asking after completion never waits on the run again: the slot was settled before the completion
        // task was, so the returned task is already finished when it is handed back.
        Task<long> resolved = run.GetValueAsync(total, TestToken);

        Assert.True(resolved.IsCompletedSuccessfully);
        Assert.Equal(6L, await resolved);
    }

    [Fact]
    public async Task AnEmptySourceCompletesWithTheSeed()
    {
        RecordingEnumerable<int> elements = new();
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(0L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0, elements.Pulls);
        Assert.Equal(1, elements.Enumerations);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AChainLongerThanNineStagesRunsInAuthoringOrder()
    {
        // Zero-padded numbering makes a document's node order the authoring order past nine occurrences,
        // where 'stage-10' would otherwise have sorted before 'stage-2'. The letters would come out
        // scrambled if either half of that had gone wrong, so this is the end-to-end statement of it.
        RunnableGraph graph = Source.From(new RecordingEnumerable<string>(string.Empty))
            .Select(text => text + "a")
            .Select(text => text + "b")
            .Select(text => text + "c")
            .Select(text => text + "d")
            .Select(text => text + "e")
            .Select(text => text + "f")
            .Select(text => text + "g")
            .Select(text => text + "h")
            .Select(text => text + "i")
            .To(s => s.Aggregate(string.Empty, (all, text) => all + text), "spelled", out ResultSlot<string> spelled);

        Assert.Equal(11, graph.Document.Nodes.Count);
        Assert.Equal("stage-0002", graph.Document.Nodes[1].Id.Value);
        Assert.Equal("stage-0010", graph.Document.Nodes[9].Id.Value);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal("abcdefghi", await run.GetValueAsync(spelled, TestToken));
    }

    [Fact]
    public async Task ARunFollowsTheEdgesRatherThanTheNodeOrder()
    {
        // The authoring API's numbering now makes node order and flow order agree, so the claim that a run
        // follows the edges needs a document where the two disagree — which only a hand-built document can
        // be. Ordinally the nodes are 'a', 'b', 'c'; the edges make the flow 'c' to 'b' to 'a', and a
        // runtime reading the node list would apply the mappings in the opposite order.
        List<string> applied = [];

        RunnableGraph graph = Graph(
            Document(
                [Node("c", "from-enumerable"), Node("b", "select"), Node("a", "ignore")],
                [Edge("c", "b"), Edge("b", "a")]),
            Bindings(
                ("c", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<string>("x"))),
                ("b", LocalStageDescriptor.Select((Func<string, string>)(text =>
                {
                    applied.Add(text);

                    return text;
                }))),
                ("a", LocalStageDescriptor.Ignore())));

        Assert.Equal(["a", "b", "c"], graph.Document.Nodes.Select(node => node.Id.Value));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(["x"], applied);
    }

    [Fact]
    public async Task AFoldWhoseSeedIsNullStartsFromNull()
    {
        // A null seed is a state, not the absence of one: what decides whether a graph folds is that it
        // has a folder, never what its seed happens to be.
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(
                s => s.Aggregate(
                    (string?)null,
                    (text, value) => (text ?? string.Empty) + value.ToString(CultureInfo.InvariantCulture)),
                "text",
                out ResultSlot<string?> text);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal("123", await run.GetValueAsync(text, TestToken));
    }

    [Fact]
    public async Task AnEmptySourceUnderANullSeedResolvesToNull()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>())
            .To(
                s => s.Aggregate(
                    (string?)null,
                    (text, value) => (text ?? string.Empty) + value.ToString(CultureInfo.InvariantCulture)),
                "text",
                out ResultSlot<string?> text);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Null(await run.GetValueAsync(text, TestToken));
    }

    [Fact]
    public async Task ASourceOfReferenceElementsCarriesNullsThrough()
    {
        // The runtime moves elements as objects, so a null element is the case where boxing and unboxing
        // could quietly go wrong.
        RunnableGraph graph = Source.From(new RecordingEnumerable<string?>("a", null, "c"))
            .Select(value => value ?? "-")
            .To(s => s.Aggregate(string.Empty, (text, value) => text + value), "text", out ResultSlot<string> text);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal("a-c", await run.GetValueAsync(text, TestToken));
    }
}
