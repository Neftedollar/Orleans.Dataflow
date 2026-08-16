using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What materializing does, and what it refuses to materialize.
/// </summary>
/// <remarks>
/// Every refusal below is unreachable through the authoring API. They are the host's defenses against a
/// document built somewhere else, and the only way to reach them is to build that document by hand, which
/// is what these tests do. Testing them is how they stay defenses rather than dead branches.
/// </remarks>
public sealed class MaterializationTests
{
    [Fact]
    public async Task MaterializeAsyncRejectsANullGraph() =>
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await Host.MaterializeAsync(null!, TestToken));

    [Fact]
    public async Task MaterializeAsyncStartsTheRunBeforeItReturns()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2), _ => gate.Wait(), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Nothing in this test moves the run along, and it reaches the first element anyway.
        await gate.Reached;

        gate.Open();
        await run.Completion;
    }

    [Fact]
    public async Task MaterializeAsyncRejectsADocumentThatDoesNotValidateAgainstTheCatalog()
    {
        RunnableGraph graph = Graph(
            Document([Node("stage-1", "unheard-of"), Node("stage-2", "ignore")], [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("does not validate against the local stage catalog", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("[unknown-stage]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("stage-1", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncNamesEveryDiagnosticOfAFailedReport()
    {
        // Two unresolvable stages, so the message has to carry both: a caller fixing a foreign document
        // needs the whole report and not its first line.
        RunnableGraph graph = Graph(
            Document([Node("stage-1", "unheard-of"), Node("stage-2", "also-unheard-of")], [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("2 diagnostics", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("also-unheard-of", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsADocumentThatIsTwoChains()
    {
        // Two independent source-to-sink chains validate against the catalog perfectly well: every port is
        // connected and every capability is declared. Only the runtime, which executes one chain, refuses.
        GraphDocument document = Document(
            [
                Node("stage-1", "from-enumerable"),
                Node("stage-2", "ignore"),
                Node("stage-3", "from-enumerable"),
                Node("stage-4", "ignore"),
            ],
            [Edge("stage-1", "stage-2"), Edge("stage-3", "stage-4")]);

        RunnableGraph graph = Graph(
            document,
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Ignore()),
                ("stage-3", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(2))),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("one linear chain", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("begin a chain", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsANodeWithNoBoundBehavior()
    {
        RunnableGraph graph = Graph(
            Document([Node("stage-1", "from-enumerable"), Node("stage-2", "ignore")], [Edge("stage-1", "stage-2")]),
            Bindings(("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1)))));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("'stage-2' has no bound behavior", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsASourceBoundToSomethingThatIsNotASequence()
    {
        RunnableGraph graph = Graph(
            Document([Node("stage-1", "from-enumerable"), Node("stage-2", "ignore")], [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(42)),
                ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("must be bound to a sequence", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("System.Int32", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsAMappingBoundToSomethingThatIsNotAFunction()
    {
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "select"), Node("stage-3", "ignore")],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Select("not a function")),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("must be bound to a Func<TIn, TOut>", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsAFilterBoundToAFunctionThatDoesNotDecide()
    {
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "where"), Node("stage-3", "ignore")],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Where((Func<int, int>)(value => value))),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("must be bound to a Func<T, bool>", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsABindingWhoseShapeCannotStandWhereTheDocumentPutsIt()
    {
        // The document says the middle node maps, and the binding table says it discards. The document is
        // the statement of topology and the table is the statement of behavior; neither is trusted to
        // imply the other, so the disagreement is reported rather than executed.
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "select"), Node("stage-3", "ignore")],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Ignore()),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("'stage-2' is a 'Ignore' stage at position 2 of 3", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("where that shape cannot stand", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsADocumentWhoseNodesAllFeedOneAnother()
    {
        // Every node is fed, so there is no head to walk from at all.
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "select"), Node("stage-2", "select")],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-1")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-2", LocalStageDescriptor.Select((Func<int, int>)(value => value)))));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("no node begins a chain", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializeAsyncRejectsAChainThatLeavesSomeNodesUnreached()
    {
        // One real chain beside a cycle nothing feeds into. Walking from the only head reaches half the
        // document, and a run that executed it would silently ignore the other half.
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "ignore"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-3", "stage-4"), Edge("stage-4", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Ignore()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value)))));

        InvalidOperationException rejected =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("do not form one chain", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("reached 2 of them", rejected.Message, StringComparison.Ordinal);
    }
}
