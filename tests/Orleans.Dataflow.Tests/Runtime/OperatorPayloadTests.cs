using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the catalog and the run planner do with a payload or a binding the authoring API would never have
/// produced.
/// </summary>
/// <remarks>
/// <para>
/// Putting the numbers in the document is what makes these tests possible and necessary at once. A count
/// that lived only in a binding table could not be wrong in a document and could not be checked by
/// anything; a count in a payload can be negative, can be a string, and can be missing, and each of those
/// has to be a diagnostic naming the node rather than a stage that quietly takes some other number of
/// elements.
/// </para>
/// <para>
/// The binding half is the mirror image: the document says which stage a node is, and a binding of the
/// wrong shape for that stage has to be reported where the mismatch is rather than as a cast failure from
/// inside a running loop.
/// </para>
/// <para>
/// Every document here is hand-built, because every one of them is unreachable through the authoring API,
/// whose operators check their arguments before they build anything.
/// </para>
/// </remarks>
public sealed class OperatorPayloadTests
{
    [Theory]
    [InlineData("""{"count":-1}""", "is -1, and it is an integer of zero or more")]
    [InlineData("""{}""", "the member 'count' is missing")]
    [InlineData("""{"count":"3"}""", "is a string, and it is an integer of zero or more")]
    [InlineData("""{"count":3,"from":1}""", "'from' is not one this stage declares")]
    [InlineData("""{"count":4294967296}""", "between -2147483648 and 2147483647")]
    [InlineData("""[]""", "the payload is an array")]
    public async Task ACountPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Counted("take", payload);

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("stage-2", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("skip")]
    [InlineData("take")]
    public async Task EveryCountedOperatorReadsItsCountFromTheDocument(string stage)
    {
        // The number the runtime executes is the document's and not the descriptor's: the binding here
        // declares one element and the payload declares two, and the run delivers two.
        List<int> observed = [];

        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", stage, "local-count-parameters", """{"count":2}"""),
                    Node("stage-3", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4))),
                ("stage-2", stage == "take" ? LocalStageDescriptor.Take(1) : LocalStageDescriptor.Skip(1)),
                ("stage-3", LocalStageDescriptor.ForEach((Action<int>)observed.Add))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(stage == "take" ? [1, 2] : (int[])[3, 4], observed);
    }

    [Theory]
    [InlineData("""{"count":3}""", "the member 'start' is missing")]
    [InlineData("""{"start":3}""", "the member 'count' is missing")]
    [InlineData("""{"count":-1,"start":0}""", "is -1, and it is an integer of zero or more")]
    [InlineData("""{"count":2,"start":2147483647}""", "which is past 2147483647")]
    [InlineData("""{"count":1,"start":0,"step":2}""", "'step' is not one this stage declares")]
    [InlineData("""{"count":1,"start":"0"}""", "is a string, and it is an integer")]
    public async Task ARangePayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "range", "local-range-parameters", payload), Node("stage-2", "ignore")],
                [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Range(0, 1)),
                ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("stage-1", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"maxTrackedKeys":0}""", "is 0, and it is a positive integer")]
    [InlineData("""{}""", "the member 'maxTrackedKeys' is missing")]
    [InlineData("""{"maxTrackedKeys":4,"evict":true}""", "'evict' is not one this stage declares")]
    public async Task ADistinctPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "distinct", "local-distinct-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Distinct(
                    new DistinctOptions { MaxTrackedKeys = 1 },
                    EqualityComparer<int>.Default)),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACountedStageCarryingTheDelegateOnlyParameterContractIsAContractMismatch()
    {
        // The contract is checked before the payload is, so a payload written for another stage is
        // reported as the mismatch it is rather than as a shape complaint about a check it was never
        // meant for.
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "take"), Node("stage-3", "ignore")],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Take(1)),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[parameter-contract-mismatch]", rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("scan", "a Func<TState, T, TState>")]
    [InlineData("take-while", "a Func<T, bool>")]
    [InlineData("take-through", "a Func<T, bool>")]
    [InlineData("skip-while", "a Func<T, bool>")]
    [InlineData("distinct", "an equality comparer")]
    public async Task AnOperatorBoundToSomethingItsShapeDoesNotAcceptIsRefused(string stage, string expected)
    {
        string contract = stage == "distinct" ? "local-distinct-parameters" : "local-parameters";
        string payload = stage == "distinct" ? """{"maxTrackedKeys":4}""" : "{}";

        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", stage, contract, payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", Misbound(stage)),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains($"must be bound to {expected}", rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("from-task", "a Task<T>")]
    [InlineData("failed", "an exception to fail with")]
    [InlineData("unfold", "a UnfoldGenerator<TState, T>")]
    public async Task ASourceBoundToSomethingItsShapeDoesNotAcceptIsRefused(string stage, string expected)
    {
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", stage), Node("stage-2", "ignore")],
                [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", Misbound(stage)),
                ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains($"must be bound to {expected}", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACallbackSinkBoundToAFunctionThatReturnsAValueIsRefused()
    {
        // The distinction the mapping stages do not make: a callback sink returns a Task and not a
        // Task<T>, because it emits nothing.
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "for-each-async", "local-parallelism-parameters", """{"maxConcurrency":1}"""),
                ],
                [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.ForEachAsync(
                    new ParallelismOptions { MaxConcurrency = 1 },
                    (Func<int, CancellationToken, Task<int>>)((value, _) => Task.FromResult(value))))));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(
            "must be bound to a Func<T, CancellationToken, Task>",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACallbackSinkWhoseCallbackProducesNoTaskFaultsTheRunRatherThanFailingObscurely()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2))
            .To(s => s.ForEachAsync(new ParallelismOptions { MaxConcurrency = 1 }, (_, _) => (Task)null!));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("returned no task", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>Builds a hand-made source-to-operator-to-sink graph with the count payload under test.</summary>
    /// <param name="stage">The counted stage's identifier text.</param>
    /// <param name="payload">The parameter payload as JSON text.</param>
    /// <returns>The graph.</returns>
    private static RunnableGraph Counted(string stage, string payload) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", stage, "local-count-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Take(1)),
                ("stage-3", LocalStageDescriptor.Ignore())));

    /// <summary>Builds an occurrence of one stage bound to something that stage cannot use.</summary>
    /// <param name="stage">The stage's identifier text.</param>
    /// <returns>The descriptor, whose binding is deliberately of the wrong shape.</returns>
    /// <remarks>
    /// A string for everything that wants a delegate, and a delegate of the wrong arity for the shapes a
    /// string would not even reach. None of these is reachable through the authoring API, whose generic
    /// signatures make the shapes agree by construction.
    /// </remarks>
    private static LocalStageDescriptor Misbound(string stage) => stage switch
    {
        "scan" => LocalStageDescriptor.Scan(0L, "not a folder"),
        "take-while" => LocalStageDescriptor.TakeWhile("not a predicate"),
        "take-through" => LocalStageDescriptor.TakeThrough((Func<int, int>)(value => value)),
        "skip-while" => LocalStageDescriptor.SkipWhile("not a predicate"),
        "distinct" => LocalStageDescriptor.Distinct(new DistinctOptions { MaxTrackedKeys = 4 }, "not a comparer"),
        "from-task" => LocalStageDescriptor.FromTask("not a task"),
        "failed" => LocalStageDescriptor.Failed(null!),
        _ => LocalStageDescriptor.Unfold(0, "not a generator"),
    };
}
