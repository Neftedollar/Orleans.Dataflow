using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the catalog and the run planner do with a keyed stage's payload or binding that the authoring API
/// would never have produced.
/// </summary>
/// <remarks>
/// <para>
/// Putting the group flow in the document is what makes these tests possible and necessary at once. A flow
/// that lived only in a binding table could not be wrong in a document and could not be checked by
/// anything; a flow in a payload can name a stage nothing declares, a stage that could never run per key, or
/// a stage whose own numbers are nonsense — and each of those has to be a diagnostic rather than a run that
/// quietly does something else.
/// </para>
/// <para>
/// The last two tests are the other half of the same rule: the document says what the group flow is and the
/// binding says what it does, and neither is trusted to imply the other. Every document here is hand-built,
/// because every one of them is unreachable through the authoring API, which writes the payload from the
/// very descriptors it binds.
/// </para>
/// </remarks>
public sealed class GroupByPayloadTests
{
    [Theory]
    [InlineData("""{"maxActiveKeys":0,"overflowPolicy":"fail","group":[]}""", "is 0, and it is a positive integer")]
    [InlineData("""{"overflowPolicy":"fail","group":[]}""", "the member 'maxActiveKeys' is missing")]
    [InlineData("""{"maxActiveKeys":2,"group":[]}""", "the member 'overflowPolicy' is missing")]
    [InlineData("""{"maxActiveKeys":2,"overflowPolicy":"evict-oldest","group":[]}""", "one of 'fail' and 'evict-idle'")]
    [InlineData("""{"maxActiveKeys":2,"overflowPolicy":"fail"}""", "the member 'group' is missing")]
    [InlineData("""{"maxActiveKeys":2,"overflowPolicy":"fail","group":{}}""", "an array of the stages one key's substream is made of")]
    [InlineData("""{"maxActiveKeys":2,"overflowPolicy":"fail","group":[],"window":1}""", "'window' is not one this stage declares")]
    public async Task AKeyedPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(Keyed(payload), TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("stage-2", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""[3]""", "stage 1 of the member 'group' is not an object")]
    [InlineData("""[{"parameters":{}}]""", "has no 'stage' member naming which stage it is")]
    [InlineData("""[{"stage":"local/nope@v1","parameters":{}}]""", "and no local stage is called that")]
    [InlineData("""[{"stage":"local/select-async@v1","parameters":{"maxConcurrency":1}}]""", "a group flow runs fused per key, so it holds element stages only")]
    [InlineData("""[{"stage":"local/buffer@v1","parameters":{"capacity":2,"overflowPolicy":"backpressure"}}]""", "a group flow runs fused per key, so it holds element stages only")]
    [InlineData("""[{"stage":"local/merge@v1","parameters":{}}]""", "a group flow runs fused per key, so it holds element stages only")]
    [InlineData("""[{"stage":"local/select@v1"}]""", "has no 'parameters' member")]
    [InlineData("""[{"stage":"local/take@v1","parameters":{"count":-1}}]""", "carries parameters 'local/take@v1' refuses")]
    [InlineData("""[{"stage":"local/select@v1","parameters":{},"name":"mine"}]""", "carries members a group stage does not")]
    [InlineData("""[{"stage":"local/select@v1","parameters":{}},{"stage":"local/never@v1","parameters":{}}]""", "stage 2 of the member 'group'")]
    public async Task AGroupFlowThisVocabularyCouldNotHaveWrittenIsRefusedStageByStage(
        string group,
        string reason)
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(
                Keyed($$"""{"maxActiveKeys":2,"overflowPolicy":"fail","group":{{group}}}"""),
                TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AGroupFlowTheTwoPlanesDisagreeAboutTheLengthOfIsRefused()
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(
                Keyed(
                    """{"maxActiveKeys":2,"overflowPolicy":"fail","group":[{"parameters":{},"stage":"local/select@v1"},{"parameters":{"count":1},"stage":"local/take@v1"}]}""",
                    LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                TestToken));

        Assert.Contains("declares a group flow of 2 stages and is bound to one of 1", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AGroupFlowTheTwoPlanesDisagreeAboutTheShapeOfIsRefusedAtThatPosition()
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(
                Keyed(
                    """{"maxActiveKeys":2,"overflowPolicy":"fail","group":[{"parameters":{},"stage":"local/select@v1"},{"parameters":{"count":1},"stage":"local/take@v1"}]}""",
                    LocalStageDescriptor.Select((Func<int, int>)(value => value)),
                    LocalStageDescriptor.Skip(1)),
                TestToken));

        // A document and a binding built from two different graphs, and the sentence says which stage of
        // which group flow they parted company at.
        Assert.Contains("stage 2 of the group flow of the keyed stage 'stage-2'", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("declared as 'local/take@v1' and bound as 'local/skip@v1'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AKeyedStageBoundToSomethingThatIsNotItsTripleIsRefused()
    {
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node(
                        "stage-2",
                        "group-by",
                        "local-group-by-parameters",
                        """{"maxActiveKeys":2,"overflowPolicy":"fail","group":[]}"""),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("stage-2", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRuntimeReadsTheBoundFromTheDocumentAndNotFromTheBinding()
    {
        // The binding declares a bound of nine and the payload declares one; the run faults at the second
        // key, which is the document's number. What the catalog validates is what the runtime executes.
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node(
                        "stage-2",
                        "group-by",
                        "local-group-by-parameters",
                        """{"maxActiveKeys":1,"overflowPolicy":"fail","group":[]}"""),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                (
                    "stage-2",
                    LocalStageDescriptor.GroupBy(
                        new GroupByOptions { MaxActiveKeys = 9 },
                        (Func<int, int>)(value => value),
                        EqualityComparer<int>.Default,
                        [])),
                ("stage-3", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        TrackedKeyOverflowException failed =
            await Assert.ThrowsAsync<TrackedKeyOverflowException>(async () => await run.Completion);

        Assert.Contains("at most 1 keys", failed.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRuntimeReadsAGroupStagesOwnNumbersFromTheDocumentToo()
    {
        List<int> observed = [];

        // The binding's take declares one element and the payload's declares two, and the run takes two.
        // The two planes are checked against each other for the *shape* of every group stage and never for
        // its numbers, because the numbers are the document's to state — one level down, the same rule the
        // planner follows for every node.
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node(
                        "stage-2",
                        "group-by",
                        "local-group-by-parameters",
                        """{"maxActiveKeys":2,"overflowPolicy":"fail","group":[{"parameters":{"count":2},"stage":"local/take@v1"}]}"""),
                    Node("stage-3", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                (
                    "stage-2",
                    LocalStageDescriptor.GroupBy(
                        new GroupByOptions { MaxActiveKeys = 2 },
                        (Func<int, int>)(_ => 0),
                        EqualityComparer<int>.Default,
                        [LocalStageDescriptor.Take(1)])),
                ("stage-3", LocalStageDescriptor.ForEach((Action<int>)observed.Add))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2], observed);
    }

    /// <summary>Builds a chain whose middle node is a keyed stage carrying a payload written by hand.</summary>
    /// <param name="payload">The parameter payload as JSON text.</param>
    /// <param name="group">The occurrences the binding declares the group flow to be.</param>
    /// <returns>The graph, fingerprinted the way closing one would have fingerprinted it.</returns>
    private static RunnableGraph Keyed(string payload, params LocalStageDescriptor[] group) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "group-by", "local-group-by-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                (
                    "stage-2",
                    LocalStageDescriptor.GroupBy(
                        new GroupByOptions { MaxActiveKeys = 2 },
                        (Func<int, int>)(value => value),
                        EqualityComparer<int>.Default,
                        group)),
                ("stage-3", LocalStageDescriptor.Ignore())));
}
