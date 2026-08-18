using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the catalog and the run planner do with a timing payload the authoring API would never have
/// written, and what a run does when a payload and a binding disagree.
/// </summary>
/// <remarks>
/// <para>
/// Putting the durations in the document is what makes these tests possible and necessary at once. A delay
/// that lived only in a binding table could not be wrong in a document and could not be checked by
/// anything; a duration in a payload can be zero, can be a string, and can be missing, and each of those
/// has to be a diagnostic naming the node rather than a stage that quietly waits for some other length of
/// time.
/// </para>
/// <para>
/// Every document here is hand-built, because every one of them is unreachable through the authoring API,
/// whose operators check their arguments before they build anything.
/// </para>
/// </remarks>
public sealed class TimingPayloadTests
{
    [Theory]
    [InlineData("""{"durationTicks":0}""", "is 0, and it is a positive count of ticks")]
    [InlineData("""{"durationTicks":-1}""", "is -1, and it is a positive count of ticks")]
    [InlineData("""{}""", "the member 'durationTicks' is missing")]
    [InlineData("""{"durationTicks":"1"}""", "is a string, and it is a positive count of ticks")]
    [InlineData("""{"durationTicks":1,"unit":"seconds"}""", "'unit' is not one this stage declares")]
    [InlineData("""[]""", "the payload is an array")]
    public async Task ADurationPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Timed("skip-within", "local-duration-parameters", payload);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", refused.Message, StringComparison.Ordinal);
        Assert.Contains("stage-2", refused.Message, StringComparison.Ordinal);
        Assert.Contains(reason, refused.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"initialDelayTicks":1}""", "the member 'intervalTicks' is missing")]
    [InlineData("""{"intervalTicks":1}""", "the member 'initialDelayTicks' is missing")]
    [InlineData("""{"initialDelayTicks":0,"intervalTicks":0}""", "is 0, and it is a positive count of ticks")]
    public async Task ATickPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "tick", "local-tick-parameters", payload),
                    Node("stage-2", "ignore"),
                ],
                [Edge("stage-1", "stage-2")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1))),
                ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("stage-1", refused.Message, StringComparison.Ordinal);
        Assert.Contains(reason, refused.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"capacity":0,"delayTicks":1,"overflowPolicy":"backpressure"}""", "is 0, and it is a positive integer")]
    [InlineData("""{"capacity":1,"delayTicks":0,"overflowPolicy":"backpressure"}""", "is 0, and it is a positive count of ticks")]
    [InlineData("""{"capacity":1,"delayTicks":1,"overflowPolicy":"drop-everything"}""", "an overflow policy is one of")]
    [InlineData("""{"delayTicks":1,"overflowPolicy":"backpressure"}""", "the member 'capacity' is missing")]
    public async Task ADelayPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Timed("delay", "local-delay-parameters", payload);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(reason, refused.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(
        """{"elements":2,"maximumBurst":1,"mode":"shaping","perTicks":1}""",
        "a burst is at least the rate it is a burst of")]
    [InlineData(
        """{"elements":1,"maximumBurst":1,"mode":"blocking","perTicks":1}""",
        "a throttle mode is one of 'shaping' and 'enforcing'")]
    [InlineData("""{"elements":1,"maximumBurst":1,"mode":"shaping"}""", "the member 'perTicks' is missing")]
    [InlineData("""{"elements":0,"maximumBurst":1,"mode":"shaping","perTicks":1}""", "is 0, and it is a positive integer")]
    public async Task AThrottlePayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Timed("throttle", "local-throttle-parameters", payload);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(reason, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATimingStageReadsItsDurationsFromTheDocumentAndNotFromItsBinding()
    {
        // The numbers the runtime executes are the document's: the binding here says the first tick is due
        // after one second and the payload says two, and the run ticks at two. That is the same rule every
        // counted stage follows, and it is why the payload is what a fingerprint is taken over.
        LocalDataflowHost host = TimingFixtures.Timed(out Testing.TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<DateTimeOffset> observed = [];

        RunnableGraph graph = Graph(
            Document(
                [
                    Node(
                        "stage-1",
                        "tick",
                        "local-tick-parameters",
                        $$"""{"initialDelayTicks":{{TimeSpan.FromSeconds(2).Ticks}},"intervalTicks":{{TimeSpan.FromSeconds(5).Ticks}}}"""),
                    Node("stage-2", "take", "local-count-parameters", """{"count":1}"""),
                    Node("stage-3", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1))),
                ("stage-2", LocalStageDescriptor.Take(1)),
                ("stage-3", LocalStageDescriptor.ForEach((Action<long>)(_ => observed.Add(clock.GetUtcNow()))))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, TimeSpan.FromSeconds(1), TestToken);
        await Task.Delay(TimeSpan.FromMilliseconds(20), TestToken);

        Assert.Empty(observed);

        clock.Advance(TimeSpan.FromSeconds(1));

        await run.Completion;

        Assert.Equal([start + TimeSpan.FromSeconds(2)], observed);
    }

    [Fact]
    public async Task ACycleWhoseOnlyBoundaryIsADelayIsRefusedLikeAnyOtherWaitingLoop()
    {
        // ADR 0005 lists "an explicit delay" beside a dropping buffer as a boundary that makes a cycle
        // legal, and for this engine that is not true: a delay waits for room below it exactly as a
        // backpressuring buffer does — its window fills, and then the pump above it waits for a slot only
        // the pump below could free. It postpones the deadlock rather than breaking it, so it is refused
        // like any other waiting loop and the ADR's parenthetical is corrected rather than implemented.
        List<StageNode> nodes =
        [
            Node("stage-1", "from-enumerable"),
            Node("stage-2", "merge"),
            Node("stage-3", "broadcast"),
            Node("stage-4", "for-each"),
            Node(
                "stage-5",
                "delay",
                "local-delay-parameters",
                $$"""{"capacity":4,"delayTicks":{{TimeSpan.FromSeconds(1).Ticks}},"overflowPolicy":"backpressure"}"""),
        ];
        List<GraphEdge> edges =
        [
            Into("stage-1", "stage-2", 0),
            Edge("stage-2", "stage-3"),
            Leg("stage-3", 0, "stage-4"),
            Leg("stage-3", 1, "stage-5"),
            Into("stage-5", "stage-2", 1),
        ];

        RunnableGraph graph = Graph(
            Declaring(nodes, edges, []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Broadcast()),
                ("stage-4", LocalStageDescriptor.ForEach((Action<int>)(_ => { }))),
                ("stage-5", LocalStageDescriptor.Delay(TimeSpan.FromSeconds(1), new BufferOptions { Capacity = 4 }))));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(
            "passes no boundary that can answer without room below it",
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains("'stage-5'", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Builds the ordinary three-node chain with one timing stage in the middle.</summary>
    /// <param name="stage">The stage identifier text, such as <c>skip-within</c>.</param>
    /// <param name="contract">The parameter contract identifier text.</param>
    /// <param name="payload">The payload text to give it.</param>
    /// <returns>The graph, whose binding is well formed so that only the payload can be at fault.</returns>
    private static RunnableGraph Timed(string stage, string contract, string payload) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", stage, contract, payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", Binding(stage)),
                ("stage-3", LocalStageDescriptor.Ignore())));

    /// <summary>Builds a well-formed binding for one timing stage.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The occurrence.</returns>
    private static LocalStageDescriptor Binding(string stage) => stage switch
    {
        "delay" => LocalStageDescriptor.Delay(TimeSpan.FromSeconds(1), new BufferOptions { Capacity = 1 }),
        "throttle" => LocalStageDescriptor.Throttle(
            new ThrottleOptions { Elements = 1, Per = TimeSpan.FromSeconds(1), MaximumBurst = 1 },
            cost: null),
        _ => LocalStageDescriptor.Timed(LocalStageKind.SkipWithin, TimeSpan.FromSeconds(1)),
    };
}
