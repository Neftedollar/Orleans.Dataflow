using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a valve promises: which elements pass, what a closed one does to everything above it, and what a
/// pause, a shutdown, and a cancellation do to a run held at one.
/// </summary>
/// <remarks>
/// <para>
/// A valve is the first control this vocabulary declares in the middle of a chain rather than at one of its
/// ends, and nothing about the control machinery needed a line for that: the graph builder collects a
/// control from any occurrence, the planner sorts controls by the port they are declared on rather than by
/// where the node stands, and a run hands every control out as soon as it exists. These tests are what makes
/// that a fact rather than a reading of the code.
/// </para>
/// <para>
/// The valve reads no clock, so most of these need none; the ones that assert what a closed valve does to a
/// stop use one only to prove that nothing else was moving.
/// </para>
/// </remarks>
public sealed class ValveTests
{
    [Fact]
    public async Task AnOpenValveIsAStageThatDoesNothing()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Valve("gate")
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "elements", out ResultSlot<IReadOnlyList<int>> elements);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        await run.Completion;

        Assert.True(valve.IsOpen);
        Assert.Equal([1, 2, 3], await run.GetValueAsync(elements, TestToken));
    }

    [Fact]
    public async Task AClosedValveHoldsTheStreamAndOpeningItLetsEverythingThrough()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Valve("gate", ValveMode.Closed)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        // The run is held at its first element and stays there: a closed valve waits rather than dropping,
        // so nothing is lost by the hold and nothing arrives during it.
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Empty(observed);
        Assert.False(valve.IsOpen);
        Assert.False(run.Completion.IsCompleted);

        valve.Open();

        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task ClosingAValveTakesEffectAtTheNextElementAndOpeningItAgainReleasesIt()
    {
        List<int> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Valve("gate")
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        await probe.EmitAsync(1, TestToken);
        await Reaches(() => observed.Count == 1, "the first element passing an open valve", TestToken);

        valve.Close();

        await probe.EmitAsync(2, TestToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        // The element that had already passed is downstream and is not called back; the one behind it waits.
        Assert.Equal([1], observed);

        valve.Open();

        await Reaches(() => observed.Count == 2, "the held element after the valve opened", TestToken);

        probe.Complete();

        await run.Completion;

        Assert.Equal([1, 2], observed);
        Assert.True(valve.IsOpen);
    }

    [Fact]
    public async Task AClosedValveBackpressuresEverythingAboveItAndHoldsNothingOfItsOwn()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 2 })
            .Valve("gate", ValveMode.Closed)
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        // A valve has no capacity of its own: what accumulates is exactly the declared buffer, one element
        // in the valve's own hand, and one in the source's hand at a boundary with no room. A fifth pull
        // would mean the valve had read ahead of the element it is holding.
        await Reaches(() => elements.Pulls >= 4, "the buffer above the valve filling", TestToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Equal(4, elements.Pulls);
        Assert.Equal(0L, run.DroppedElements);

        valve.Open();

        await run.Completion;

        Assert.Equal(8L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task PausingARunHeldAtAClosedValveReachesQuiescence()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Valve("gate", ValveMode.Closed)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        // The wait a closed valve takes is one of this runtime's own, so it says so to the pause gate: a run
        // that is waiting for a switch nobody has flipped can still be paused.
        await run.PauseAsync(TestToken).WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Assert.True(run.IsPaused);

        // Opened while the run is held: the wait ends, and the element stays in the stage's hand until the
        // run is resumed, which is the same park every clock wait takes.
        valve.Open();

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Empty(observed);
        Assert.Equal(0, clock.PendingTimers);

        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task ShuttingDownARunHeldAtAClosedValveDeliversTheElementItWasHolding()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Valve("gate", ValveMode.Closed)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        await Reaches(() => !valve.IsOpen, "the valve being closed", TestToken);
        await run.ShutdownAsync();
        await run.Completion;

        // A stop is not a stream: the element the valve was holding is kept rather than held for a switch
        // nobody will flip, and the elements the source still had are not admitted, which is what "stop
        // pulling and keep what you have" means at a valve.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1], observed);
    }

    [Fact]
    public async Task CancellingARunHeldAtAClosedValveAbandonsIt()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Valve("gate", ValveMode.Closed)
            .To(s => s.ForEach(observed.Add));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IValve valve = await run.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        await Reaches(() => !valve.IsOpen, "the valve being closed", TestToken);

        // The claim is that it returns: disposal waits for every segment to leave its loop, so a valve wait
        // that could not be woken by a cancellation would hang here rather than fail an assertion.
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphHaveTwoValves()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1])
            .Valve("gate", ValveMode.Closed)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        IValve one = await first.GetValueAsync(graph.Control<IValve>("gate"), TestToken);
        IValve two = await second.GetValueAsync(graph.Control<IValve>("gate"), TestToken);

        Assert.NotSame(one, two);

        one.Open();

        await first.Completion;

        // Opening one run's valve says nothing about the other's: a control belongs to its run the way an
        // enumerator and a fold state do.
        Assert.False(two.IsOpen);
        Assert.False(second.Completion.IsCompleted);

        two.Open();

        await second.Completion;

        Assert.Equal([1, 1], observed);
    }

    [Fact]
    public async Task TwoValvesInOneChainAreTwoControlsAndBothHaveToBeOpen()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2])
            .Valve("first", ValveMode.Closed)
            .Select(value => value * 10)
            .Valve("second", ValveMode.Closed)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IValve first = await run.GetValueAsync(graph.Control<IValve>("first"), TestToken);
        IValve second = await run.GetValueAsync(graph.Control<IValve>("second"), TestToken);

        first.Open();

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Empty(observed);

        second.Open();

        await run.Completion;

        Assert.Equal([10, 20], observed);
    }

    [Fact]
    public void AValveDeclaresItsStartingStateInTheDocumentAndNothingElse()
    {
        RunnableGraph open = Source.From([1]).Valve("gate").To(s => s.Ignore());
        RunnableGraph closed = Source.From([1]).Valve("gate", ValveMode.Closed).To(s => s.Ignore());

        Assert.Equal(
            """{"mode":"open"}""",
            open.Document.Nodes.Single(node => node.Stage.Stage.Value == "valve").Parameters.ToElement().GetRawText());
        Assert.Equal(
            """{"mode":"closed"}""",
            closed.Document.Nodes.Single(node => node.Stage.Stage.Value == "valve").Parameters.ToElement().GetRawText());

        // Two graphs that start in different states are two graphs, and one program built twice is one.
        Assert.NotEqual(open.Fingerprint, closed.Fingerprint);
        Assert.Equal(
            Source.From([1]).Valve("gate").To(s => s.Ignore()).Fingerprint,
            open.Fingerprint);
    }

    [Fact]
    public void AValveNamesItsControlAndRefusesAStateNoMemberDeclares()
    {
        Source<int> source = Source.From([1]);

        Assert.Equal(
            "controlName",
            Assert.Throws<ArgumentNullException>(() => source.Valve(null!)).ParamName);
        Assert.Equal(
            "controlName",
            Assert.Throws<ArgumentException>(() => source.Valve("not a name")).ParamName);
        Assert.Equal(
            "initialMode",
            Assert.Throws<ArgumentOutOfRangeException>(() => source.Valve("gate", (ValveMode)7)).ParamName);
    }

    [Theory]
    [InlineData("""{"mode":"half"}""", "a valve starts in one of 'open' and 'closed'")]
    [InlineData("""{}""", "the member 'mode' is missing")]
    [InlineData("""{"mode":true}""", "is a boolean, and it is one of two state names")]
    public async Task AValvePayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "valve", "local-valve-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Valve(ValveMode.Open, Orleans.Dataflow.Identity.ResultSlotId.Create("gate"))),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("stage-2", refused.Message, StringComparison.Ordinal);
        Assert.Contains(reason, refused.Message, StringComparison.Ordinal);
    }
}
