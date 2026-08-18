using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the failure-injection seam promises: an ordinary stage of an ordinary document that throws exactly
/// where a test said it would, on every run.
/// </summary>
/// <remarks>
/// <para>
/// Determinism is the whole contract, so every assertion here is an exact one. A fault point that threw
/// "about the second element" would pass a test that counted failures, and would be useless for proving what
/// a supervision policy does — which is the only reason this seam exists.
/// </para>
/// <para>
/// The arming is declared in the graph rather than set through the control wherever the source produces on
/// its own, because a run starts as soon as it is materialized: a test that armed through the control there
/// would be racing the elements it wanted to fail. The control's own tests pace the run through a source
/// probe, which is what makes "from the next arrival" a moment a test can name.
/// </para>
/// </remarks>
public sealed class FaultPointTests
{
    [Fact]
    public async Task ADeclaredFaultPointThrowsAtExactlyTheArrivalItNames()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        FaultInjectedException failed =
            await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        // The arrival is in the exception because it is the fact the test declared: a run that failed at the
        // third element when the arming said the second is a run that offered one twice.
        Assert.Equal(2, failed.Arrival);
        Assert.Equal([1], observed);
    }

    [Fact]
    public async Task AFaultPointArmedForNothingPassesEveryElementThrough()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Never, firstFailure: 1))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task AFaultPointArmedAlwaysThrowsAtEveryArrivalFromTheOneItNames()
    {
        List<int> observed = [];

        // A resuming scope is what makes "always" observable at all: without one the run would end at the
        // first failure, and always and once would be the same run.
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Always, firstFailure: 3)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2], observed);
        Assert.Equal(2, run.SupervisedFailures);
    }

    [Fact]
    public async Task AFaultPointArmedOnceHealsAfterTheArrivalItNames()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3)))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The one difference from the test above, in one element: once heals and always does not.
        Assert.Equal([1, 2, 4], observed);
        Assert.Equal(1, run.SupervisedFailures);
    }

    [Fact]
    public async Task TheControlReportsEveryArrivalAndEveryThrow()
    {
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Via(TestFlow.FaultPoint<int>("faulted", FaultPointMode.Always, firstFailure: 3))
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>())
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IFaultPoint faulted = await run.GetValueAsync(graph.Control<IFaultPoint>("faulted"), TestToken);

        // The fault point stands above the scope, so nothing supervises it and the run ends at the third
        // element. Both counters are read after the run has come to rest, which is the only moment they are
        // facts rather than readings.
        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        Assert.Equal(3, faulted.ElementsSeen);
        Assert.Equal(1, faulted.FaultsThrown);
    }

    [Fact]
    public async Task ATestArmsAFaultPointThroughItsControlFromTheNextArrival()
    {
        List<int> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Via(TestFlow.FaultPoint<int>("faulted", FaultPointMode.Never, firstFailure: 1))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);
        IFaultPoint faulted = await run.GetValueAsync(graph.Control<IFaultPoint>("faulted"), TestToken);

        await probe.EmitAsync(1, TestToken);
        await probe.EmitAsync(2, TestToken);
        await Reaches(() => observed.Count == 2, "both elements reaching the sink", TestToken);

        // Two arrivals have already happened, and the arming counts from the next one — which is what makes
        // this a statement about the elements the test is about to emit rather than about the run's history.
        // The second of them is the one that fails, so the third element passes and the fourth does not.
        faulted.Arm(FaultPointMode.Once, firstFailure: 2);

        await probe.EmitAsync(3, TestToken);
        await Reaches(() => observed.Count == 3, "the third element reaching the sink", TestToken);
        await probe.EmitAsync(4, TestToken);

        FaultInjectedException failed =
            await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        Assert.Equal(4, failed.Arrival);
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task DisarmingHealsAFaultPointBeforeItEverThrows()
    {
        List<int> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Via(TestFlow.FaultPoint<int>("faulted", FaultPointMode.Never, firstFailure: 1))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);
        IFaultPoint faulted = await run.GetValueAsync(graph.Control<IFaultPoint>("faulted"), TestToken);

        await probe.EmitAsync(1, TestToken);
        await Reaches(() => observed.Count == 1, "the first element reaching the sink", TestToken);

        // Armed for every element from the next one, and then healed before that element arrives. The claim
        // is the one a test needs from a control it may change its mind with: the last arming wins, and it
        // takes effect at the next element rather than retroactively.
        faulted.Arm(FaultPointMode.Always, firstFailure: 1);
        faulted.Disarm();

        await probe.EmitAsync(2, TestToken);
        await probe.EmitAsync(3, TestToken);
        probe.Complete();

        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
        Assert.Equal(0, faulted.FaultsThrown);
    }

    [Fact]
    public async Task AFaultPointThrowsWhateverItsFactoryAnswers()
    {
        RunnableGraph graph = Source.From([1, 2])
            .Via(TestFlow.FaultPoint<int>(
                FaultPointMode.Once,
                firstFailure: 1,
                arrival => new BufferOverflowException($"injected at {arrival}")))
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        BufferOverflowException failed =
            await Assert.ThrowsAsync<BufferOverflowException>(async () => await run.Completion);

        // What a fault point throws is a binding, so the run reports the author's own instance unwrapped,
        // exactly as it reports a throwing lambda's.
        Assert.Equal("injected at 1", failed.Message);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphHaveTwoFaultPointsWithTwoCounters()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Via(TestFlow.FaultPoint<int>("faulted", FaultPointMode.Never, firstFailure: 1))
            .To(Sink.Ignore<int>());

        ResultSlot<IFaultPoint> slot = graph.Control<IFaultPoint>("faulted");

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await first.Completion;

        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);
        await second.Completion;

        IFaultPoint one = await first.GetValueAsync(slot, TestToken);
        IFaultPoint other = await second.GetValueAsync(slot, TestToken);

        Assert.NotSame(one, other);
        Assert.Equal(3, one.ElementsSeen);
        Assert.Equal(3, other.ElementsSeen);
    }

    [Fact]
    public void AFaultPointIsAnOrdinaryStageTheCatalogValidates()
    {
        RunnableGraph graph = Source.From([1])
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
            .To(Sink.Ignore<int>());

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

        Assert.True(report.IsValid);

        // The arming is in the document, because it changes what the graph observably does. A reader of the
        // document can see which element this graph fails at without running it.
        StageNode injected = graph.Document.Nodes.Single(node => node.Stage.Stage.ToString() == "fault-point");

        Assert.Equal("""{"firstFailure":2,"mode":"once"}""", injected.Parameters.ToString());
    }

    [Fact]
    public void TwoArmingsAreTwoGraphs()
    {
        RunnableGraph second = Source.From([1])
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
            .To(Sink.Ignore<int>());

        RunnableGraph third = Source.From([1])
            .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3))
            .To(Sink.Ignore<int>());

        // The rule this vocabulary follows everywhere, read over the injection seam: a number that changes
        // what a graph does is in the payload, and therefore in the fingerprint taken over it.
        Assert.NotEqual(second.Fingerprint, third.Fingerprint);
    }

    [Fact]
    public void AFaultPointRefusesAnArrivalBelowOne()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 0));

        Assert.Equal("firstFailure", refused.ParamName);
    }

    [Fact]
    public void AFaultPointRefusesAModeNoMemberDeclares()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => TestFlow.FaultPoint<int>((FaultPointMode)9, firstFailure: 1));

        Assert.Equal("mode", refused.ParamName);
    }

    [Fact]
    public void AFaultPointIsRefusedInsideAGroupFlowByName()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2]).GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value,
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 1))));

        // One counter per key is not what "fail the second element" means to the test that wrote it, so the
        // refusal is by name rather than a reading nobody asked for.
        Assert.Equal("group", refused.ParamName);
        Assert.Contains("'local/fault-point@v1' at position 1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AControlBearingFaultPointIsRefusedInsideAScopeByName()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>("faulted", FaultPointMode.Once, firstFailure: 1))));

        // The stages of a scope are not nodes, so a slot declared on one would be a slot nothing could ever
        // resolve. The refusal says exactly that and names the control.
        Assert.Equal("scope", refused.ParamName);
        Assert.Contains("declaring the control 'faulted'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFaultPointInsideAScopeNeedsNoControlAndDrivesAllOfIt()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Select(value => value * 10))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([10, 30, 40], observed);
        Assert.Equal(1, run.SupervisedFailures);
    }
}
