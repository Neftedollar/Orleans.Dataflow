using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What <c>await using</c> means for a run in a cluster: that it has stopped, and not that the request to
/// stop it has been sent.
/// </summary>
/// <remarks>
/// <para>
/// The two handles answer the same request, and M8.3 removed exactly this shape of asymmetry from
/// <c>ShutdownAsync</c> on the grounds that a caller writing against both should not have to remember which
/// one it holds. Disposal was the one member where it survived: the local handle cancelled and awaited its
/// run, and the cluster handle sent a cancel and returned. Measured before M8.4, disposal of a live run
/// returned in eight milliseconds with the run's completion still <c>WaitingForActivation</c>, a grain call
/// still in flight, and the sink recording two more elements after the block that owned the handle had
/// exited.
/// </para>
/// <para>
/// <b>The assertions are about state and never about elapsed time.</b> What flipped is not that disposal
/// got slower — on a machine under load anything can be slow — but that when it returns, the run has
/// reached a terminal state. So the claim is written as one: read <see cref="OrleansRunHandle.Completion"/>
/// the instant disposal has returned, and require that it has already settled. A wait is bounded by
/// <see cref="Deadline"/> only so that a regression fails this test with a sentence instead of hanging the
/// suite.
/// </para>
/// <para>
/// <b>The run is provably live at the moment disposal is asked for.</b> The range source emits everything
/// it was asked for, raises its halt signal, and then parks until the run is stopped, so a test that has
/// observed that signal knows the run is executing rather than guessing that it might be.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class TeardownTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DisposalReturnsOnlyOnceTheRunHasReachedATerminalState()
    {
        const string Log = "teardown-live-log";
        const string Halt = "teardown-live-halted";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording("teardown-live", count: 4, Log, halt: Halt);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The source has emitted its last element and parked, so the run is still executing here and will
        // go on executing until something stops it. Disposal is the something.
        await TestSignals.Reached(Halt);

        await Deadline.Within(handle.DisposeAsync().AsTask(), "disposal to return");

        // The flip. Nothing was awaited on this task before disposal, so its state is entirely disposal's
        // doing: settled means the teardown observed the run stop, and cancelled means it was this handle
        // that stopped it.
        Assert.True(handle.Completion.IsCompleted);
        Assert.True(handle.Completion.IsCanceled);
    }

    [Fact]
    public async Task NothingTheRunDoesHappensAfterDisposalHasReturned()
    {
        const string Log = "teardown-quiet-log";
        const string Halt = "teardown-quiet-halted";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording("teardown-quiet", count: 4, Log, halt: Halt);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached(Halt);
        await Deadline.Within(handle.DisposeAsync().AsTask(), "disposal to return");

        // Read once, immediately, and then compared against a second reading taken after everything else
        // this test does. The comparison is not a wait dressed up as one: the run has settled, so what the
        // sink holds cannot change, and the point of reading twice is that before M8.4 it could.
        IReadOnlyList<long> settled = TestDeliveries.Of(Log);

        Assert.True(handle.Completion.IsCompleted);
        Assert.Equal(settled, TestDeliveries.Of(Log));

        // A cancelled run abandons rather than drains, so how many of the four reached the sink is not
        // fixed; that none of them arrives afterwards is.
        Assert.True(settled.Count <= 4, $"the sink recorded {settled.Count} of four elements");
    }

    [Fact]
    public async Task DisposingARunThatHasAlreadyEndedKeepsTheOutcomeItHad()
    {
        const string Log = "teardown-ended-log";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording("teardown-ended", count: 3, Log);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Deadline.Within(handle.Completion, "the run to complete");
        await Deadline.Within(handle.DisposeAsync().AsTask(), "disposal to return");

        // Disposal cancels, and a run that has already reached a terminal state keeps the one it had: the
        // wait M8.4 added observes an ending rather than manufacturing one.
        Assert.Equal(TaskStatus.RanToCompletion, handle.Completion.Status);
        Assert.Equal([1L, 2L, 3L], TestDeliveries.Of(Log));
    }

    [Fact]
    public async Task DisposalStillNeverThrowsAndLeavesTheFailureWhereItWas()
    {
        const string Log = "teardown-failed-log";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.RecordingFailing("teardown-failed", count: 4, Log, failAt: 3);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => handle.Completion);

        // The contract the new wait must not break: a teardown that rethrew the run's failure would replace
        // whatever exception the caller's own block was unwinding with, and how a run ended is what
        // Completion is for.
        await Deadline.Within(handle.DisposeAsync().AsTask(), "disposal to return");

        Assert.True(handle.Completion.IsFaulted);
        Assert.Contains("fail at 3", failed.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CancellingTheRunReachesAGrainCallAlreadyInFlightInTheSink()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "teardown-sink-call",
            AdapterVocabulary.Feed,
            AdapterVocabulary.Pricing,
            sink: AdapterVocabulary.CancellableRecording);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The sink has handed its callee a call that nothing in this suite can answer, so at this moment the
        // run is provably holding one grain call in flight at its terminal — and the segment's own thread is
        // blocked on it, which is what a terminal's window does to keep its bound with no scheduler.
        await TestSignals.Reached(AdapterLedgerGrain.CancellableEntered);

        Task disposal = handle.DisposeAsync().AsTask();

        // The claim, and it is about the callee rather than about the run: the grain observed the token its
        // caller handed it being cancelled. Until M8.4 closed it, a terminal received no run context at all —
        // DataflowStageRuntime.Terminal took a Func<object?> and had no parameter a token could arrive
        // through — so the window submitted every call with CancellationToken.None and nothing a cancelled
        // run did could raise this signal. Reverting that one argument makes this wait time out.
        await TestSignals.Reached(AdapterLedgerGrain.CancellableCancelled);

        // And the cancellation is what released the blocked thread: the run reaches a terminal state, and
        // the callee is no longer holding anything, so nothing is left running behind this test.
        await Deadline.Within(disposal, "disposal to return");

        Assert.True(handle.Completion.IsCompleted);
        Assert.Equal(0, AdapterObservations.InFlight);
    }

    [Fact]
    public async Task AGracefulShutdownStillDrainsAGrainCallSinkRatherThanAbandoningIt()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "teardown-sink-drain",
            AdapterVocabulary.Feed,
            AdapterVocabulary.Pricing,
            sink: AdapterVocabulary.DrainingRecording);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // This callee watches the token it was handed, so the shutdown below is asked for at the one moment
        // where cancelling the sink's call would be observable rather than merely different in principle.
        await TestSignals.Reached(AdapterLedgerGrain.DrainEntered);

        await handle.ShutdownAsync();

        // The other half of which token a sink carries, and the reason it is the run token rather than the
        // stop token: a shutdown stops production and lets everything already admitted reach the terminal,
        // so a call in flight here must survive it. Released after the shutdown was asked for, the parked
        // call finishes and the run ends cleanly; carrying the stop token instead, this call is cancelled
        // the instant the shutdown lands, the element it was carrying is lost, and the run fails rather
        // than completing.
        TestSignals.Raise(AdapterLedgerGrain.DrainRelease);

        await Deadline.Within(handle.Completion, "the run to drain and complete");

        // Both assertions are about the call that was in flight, and neither is about how many elements
        // came after it. A shutdown stops production, so how much of the feed had been admitted when this
        // one landed is a property of how far the source had run ahead — which is a scheduling fact and
        // not a promise the run makes. Asserting a count here asserted that production had finished, which
        // is exactly what a shutdown is entitled not to let happen.
        Assert.Equal(TaskStatus.RanToCompletion, handle.Completion.Status);
        Assert.NotEmpty(AdapterObservations.Recorded);
        Assert.Equal(0, AdapterObservations.InFlight);
    }

    [Fact]
    public async Task DisposingTwiceIsStillHarmless()
    {
        const string Log = "teardown-twice-log";
        const string Halt = "teardown-twice-halted";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording("teardown-twice", count: 2, Log, halt: Halt);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached(Halt);

        await Deadline.Within(handle.DisposeAsync().AsTask(), "the first disposal to return");

        // The second disposal has a settled completion to observe rather than a run to stop, which is the
        // path a caller reaches by disposing a handle inside a block that also disposes it.
        await Deadline.Within(handle.DisposeAsync().AsTask(), "the second disposal to return");

        Assert.True(handle.Completion.IsCanceled);
    }
}
