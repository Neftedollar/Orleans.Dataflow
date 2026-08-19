using System.Globalization;
using Orleans.Core.Internal;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Serialization;
using Orleans.Runtime;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What the two-grain protocol refuses to take on trust from whoever is calling it.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is not authorization and does not pretend to be.</b> Orleans hands a grain no caller identity, so
/// there is nothing here to authenticate against and a cluster is one trust domain: every call these tests
/// make is one an ordinary cluster client can make, and the fix for a deployment that needs tenants apart is
/// clusters apart. What is being pinned is narrower and worth pinning on its own — that the protocol does not
/// believe things it can check for itself. An epoch has to be one the register issued. Reading a declaration
/// must not fence the activation executing it. A report that was refused must not vanish.
/// </para>
/// <para>
/// Every one of these was reachable by accident as well as on purpose: a client holding a stale ticket, a
/// monitor asking a coordinator what a durable run is, a silo that came back while another still believed it
/// owned the run. The trust model is what makes them uninteresting as attacks; the protocol defect is what
/// made them corrupt a run either way.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class TrustBoundaryTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that fails a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AStartCarryingAnEpochNoCoordinatorIssuedIsRefusedAndLeavesTheRunStartable()
    {
        const string Pipeline = "trust-unissued-epoch";
        const string Run = "the-run";

        // A durable run declared and not yet started, which is the state a run grain is in between the two
        // hops of a materialization and again after every silo death.
        PipelineDefinition pipeline = TestPipelines.Recording(Pipeline, count: 3, "trust-unissued-epoch");

        TestDeliveries.Clear("trust-unissued-epoch");

        PipelineRunTicket declared = await Coordinator(pipeline).DeclareDurableRunAsync(
            GraphDocumentSerializer.Serialize(pipeline.Document),
            new DurableRunDeclaration { RunId = Run, EveryElements = 2 });

        IPipelineRunGrain run = RunGrain(pipeline, Run);

        // The epoch nobody can outbid. This member takes an epoch rather than issuing one, so what it used to
        // store was whatever it was handed — after which every declaration compared as older, was answered
        // with long.MaxValue, and the run could never start in that activation again.
        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => run.StartAsync(GraphDocumentSerializer.Serialize(pipeline.Document), long.MaxValue));

        Assert.Contains(
            long.MaxValue.ToString(CultureInfo.InvariantCulture),
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains(Pipeline, refused.Message, StringComparison.Ordinal);

        // And the run starts, under the epoch its own declaration recorded, exactly as if the refused call
        // had never been made. That is the half that matters: a refusal that left the grain wedged would be
        // the same defect wearing a different exception.
        long epoch = await run.EnsureStartedAsync(declared.Epoch);

        Assert.Equal(declared.Epoch, epoch);

        RunStatusSnapshot status = await run.GetStatusAsync(epoch);

        Assert.NotEqual(RunPhase.NotStarted, status.Phase);

        await Poll.UntilAsync(
            () => TestDeliveries.Of("trust-unissued-epoch").Count == 3,
            "the run this grain could still host delivered its elements");
    }

    [Fact]
    public async Task AnEpochNoCoordinatorIssuedDoesNotSupersedeALiveAttempt()
    {
        const string Pipeline = "trust-unissued-supersede";
        const string Run = "the-run";
        const string Halt = "trust-unissued-supersede-halted";

        PipelineDefinition pipeline = TestPipelines.Recording(
            Pipeline,
            count: 4,
            "trust-unissued-supersede",
            halt: Halt);

        TestDeliveries.Clear("trust-unissued-supersede");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        await TestSignals.Reached(Halt);

        IPipelineRunGrain run = RunGrain(pipeline, Run);
        long live = handle.Epoch;

        // The other half of the same defect, on the member a client is meant to call. A declared epoch above
        // this attempt's is how a replacement announces itself, so the grain abandons what it is hosting and
        // takes up the register's version — and a number the register never issued would make that abandon
        // a live run for nothing.
        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => run.EnsureStartedAsync(long.MaxValue - 1L));

        Assert.Contains("not one the coordinator", refused.Message, StringComparison.Ordinal);

        // Undisturbed: same attempt, same epoch, same elements. A restart would have replayed the window
        // between the stored cursor and here.
        RunStatusSnapshot status = await run.GetStatusAsync(live);

        Assert.Equal(RunPhase.Running, status.Phase);
        Assert.Equal(live, status.Epoch);
        Assert.Equal([1L, 2L, 3L, 4L], TestDeliveries.Of("trust-unissued-supersede"));
    }

    [Fact]
    public async Task ReadingADeclarationOfALiveRunFencesNobodyAndItsEndingIsStillRecorded()
    {
        const string Pipeline = "trust-read-does-not-fence";
        const string Run = "tail-run";
        const string Log = "trust-read-does-not-fence";

        TestDeliveries.Clear(Log);

        // The M5.3 defect M5.4 closed by value, reachable again through a member that only reads. A claim
        // used to mint a fresh epoch on every call, so anything that merely asked what this run was — a
        // monitor, a runbook, a second client — superseded the activation executing it; the attempt's own
        // report of how the run ended was then refused as stale, nothing was recorded, and the next
        // activation continued a finished run and ran its tail a second time.
        PipelineDefinition pipeline = TestPipelines.Recording(Pipeline, count: 6, Log);

        OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        long live = handle.Epoch;

        DurableRunClaim? read = await Coordinator(pipeline).ClaimDurableRunAsync(Run);

        Assert.NotNull(read);
        Assert.Equal(live, read!.Epoch);

        await Deadline.Within(handle.Completion, $"the run {handle.RunId} completed");

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], TestDeliveries.Of(Log));

        // The register knows the run is over, which is the thing a reading used to make impossible.
        DurableRunClaim? afterEnd = await Coordinator(pipeline).ClaimDurableRunAsync(Run);

        Assert.Equal(RunPhase.Completed, afterEnd?.Outcome);

        // And the consequence, measured rather than argued: an activation recycled exactly as a silo restart
        // recycles it is told how the run ended instead of being handed a position to continue.
        await RunGrain(pipeline, Run).AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        await using OrleansRunHandle again = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        await Deadline.Within(again.Completion, $"the finished run {again.RunId} reported how it ended");

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], TestDeliveries.Of(Log));
    }

    [Fact]
    public async Task AnEndingThatCouldNotBeRecordedIsCarriedOnEveryLaterReadingOfTheAttempt()
    {
        const string Pipeline = "trust-unrecorded-ending";
        const string Run = "superseded";
        const string Log = "trust-unrecorded-ending";
        const string Gate = "trust-unrecorded-ending-gate";
        const string Halt = "trust-unrecorded-ending-halted";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            Pipeline,
            count: 12,
            Log,
            halt: Halt,
            gate: Gate,
            gateAt: 7);

        OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 5 },
            Token);

        long live = handle.Ticket.Epoch;

        await TestSignals.Reached($"{Gate}-reached");

        // A replacement is the one operation that legitimately supersedes an executing attempt: it mints a
        // fresh epoch and clears the store, and it deliberately does not stop what is running, because the
        // member that rewrites the register may not await a run grain. So what follows is a live attempt
        // whose claim is already stale — the state the fencing on the ending report exists for.
        _ = await Coordinator(pipeline).ReplaceDurableRunAsync(
            GraphDocumentSerializer.Serialize(pipeline.Document),
            new DurableRunDeclaration { RunId = Run, EveryElements = 5 });

        TestSignals.Raise(Gate);

        IPipelineRunGrain run = RunGrain(pipeline, Run);

        // The superseded attempt reaches a terminal state — its next capture presents an ETag the emptied
        // store has moved on from — and its report of that is refused. Refusing it is right; losing it was
        // not, because a durable run whose ending nobody wrote down is exactly the run a later activation
        // resumes and re-runs the tail of.
        await Poll.UntilAsync(
            async () => (await run.GetStatusAsync(live)).UnrecordedEnding is not null,
            "the attempt reported an ending its coordinator refused");

        RunStatusSnapshot status = await run.GetStatusAsync(live);

        Assert.Equal(RunPhase.Faulted, status.Phase);
        Assert.NotNull(status.UnrecordedEnding);
        Assert.Contains("Faulted", status.UnrecordedEnding!, StringComparison.Ordinal);
        Assert.Contains(
            live.ToString(CultureInfo.InvariantCulture),
            status.UnrecordedEnding!,
            StringComparison.Ordinal);

        // Carried on every later reading rather than raised once: a fact about the run, not an event of one
        // poll. Reported as a reading and never as a refusal, because a poll that faulted on this would stop
        // reporting the outcome it is being polled for.
        RunStatusSnapshot again = await run.GetStatusAsync(live);

        Assert.Equal(status.UnrecordedEnding, again.UnrecordedEnding);

        await handle.DisposeAsync();
    }

    /// <summary>Addresses the coordinator of one pipeline.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <returns>The coordinator grain.</returns>
    private IPipelineCoordinatorGrain Coordinator(PipelineDefinition pipeline) =>
        cluster.Cluster.Client.GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value);

    /// <summary>Addresses one run grain of one pipeline.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The run grain.</returns>
    private IPipelineRunGrain RunGrain(PipelineDefinition pipeline, string run) =>
        cluster.Cluster.Client.GetGrain<IPipelineRunGrain>($"{pipeline.Id.Value}/{run}");
}
