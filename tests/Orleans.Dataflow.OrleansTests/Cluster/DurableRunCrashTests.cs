using System.Globalization;
using Orleans.Core.Internal;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Runtime;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a silo dying does to a run that was declared durable, and what it still does to one that was not.
/// </summary>
/// <remarks>
/// <para>
/// The other half of the row M5.2 advanced with its local half. There the crash was an injected failure
/// inside one process and what survived it was an in-memory store beside the run; here the host stops
/// existing, the run's engine threads stop with it, and what survives is a store the silos share. Nothing
/// about the checkpoint model changes across that boundary — the document, the ETag, the cursor and the
/// refusals are M5.2's, unaltered — and that is the claim: the model was built to be hosted, and hosting it
/// needed a store behind a silo, a resume trigger, and a wire, rather than a second model.
/// </para>
/// <para>
/// <b>The measuring instrument is a log rather than a total.</b> A duplicate window is a claim about which
/// elements were delivered twice, so the recording sink writes down every element it is handed and these
/// tests compare whole sequences. The log lives in the test process, which is what lets it outlive the silo
/// whose death the window is measured across; in a multi-process cluster the shipped answer is a commit
/// mark, and saying so is why the log is confined to a test project.
/// </para>
/// <para>
/// <b>The arithmetic is exact because the graph is deliberately plain.</b> A cursored source straight into a
/// recording sink is one fused segment with no buffer in it, so an element is recorded before the run
/// advances the cursor past it and the stored cursor and the log agree at every quiescent moment. A batch or
/// a declared buffer in the middle has a loss window of its own — measured in the local suite, where it
/// belongs — and putting one here would blur two claims into one.
/// </para>
/// <para>
/// Each test that kills a silo restores the cluster afterwards, so the next one starts from three.
/// </para>
/// </remarks>
[Collection(MultiSiloClusterCollectionDefinition.Name)]
public sealed class DurableRunCrashTests(MultiSiloCluster cluster) : IAsyncLifetime
{
    /// <summary>How many kills the repeated-kill test performs.</summary>
    /// <remarks>
    /// Three, because each one costs a silo teardown and a restart and nothing asserted here distinguishes
    /// three from ten: what the repetition buys is that the outcome is a property of the arrangement rather
    /// than of one lucky moment, and three arrangements make that point at a third of the cost of nine.
    /// </remarks>
    private const int Kills = 3;

    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <inheritdoc/>
    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await cluster.RestoreSilosAsync();

    [Fact]
    public async Task ADurableRunResumesOnASurvivingSiloAndReplaysExactlyTheWindowSinceItsLastCheckpoint()
    {
        const string Log = "durable-window";
        const string Run = "window";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-window",
            count: 5,
            Log,
            halt: "durable-window-halted");

        OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);

        // The source emits five elements and then says so rather than ending, so what the kill interrupts is
        // a run that is alive and has a position. By the time the signal is raised the capture the element
        // bound made due at three has been written: the run holds itself at the very element that reached
        // the bound, writes inside that hold, and only then produces the fourth.
        await TestSignals.Reached("durable-window-halted");

        Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await StoredCursorAsync(pipeline, Run));

        IPipelineRunGrain run = cluster.Run(handle);

        Assert.Equal(1, await cluster.ActivationsOfAsync(run));

        SiloAddress killed = await cluster.KillHostOfAsync(run);

        // Nothing is running anywhere: the attempt is as dead as an ordinary run's would be. What differs is
        // only what is left behind, and what is left behind is a position rather than nothing.
        Assert.Equal(0, await cluster.ActivationsOfAsync(run));

        // The client's own poll is the resume trigger and there is no second protocol. Reading the handle's
        // completion starts the loop; the loop addresses the run; addressing it activates a grain on a
        // surviving silo; that activation finds the checkpoint and continues the run.
        Task completion = handle.Completion;

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the resumed attempt replayed the window between the stored cursor and the kill");

        // The whole claim of at-least-once between commit points, as a sequence rather than a total: the
        // elements after the stored cursor are delivered a second time, in order, and nothing else is.
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 4L, 5L], TestDeliveries.Of(Log));

        // The resumed attempt claimed a fresh epoch — a resume is a new claim to the same run — and the
        // handle followed the run rather than the attempt, which is what makes the loss exception
        // unreachable here.
        await Poll.UntilAsync(
            () => handle.Epoch > handle.Ticket.Epoch,
            "the handle adopted the epoch the resumed attempt claimed");

        RunStatusSnapshot status = await run.GetStatusAsync(handle.Epoch);

        Assert.Equal(RunPhase.Running, status.Phase);
        Assert.Equal(handle.Epoch, status.Epoch);
        Assert.NotEqual(killed, await cluster.SiloOfAsync(run));

        await handle.ShutdownAsync();
        await Deadline.Within(completion, $"the resumed run {handle.RunId} drained and completed");
    }

    [Fact]
    public async Task AResumeReachedThroughTheCoordinatorsOwnStatusCallStillAnswers()
    {
        const string Log = "durable-through-coordinator";
        const string Run = "through-coordinator";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-through-coordinator",
            count: 5,
            Log,
            halt: "durable-through-coordinator-halted");

        await using OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);

        await TestSignals.Reached("durable-through-coordinator-halted");

        Assert.Equal(3L, await StoredCursorAsync(pipeline, Run));

        _ = await cluster.KillHostOfAsync(cluster.Run(handle));

        // The cycle this test exists for. A run grain claims its epoch from its coordinator when it comes
        // up, and the coordinator's status member forwards to run grains — so a poll that arrives through
        // the coordinator makes each grain wait for the other unless the forwarding member interleaves. It
        // does, and this is what says so: without that the call below would sit until a response timeout
        // rather than answering at all.
        PipelineFencingException refused = await Assert.ThrowsAsync<PipelineFencingException>(
            () => Deadline.Within(
                cluster.Coordinator(pipeline).GetStatusAsync(handle.RunId, handle.Ticket.Epoch),
                "the coordinator answered a status call that woke a durable run"));

        // The refusal is the evidence rather than an inconvenience: a fresh epoch means the resume happened
        // on the very turn this call drove, and the epoch it names is the one the resumed attempt claimed.
        Assert.True(refused.CurrentEpoch > handle.Ticket.Epoch);
        Assert.Equal(handle.Ticket.Epoch, refused.CallerEpoch);

        RunStatusSnapshot status = await Deadline.Within(
            cluster.Coordinator(pipeline).GetStatusAsync(handle.RunId, refused.CurrentEpoch),
            "the coordinator answered for the resumed attempt");

        Assert.Equal(RunPhase.Running, status.Phase);

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the resumed attempt replayed the window");
    }

    [Fact]
    public async Task RepeatedKillsLeaveACheckpointThatStillReadsAndAWindowNoWiderThanTheDeclaredBound()
    {
        const string Log = "durable-battered";
        const string Run = "battered";
        const int Count = 2000;

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-battered",
            Count,
            Log,
            halt: "durable-battered-halted");

        List<long> interrupted = [];

        for (int attempt = 1; attempt <= Kills; attempt++)
        {
            // The same declaration every time, which is what a durable run identity means: the first call
            // starts the run and every later one continues whatever is there. Two thousand elements at a
            // capture per element is a long stream of writes to be killed in the middle of.
            OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 1);

            _ = await cluster.KillHostOfAsync(cluster.Run(handle));

            // Either the write in flight completed or it did not, and the resumed run must not be able to
            // tell the difference by finding half a document. The store answers that, and it answers it as
            // itself rather than through a run: what it holds parses, names this graph, and carries a
            // position inside the stream.
            long stored = await StoredCursorAsync(pipeline, Run);

            Assert.InRange(stored, 0L, Count);

            interrupted.Add(stored);

            await cluster.RestoreSilosAsync();
        }

        // Without this the test could pass having proved nothing: three kills that all landed after the
        // stream had already ended would leave one attempt's log and one final cursor, and every assertion
        // below would hold trivially. Measured rather than hoped for — over nine kills of this shape every
        // one of them landed mid-stream — and stated as a requirement so that a machine fast enough to
        // change that fails here, naming the fix, rather than quietly weakening the suite.
        Assert.Contains(
            interrupted,
            stored => stored < Count);

        await using OrleansRunHandle final = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 1);

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Contains(Count),
            "the last attempt reached the end of the stream");

        IReadOnlyList<long> delivered = TestDeliveries.Of(Log);

        // Nothing was lost, whatever moment each kill landed on: a resume reopens at the stored cursor and
        // the elements between that cursor and the crash were already recorded, so the union of the attempts
        // is the whole stream.
        for (long element = 1; element <= Count; element++)
        {
            Assert.Contains(element, delivered);
        }

        // And the window is bounded by the declared cadence rather than merely finite. A capture is due
        // after every element and is written inside the hold that element's delivery requested, so at any
        // instant the store is at most one element behind what the sink has recorded — which makes "at most
        // one replayed element per kill" an arithmetic consequence of the bound and not an observation.
        Assert.InRange(delivered.Count, Count, Count + Kills);

        await final.ShutdownAsync();
        await Deadline.Within(final.Completion, $"the run {final.RunId} drained and completed");
    }

    [Fact]
    public async Task ASupersededAttemptsCheckpointWriteIsRefusedAndKillsThatAttempt()
    {
        const string Log = "durable-fenced";
        const string Run = "fenced";
        const string Gate = "durable-fenced-gate";

        TestDeliveries.Clear(Log);

        // The gate is what makes this deterministic. A live run has to have a capture still to come when the
        // store is superseded, and a run whose source has run out or parked at its halt has none; the gate
        // stops the source between the capture at five and the capture at ten, which is a rendezvous rather
        // than a length of time. It sits at the seventh element and not the sixth because a capture due at
        // five does not complete until the sixth has been produced — the segment takes its next step before
        // it parks — so a gate one element earlier would hold the capture open instead of the stream.
        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-fenced",
            count: 12,
            Log,
            halt: "durable-fenced-halted",
            gate: Gate,
            gateAt: 7);

        await using OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 5);

        await TestSignals.Reached($"{Gate}-reached");

        Assert.Equal(5L, await StoredCursorAsync(pipeline, Run));

        // The state a second attempt of this run would have left behind: the same document under a newer
        // ETag, written by somebody else. Orleans will not let two activations of one run grain exist, so
        // the race cannot be staged for real; what can be staged is exactly what the race leaves, which is
        // the precedent the coordinator store set for its own fencing test.
        cluster.Checkpoints.Supersede(GraphId.Create(pipeline.Id.Value), RunId.Create(Run));

        TestSignals.Raise(Gate);

        // The next capture presents an ETag the store has moved on from, and the documented consequence is
        // that the stale writer dies rather than retrying: retrying would overwrite the truth a fresh
        // attempt is building with a snapshot of a run that owns nothing.
        PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        Assert.Equal(typeof(CheckpointConflictException).FullName, failed.FailureType);
        Assert.Contains(Run, failed.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADurableRunDeclaredTwiceUnderOneNameWithTwoDocumentsIsRefusedByName()
    {
        const string Run = "two-documents";

        PipelineDefinition first = TestPipelines.Recording("durable-mismatch", count: 4, "durable-mismatch-a");
        PipelineDefinition second = TestPipelines.Recording("durable-mismatch", count: 5, "durable-mismatch-a");

        Assert.NotEqual(first.Fingerprint, second.Fingerprint);

        TestDeliveries.Clear("durable-mismatch-a");

        await using (OrleansRunHandle running = await cluster.MaterializeDurableAsync(first, Run, everyElements: 2))
        {
            await Deadline.Within(running.Completion, $"the run {running.RunId} completed");
        }

        // V1 continues one document per durable run identity, and the refusal is by name with both
        // fingerprints on it: an author who edited a pipeline and kept its run name reads which document the
        // name already belongs to rather than discovering a checkpoint being restored into a graph it does
        // not describe.
        PipelineResumeRefusedException refused = await Assert.ThrowsAsync<PipelineResumeRefusedException>(
            () => cluster.MaterializeDurableAsync(second, Run, everyElements: 2));

        Assert.Equal(first.Fingerprint.ToString(), refused.StoredFingerprint);
        Assert.Equal(second.Fingerprint.ToString(), refused.DeclaredFingerprint);
        Assert.Contains(Run, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnActivationWhoseCheckpointNamesAnotherGraphRefusesToContinueTheRun()
    {
        const string Run = "foreign-checkpoint";

        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-foreign",
            count: 3,
            "durable-foreign",
            halt: "durable-foreign-halted");

        TestDeliveries.Clear("durable-foreign");

        // Written before anything activates the run grain, so what the activation finds is a checkpoint of a
        // graph that is not this one. The other path to the same refusal — a second declaration under one
        // name — is refused by the coordinator before a run exists; this is the belt-and-braces half, and it
        // is the one that catches a store somebody else wrote into.
        _ = await cluster.Checkpoints.WriteAsync(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(Run),
            LocalCheckpointDocument.Write(
                GraphFingerprint.OfSerialized([1, 2, 3]),
                GraphRevision.Create(1),
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            Token);

        PipelineResumeRefusedException refused = await Assert.ThrowsAsync<PipelineResumeRefusedException>(
            () => cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2));

        Assert.Equal(pipeline.Fingerprint.ToString(), refused.DeclaredFingerprint);
        Assert.NotEqual(refused.DeclaredFingerprint, refused.StoredFingerprint);
        Assert.Empty(TestDeliveries.Of("durable-foreign"));
    }

    [Fact]
    public async Task ACompletedDurableRunIsContinuedByALaterActivationAndRunsItsTailAgain()
    {
        const string Log = "durable-completed";
        const string Run = "completed";

        TestDeliveries.Clear(Log);

        // The limit this milestone leaves, pinned by value rather than described. A run grain persists
        // nothing about how an attempt ended, so once its activation is gone the checkpoint is all there is
        // — and a checkpoint says *where*, never *whether*. A durable run that finished is therefore
        // indistinguishable from one that died at the same position, and the next activation continues it.
        // Five elements at a capture every two, so the captures fall due at the second and the fourth and
        // the last of them completes while the run is plainly still going — the fifth element is what
        // releases it. A count that put the last capture on the very last element would be asking about a
        // different thing: a capture the source's end raced, which either wins or is skipped by the loop's
        // own "the run is over" guard, so the stored position of a run that ended is not a number a test may
        // name. This one is.
        PipelineDefinition pipeline = TestPipelines.Recording("durable-completed", count: 5, Log);

        await using (OrleansRunHandle first = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2))
        {
            await Deadline.Within(first.Completion, $"the run {first.RunId} completed");

            Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
            Assert.Equal(4L, await StoredCursorAsync(pipeline, Run));

            await cluster.Run(first).AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        }

        await using OrleansRunHandle again = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2);

        await Deadline.Within(again.Completion, $"the continued run {again.RunId} completed");

        // The tail after the stored cursor, delivered a second time. At-least-once taken to its conclusion
        // rather than a defect — and the reason a durable run is declared by an author who means it, rather
        // than being what every run gets. Forgetting a finished run's position is an operational decision
        // (`ICheckpointStore.ClearAsync`) that no runtime here makes on a deployment's behalf.
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 5L], TestDeliveries.Of(Log));
    }

    [Fact]
    public async Task ARunThatWasNotDeclaredDurableStillReportsTheLossThroughTheSameKill()
    {
        const string Log = "not-durable";

        TestDeliveries.Clear(Log);

        // The contrast, and it is the same pipeline through the same kill: what changes is only that nobody
        // asked for the run's position to survive, so nothing wrote one and there is nothing to continue.
        PipelineDefinition pipeline = TestPipelines.Recording(
            "not-durable",
            count: 5,
            Log,
            halt: "not-durable-halted");

        OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

        await TestSignals.Reached("not-durable-halted");

        IPipelineRunGrain run = cluster.Run(handle);

        _ = await cluster.KillHostOfAsync(run);

        PipelineRunLostException lost = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        Assert.Contains(handle.RunId, lost.Message, StringComparison.Ordinal);

        // Nothing was replayed, which is the other half of the contrast: the log holds exactly one attempt's
        // worth of elements and the store holds nothing at all for this run.
        Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
        Assert.False(cluster.Checkpoints.Holds(GraphId.Create(pipeline.Id.Value), RunId.Create(handle.RunId)));

        await handle.DisposeAsync();
    }

    [Fact]
    public async Task ADurableRunWithNoCheckpointYetIsALostAttemptExactlyAsAnOrdinaryOneIs()
    {
        const string Log = "durable-too-soon";
        const string Run = "too-soon";

        TestDeliveries.Clear(Log);

        // Durable, and with a bound no run of five elements will ever reach — so the run is declared durable,
        // writes nothing, and dies with nothing to continue from. Stating it as a test rather than as a
        // sentence is the point: durability is not a promise that an attempt survives, it is a promise that
        // a stored position is continued, and a run that never stored one is exactly as lost as one that
        // never asked to be durable.
        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-too-soon",
            count: 5,
            Log,
            halt: "durable-too-soon-halted");

        OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 1000);

        await TestSignals.Reached("durable-too-soon-halted");

        Assert.False(cluster.Checkpoints.Holds(GraphId.Create(pipeline.Id.Value), RunId.Create(Run)));

        _ = await cluster.KillHostOfAsync(cluster.Run(handle));

        PipelineRunLostException lost = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        Assert.Contains(handle.RunId, lost.Message, StringComparison.Ordinal);
        Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));

        await handle.DisposeAsync();
    }

    /// <summary>Reads the cursor the store currently holds for one durable run.</summary>
    /// <param name="pipeline">The pipeline the run belongs to.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The stored position, or zero when the store holds nothing for that pair.</returns>
    /// <remarks>
    /// Asked of the store rather than of the run, because every claim these tests make about a position is a
    /// claim about what was written down: a number read off a live run would only say what that run
    /// believes, and what a resume continues from is what the store holds. The document is parsed with the
    /// runtime's own reader, so a document that no longer reads fails here rather than being quietly
    /// tolerated.
    /// </remarks>
    private async Task<long> StoredCursorAsync(PipelineDefinition pipeline, string run)
    {
        StoredCheckpoint? stored = await cluster.Checkpoints.ReadAsync(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(run),
            Token);

        if (stored is not { } held)
        {
            return 0L;
        }

        Assert.True(
            LocalCheckpointDocument.TryRead(
                held.Document,
                out LocalCheckpoint? checkpoint,
                out IReadOnlyList<string> violations),
            $"The stored checkpoint for '{run}' does not read: {string.Join("; ", violations)}.");

        Assert.Equal(pipeline.Fingerprint, checkpoint!.Graph);
        Assert.Single(checkpoint.Cursors);

        foreach (KeyValuePair<NodeId, CanonicalJsonValue> cursor in checkpoint.Cursors)
        {
            return cursor.Value.ToElement().GetProperty("index").GetInt64();
        }

        throw new InvalidOperationException(string.Create(
            CultureInfo.InvariantCulture,
            $"The checkpoint of '{run}' carries no cursor, which the assertion above has already refused."));
    }
}
