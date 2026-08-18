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
/// What a durable run identity means when the document behind it changes, and what a run that has ended
/// means to the activation that comes after it.
/// </summary>
/// <remarks>
/// <para>
/// Two questions M5.3 left open and this milestone answers, and they turn out to be one question seen from
/// two sides. A checkpoint is addressed by <c>(graph, run)</c> and describes one document at one revision, so
/// the register has to say which document a name belongs to — and a checkpoint says <em>where</em> a run
/// reached and never <em>whether</em> it is over, so the register has to say that too. Both facts live on the
/// declaration, both are written by somebody who survives the run, and neither is a change to the checkpoint
/// schema.
/// </para>
/// <para>
/// <b>The v1 rules, stated as the tests that hold them.</b> A new revision under a <em>new</em> name runs
/// beside the old one, with its own position and its own ending. A new revision under an <em>existing</em>
/// name is refused by name, and the way to mean it anyway is a spelling that says it destroys —
/// <c>ReplaceDurableRunAsync</c>, which clears the checkpoint and supersedes the attempt. Migrating a
/// checkpoint across a changed document is neither, and is a recorded deferral rather than a silent best
/// effort (ADR 0007).
/// </para>
/// <para>
/// Nothing here kills a silo, which is why it is a class of its own rather than more of the crash suite: the
/// fixture is shared with it for the store and the register, and every claim below is about what a cluster
/// remembers rather than about what surviving one costs.
/// </para>
/// </remarks>
[Collection(MultiSiloClusterCollectionDefinition.Name)]
public sealed class DurableRunRevisionTests(MultiSiloCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TwoRevisionsOfOnePipelineRunSideBySideUnderTwoDurableNames()
    {
        const string FirstLog = "revision-one";
        const string SecondLog = "revision-two";

        TestDeliveries.Clear(FirstLog);
        TestDeliveries.Clear(SecondLog);

        // The revision is a member of the canonical bytes, so two revisions of otherwise identical content
        // are two documents with two identities. Asserted on its own, over documents differing in nothing
        // else, because everything below depends on it and the two pipelines the test actually runs differ
        // in their logs as well.
        Assert.NotEqual(
            TestPipelines.RecordingDoubled("durable-revisions", count: 5, FirstLog, revision: 1).Fingerprint,
            TestPipelines.RecordingDoubled("durable-revisions", count: 5, FirstLog, revision: 2).Fingerprint);

        PipelineDefinition first = TestPipelines.RecordingDoubled(
            "durable-revisions",
            count: 5,
            FirstLog,
            revision: 1,
            halt: "durable-revisions-one-halted");

        PipelineDefinition second = TestPipelines.RecordingDoubled(
            "durable-revisions",
            count: 5,
            SecondLog,
            revision: 2,
            halt: "durable-revisions-two-halted");

        // One pipeline identity, so one coordinator orders both, and two run identities, so they are two
        // runs. That is the whole of rule (a): a new revision does not have to displace the run its
        // predecessor is in the middle of, it only has to be given a name of its own.
        await using OrleansRunHandle running = await cluster.MaterializeDurableAsync(first, "one", everyElements: 3);
        await using OrleansRunHandle revised = await cluster.MaterializeDurableAsync(second, "two", everyElements: 3);

        await TestSignals.Reached("durable-revisions-one-halted");
        await TestSignals.Reached("durable-revisions-two-halted");

        // Both alive at once, each having delivered its own document's elements into its own log, and
        // neither having seen the other's.
        Assert.Equal([2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(FirstLog));
        Assert.Equal([2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(SecondLog));

        Assert.Equal(RunPhase.Running, (await cluster.Run(running).GetStatusAsync(running.Epoch)).Phase);
        Assert.Equal(RunPhase.Running, (await cluster.Run(revised).GetStatusAsync(revised.Epoch)).Phase);

        // Two checkpoints, one per name, each naming the document its own run is a run of. A store keyed by
        // the pipeline alone would have had one of them overwrite the other; keyed by the run, the two
        // revisions have nothing to disagree about.
        LocalCheckpoint held = await StoredAsync("durable-revisions", "one");
        LocalCheckpoint revisedHeld = await StoredAsync("durable-revisions", "two");

        Assert.Equal(first.Fingerprint, held.Graph);
        Assert.Equal(second.Fingerprint, revisedHeld.Graph);
        Assert.NotEqual(held.Graph, revisedHeld.Graph);

        await running.ShutdownAsync();
        await revised.ShutdownAsync();

        await Deadline.Within(running.Completion, $"the run {running.RunId} drained and completed");
        await Deadline.Within(revised.Completion, $"the run {revised.RunId} drained and completed");
    }

    [Fact]
    public async Task ANewRevisionUnderALiveNameIsRefusedAndReplacingItClearsTheCheckpointAndSupersedesTheAttempt()
    {
        const string Log = "durable-replaced";
        const string Run = "replaced";

        TestDeliveries.Clear(Log);

        PipelineDefinition original = TestPipelines.RecordingDoubled(
            "durable-replace",
            count: 5,
            Log,
            revision: 1,
            halt: "durable-replace-halted");

        PipelineDefinition revised = TestPipelines.RecordingDoubled(
            "durable-replace",
            count: 5,
            Log,
            revision: 2,
            halt: "durable-replace-revised-halted");

        OrleansRunHandle running = await cluster.MaterializeDurableAsync(original, Run, everyElements: 3);

        await TestSignals.Reached("durable-replace-halted");

        Assert.Equal([2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await StoredCursorAsync("durable-replace", Run));

        // Rule (b), first half: the ordinary declaration refuses, by name and with both fingerprints, and
        // changes nothing. An author who edited a pipeline and kept its run name reads which document the
        // name already belongs to rather than discovering a checkpoint being restored into a graph it does
        // not describe.
        PipelineResumeRefusedException refused = await Assert.ThrowsAsync<PipelineResumeRefusedException>(
            () => cluster.MaterializeDurableAsync(revised, Run, everyElements: 3));

        Assert.Equal(original.Fingerprint.ToString(), refused.StoredFingerprint);
        Assert.Equal(revised.Fingerprint.ToString(), refused.DeclaredFingerprint);
        Assert.Equal([2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await StoredCursorAsync("durable-replace", Run));

        // Rule (b), second half: the destructive spelling. It is a different method rather than a flag,
        // because what it does is not a variation of declaring — it throws away a position nothing can
        // recover and supersedes an attempt that is executing right now.
        await using OrleansRunHandle replacement = await cluster.ReplaceDurableRunAsync(revised, Run, everyElements: 3);

        Assert.True(
            replacement.Ticket.Epoch > running.Epoch,
            string.Create(
                CultureInfo.InvariantCulture,
                $"The replacement claimed the epoch {replacement.Ticket.Epoch} and the attempt it replaced held {running.Epoch}, so nothing about the old claim stopped being current."));

        await TestSignals.Reached("durable-replace-revised-halted");

        // The discriminator, and the whole point of clearing rather than migrating. A replacement that had
        // inherited the old position would have reopened the source at three and the log would read
        // `…, 8, 10`; from an empty store the new revision runs its document from the beginning.
        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 10,
            "the replacement ran the new revision from the beginning rather than from the old position");

        Assert.Equal([2L, 4L, 6L, 8L, 10L, 2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(Log));

        // And what is in the store now describes the document that is running, which is the invariant the
        // refusal above exists to protect.
        Assert.Equal(revised.Fingerprint, (await StoredAsync("durable-replace", Run)).Graph);

        // The superseded claim is refused, and it is asserted against the grain rather than against the old
        // handle on purpose: a *durable handle follows the run rather than the attempt*, so the handle would
        // adopt the epoch this refusal names and carry on — which after a replacement means carrying on with
        // the document that took the name over. The claim is what stops being current; a handle addressing a
        // name keeps addressing whatever answers to it.
        PipelineFencingException fenced = await Assert.ThrowsAsync<PipelineFencingException>(
            () => cluster.Run(running).ShutdownAsync(running.Ticket.Epoch));

        Assert.Equal(replacement.Epoch, fenced.CurrentEpoch);
        Assert.Equal(running.Ticket.Epoch, fenced.CallerEpoch);

        await replacement.ShutdownAsync();
        await Deadline.Within(replacement.Completion, $"the replacement {replacement.RunId} drained and completed");

        // Disposed last, because disposing it earlier would have cancelled the replacement through the very
        // adoption the assertion above is about.
        await running.DisposeAsync();
    }

    [Fact]
    public async Task AFailedDurableRunReportsItsFailureToALaterActivationRatherThanFaultingAgain()
    {
        const string Log = "durable-failed";
        const string Run = "failed";

        TestDeliveries.Clear(Log);

        // Three elements reach the sink and the fourth throws, with a capture every two — so the run fails
        // with a stored position behind it, which is exactly the state that used to be indistinguishable
        // from a crash.
        PipelineDefinition pipeline = TestPipelines.RecordingFailing("durable-failed", count: 5, Log, failAt: 4);

        string failure;

        await using (OrleansRunHandle first = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2))
        {
            PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
                () => Deadline.Within(first.Completion, $"the run {first.RunId} reported how it ended"));

            failure = failed.FailureMessage ?? string.Empty;

            Assert.Equal([1L, 2L, 3L], TestDeliveries.Of(Log));
            Assert.Equal(2L, await StoredCursorAsync("durable-failed", Run));

            await cluster.Run(first).AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        }

        await using OrleansRunHandle again = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2);

        PipelineRunFailedException reported = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(again.Completion, $"the continued run {again.RunId} reported how it ended"));

        // The same failure, read off the declaration rather than produced a second time — which the log is
        // what proves: a resumed attempt would have reopened at two, delivered the third element again, and
        // then failed at the fourth, leaving `[1, 2, 3, 3]`.
        Assert.Equal(failure, reported.FailureMessage);
        Assert.Equal([1L, 2L, 3L], TestDeliveries.Of(Log));
    }

    [Fact]
    public async Task ReplacingAFinishedRunWithItsOwnDocumentIsHowItIsRunAgain()
    {
        const string Log = "durable-rerun";
        const string Run = "rerun";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording("durable-rerun", count: 5, Log);

        long finished;

        await using (OrleansRunHandle first = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2))
        {
            await Deadline.Within(first.Completion, $"the run {first.RunId} completed");

            finished = first.Epoch;

            Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
        }

        // Declaring a finished run again reports the ending and starts nothing, which is the contract the
        // crash suite pins by value. So the same call cannot also mean "run it again" — and the operation
        // that does is the destructive one, whose meaning does not depend on whether the document changed.
        await using OrleansRunHandle rerun = await cluster.ReplaceDurableRunAsync(pipeline, Run, everyElements: 2);

        Assert.True(
            rerun.Ticket.Epoch > finished,
            string.Create(
                CultureInfo.InvariantCulture,
                $"The re-run claimed the epoch {rerun.Ticket.Epoch} and the run it replaced ended under {finished}, so a stale handle would still have been current."));

        await Deadline.Within(rerun.Completion, $"the re-run {rerun.RunId} completed");

        // From the beginning and not from the position the first run ended at, because the replacement
        // cleared it: a run continuing from cursor four would have delivered `5` alone.
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
    }

    [Fact]
    public async Task ADurableRunThatEndedWithoutEverCheckpointingIsStillRecordedAsFinished()
    {
        const string Log = "durable-unwritten";
        const string Run = "unwritten";

        TestDeliveries.Clear(Log);

        // Durable under a bound no run of five elements will ever reach, so nothing is written to the store
        // at all — the shape the crash suite uses to say that durability promises a stored position is
        // continued rather than that an attempt survives. What this proves is the other half of that
        // sentence: an *ending* travels on the declaration and not in the checkpoint, so it is recorded
        // whether or not a position ever was.
        PipelineDefinition pipeline = TestPipelines.Recording("durable-unwritten", count: 5, Log);

        await using (OrleansRunHandle first = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 1000))
        {
            await Deadline.Within(first.Completion, $"the run {first.RunId} completed");

            Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
            Assert.False(cluster.Checkpoints.Holds(GraphId.Create(pipeline.Id.Value), RunId.Create(Run)));

            await cluster.Run(first).AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        }

        await using OrleansRunHandle again = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 1000);

        await Deadline.Within(again.Completion, $"the continued run {again.RunId} reported how it ended");

        // The sharp version of the claim: there is no checkpoint here to refuse a second run, so without the
        // ending on the declaration this materialization would have started the stream over and the log would
        // hold ten elements.
        Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
        Assert.False(cluster.Checkpoints.Holds(GraphId.Create(pipeline.Id.Value), RunId.Create(Run)));
    }

    [Fact]
    public async Task AnEndingReportedByASupersededAttemptIsRefusedAndLeavesTheDeclarationOpen()
    {
        const string Log = "durable-stale-report";
        const string Run = "stale-report";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-stale-report",
            count: 5,
            Log,
            halt: "durable-stale-report-halted");

        OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);
        IPipelineRunGrain grain = cluster.Run(handle);

        await TestSignals.Reached("durable-stale-report-halted");

        Assert.Equal(3L, await StoredCursorAsync("durable-stale-report", Run));

        IPipelineCoordinatorGrain coordinator = cluster.Coordinator(pipeline);

        // A cancellation is not an ending, and the refusal says why. This is the direct form of the rule the
        // run grain obeys on its own: a deactivation cancels the run it was hosting, so a coordinator that
        // accepted cancellation as an ending would retire a durable run every time its silo recycled.
        ArgumentException phase = await Assert.ThrowsAsync<ArgumentException>(
            () => coordinator.ReportDurableRunEndedAsync(
                Run,
                new RunStatusSnapshot { Phase = RunPhase.Canceled, Epoch = handle.Epoch }));

        Assert.Contains("Cancellation", phase.Message, StringComparison.Ordinal);

        // And an ending reported by a claim that is no longer current is refused by the epoch, exactly as
        // every other call carrying one is. A stale attempt finishing late is the case it exists for: its
        // work is over, its claim is not, and what it would be retiring is somebody else's run.
        PipelineFencingException fenced = await Assert.ThrowsAsync<PipelineFencingException>(
            () => coordinator.ReportDurableRunEndedAsync(
                Run,
                new RunStatusSnapshot { Phase = RunPhase.Completed, Epoch = handle.Epoch - 1 }));

        Assert.Equal(handle.Epoch, fenced.CurrentEpoch);
        Assert.Equal(handle.Epoch - 1, fenced.CallerEpoch);

        // The declaration is untouched by either refusal, which the next activation is what proves: it is
        // handed a document and a position rather than an ending, and replays the window since the
        // checkpoint. The attempt is stopped and its activation waited out first — a cancelled run stays in
        // the grain that cancelled it, so "the next activation" has to be an actual next one.
        await handle.DisposeAsync();
        await grain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        await Poll.UntilAsync(
            async () => await cluster.ActivationsOfAsync(grain) == 0,
            "the activation hosting the cancelled attempt was recycled");

        await using OrleansRunHandle continued = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the continued attempt replayed the window between the stored cursor and the deactivation");

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 4L, 5L], TestDeliveries.Of(Log));

        await continued.ShutdownAsync();
        await Deadline.Within(continued.Completion, $"the continued run {continued.RunId} drained and completed");
    }

    [Fact]
    public async Task AnEndingObservedThroughTheCoordinatorsOwnStatusCallStillAnswers()
    {
        const string Log = "durable-report-hop";
        const string Run = "report-hop";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording("durable-report-hop", count: 4, Log);

        await using OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2);

        // The shape M5.3's resume test pins, seen from the other end. A run grain now calls its coordinator
        // to report an ending as well as to claim an epoch, and a status poll that observes the ending awaits
        // that report before answering — so a poll arriving *through* the coordinator's own passthrough would
        // make each grain wait for the other unless the forwarding member interleaves. It does, and the bound
        // is what says so: without it this call would sit until a response timeout rather than answering.
        //
        // Which of the two reporting paths made the call is a race this test does not pin — the run's own
        // watcher may have reported the moment the run ended, in which case the await inside the run grain is
        // of a settled task. The structure is identical either way, which is why this is a regression guard
        // rather than a second proof of the shape.
        RunStatusSnapshot? terminal = null;

        await Poll.UntilAsync(
            async () =>
            {
                terminal = await Deadline.Within(
                    cluster.Coordinator(pipeline).GetStatusAsync(handle.RunId, handle.Epoch),
                    "the coordinator answered a status call for a run that was ending");

                return terminal.Phase is RunPhase.Completed;
            },
            "the coordinator's own status call reported the run as completed");

        Assert.Equal([1L, 2L, 3L, 4L], TestDeliveries.Of(Log));

        // And the ending that call observed is the one the register holds, which the next activation is what
        // proves: it reports the completion rather than continuing from the stored position.
        await cluster.Run(handle).AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        await using OrleansRunHandle again = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 2);

        await Deadline.Within(again.Completion, $"the continued run {again.RunId} reported how it ended");

        Assert.Equal([1L, 2L, 3L, 4L], TestDeliveries.Of(Log));
    }

    [Fact]
    public async Task ACancelledDurableRunIsNotFinishedAndIsContinuedByItsNextActivation()
    {
        const string Log = "durable-cancelled";
        const string Run = "cancelled";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            "durable-cancelled",
            count: 5,
            Log,
            halt: "durable-cancelled-halted");

        OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);
        IPipelineRunGrain grain = cluster.Run(handle);

        await TestSignals.Reached("durable-cancelled-halted");

        Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await StoredCursorAsync("durable-cancelled", Run));

        // The sharp edge of "completing and failing are endings and cancelling is not", proved through the
        // ordinary cancellation a handle performs rather than by asserting the rule at the coordinator. The
        // activation is recycled afterwards because a cancelled run is still in the grain that cancelled it,
        // and this is a claim about what the *next* activation is told.
        await handle.DisposeAsync();
        await grain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        await Poll.UntilAsync(
            async () => await cluster.ActivationsOfAsync(grain) == 0,
            "the activation hosting the cancelled attempt was recycled");

        await using OrleansRunHandle continued = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the cancelled run was continued rather than reported as finished");

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 4L, 5L], TestDeliveries.Of(Log));

        await continued.ShutdownAsync();
        await Deadline.Within(continued.Completion, $"the continued run {continued.RunId} drained and completed");
    }

    /// <summary>Reads the checkpoint the store currently holds for one durable run.</summary>
    /// <param name="graph">The pipeline identity the run belongs to.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The parsed checkpoint.</returns>
    /// <remarks>
    /// Asked of the store rather than of the run, for the reason the crash suite's own reader is: every
    /// claim here is a claim about what was written down, and a number read off a live run would only say
    /// what that run believes.
    /// </remarks>
    private async Task<LocalCheckpoint> StoredAsync(string graph, string run)
    {
        StoredCheckpoint? stored = await cluster.Checkpoints.ReadAsync(
            GraphId.Create(graph),
            RunId.Create(run),
            Token);

        Assert.True(stored is not null, $"The store holds no checkpoint for the run '{run}' of '{graph}'.");

        Assert.True(
            LocalCheckpointDocument.TryRead(
                stored!.Value.Document,
                out LocalCheckpoint? checkpoint,
                out IReadOnlyList<string> violations),
            $"The stored checkpoint for '{run}' does not read: {string.Join("; ", violations)}.");

        return checkpoint!;
    }

    /// <summary>Reads the cursor the store currently holds for one durable run.</summary>
    /// <param name="graph">The pipeline identity the run belongs to.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The stored position.</returns>
    private async Task<long> StoredCursorAsync(string graph, string run)
    {
        LocalCheckpoint checkpoint = await StoredAsync(graph, run);

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
