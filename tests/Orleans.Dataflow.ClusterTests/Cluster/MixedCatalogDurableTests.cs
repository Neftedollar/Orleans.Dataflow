using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Runtime;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What a half-upgraded cluster does to a durable run when the silo hosting it dies: which resumes are
/// refused, which are not, and what the difference actually is.
/// </summary>
/// <remarks>
/// <para>
/// The rolling-upgrade row's other half. Its first half — a <em>start</em> validated against the catalog of
/// whichever silo reads the document — has been proved since phase 4; what a resume adds is that the silo
/// which reads the document the second time is chosen by a death rather than by a client, so a run can be
/// accepted by a deployment and then find itself somewhere that cannot execute it.
/// </para>
/// <para>
/// <b>The distinction these two tests exist to draw is between two fingerprints that are easy to confuse.</b>
/// A <em>catalog</em> fingerprint is the identity of a silo's whole vocabulary and differs between any two
/// silos that publish different stage sets; a <em>document</em> fingerprint is the identity of the graph a
/// run is a run of, and it is the only one a resume compares. So a resume onto a silo whose catalog identity
/// is not the dead one's is fine when every stage the document names resolves there, and refused when one
/// does not — and the refusal is about resolution, never about the catalogs being unequal.
/// </para>
/// <para>
/// Both tests restore the cluster afterwards, so the next one starts from two silos.
/// </para>
/// </remarks>
[Collection(MixedCatalogClusterCollectionDefinition.Name)]
public sealed class MixedCatalogDurableTests(MixedCatalogCluster cluster) : IAsyncLifetime
{
    /// <inheritdoc/>
    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await cluster.RestoreSilosAsync();

    [Fact]
    public async Task AResumeIsRefusedByASurvivingSiloThatCannotResolveTheDocumentAndKeepsEverythingForOneThatCan()
    {
        const string Log = "mixed-refused";
        const string Run = "refused";

        TestDeliveries.Clear(Log);

        // A document naming test/double@v1, which is precisely the stage the stale silo's catalog does not
        // publish — so this pipeline is the one a half-upgraded cluster splits on.
        PipelineDefinition pipeline = TestPipelines.RecordingDoubled(
            "mixed-catalog-refused",
            count: 5,
            Log,
            revision: 1,
            halt: "mixed-catalog-refused-halted");

        _ = await cluster.PlaceOnUpgradedAsync(cluster.Coordinator(pipeline));

        PipelineRunTicket declared = await cluster.DeclareAsync(pipeline, Run, everyElements: 3);
        IPipelineRunGrain run = cluster.Run(pipeline, Run);

        // A declaration starts nothing, and a durable run with no checkpoint yet is not resumed by a status
        // poll — so this activates the grain and leaves it empty, which is what makes the placement below a
        // decision rather than a coin flip.
        Assert.Equal(RunPhase.NotStarted, (await run.GetStatusAsync(declared.Epoch)).Phase);

        _ = await cluster.PlaceOnUpgradedAsync(run);

        long epoch = await run.EnsureStartedAsync(declared.Epoch);

        await TestSignals.Reached("mixed-catalog-refused-halted");

        Assert.Equal([2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await cluster.StoredCursorAsync(pipeline, Run));

        // The upgraded silo dies and the stale one is the sole survivor, so where the run comes back is not
        // a matter of luck: there is one host left and its catalog cannot resolve the document.
        _ = await cluster.KillHostOfAsync(run);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => Deadline.Within(
                run.GetStatusAsync(epoch),
                "the surviving silo answered for the run it cannot execute"));

        // Refused by name, in the very words a start is refused with — the M3 catalog discipline run again at
        // resume time, against this host's own vocabulary rather than against the one that accepted the
        // declaration.
        Assert.Contains("test/double@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("does not register", refused.Message, StringComparison.Ordinal);

        // And nothing was consumed by the refusal. The declaration is still there, the checkpoint is still at
        // three, and not one element was delivered — which is what makes the refusal a pause rather than a
        // loss.
        Assert.Equal([2L, 4L, 6L, 8L, 10L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await cluster.StoredCursorAsync(pipeline, Run));

        // The same refusal to every later caller, from what the activation already learned rather than from
        // a second claim: a poll that re-asked the coordinator would mint an epoch per poll.
        PipelineRejectedException again = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => run.GetStatusAsync(epoch));

        Assert.Equal(refused.Message, again.Message);

        // A silo that publishes the whole vocabulary comes back, and the very same declaration continues from
        // the position the refusal left untouched — asked again until an activation lands somewhere that can
        // build the document, which is the operational story rather than a staged one.
        await cluster.RestoreSilosAsync();

        long resumed = await cluster.ResumeOnACapableSiloAsync(run, declared.Epoch);

        Assert.True(
            resumed > epoch,
            $"The resumed attempt claimed {resumed} and the attempt it followed held {epoch}, so the resume did not take a fresh claim.");

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the resumed attempt replayed the window between the stored cursor and the kill");

        Assert.Equal([2L, 4L, 6L, 8L, 10L, 8L, 10L], TestDeliveries.Of(Log));

        await run.ShutdownAsync(resumed);

        await Poll.UntilAsync(
            async () => (await run.GetStatusAsync(resumed)).Phase is RunPhase.Completed,
            "the resumed run drained and completed");
    }

    [Fact]
    public async Task AResumeOntoASiloWhoseCatalogIdentityDiffersSucceedsWhenEveryStageStillResolves()
    {
        const string Log = "mixed-compatible";
        const string Run = "compatible";

        TestDeliveries.Clear(Log);

        // A document naming nothing the upgrade touched, so both catalogs resolve every stage of it while
        // still being two different vocabularies.
        PipelineDefinition pipeline = TestPipelines.Recording(
            "mixed-catalog-compatible",
            count: 5,
            Log,
            halt: "mixed-catalog-compatible-halted");

        _ = await cluster.PlaceOnUpgradedAsync(cluster.Coordinator(pipeline));

        PipelineRunTicket declared = await cluster.DeclareAsync(pipeline, Run, everyElements: 3);
        IPipelineRunGrain run = cluster.Run(pipeline, Run);

        _ = await run.GetStatusAsync(declared.Epoch);
        _ = await cluster.PlaceOnUpgradedAsync(run);

        long epoch = await run.EnsureStartedAsync(declared.Epoch);

        await TestSignals.Reached("mixed-catalog-compatible-halted");

        Assert.Equal([1L, 2L, 3L, 4L, 5L], TestDeliveries.Of(Log));
        Assert.Equal(3L, await cluster.StoredCursorAsync(pipeline, Run));

        SiloAddress killed = await cluster.KillHostOfAsync(run);

        // The coordinator has come back on the stale silo, so a ticket it issues now reports that silo's
        // vocabulary. Two catalog identities for one document both silos accept — the exact fact the phase-4
        // rolling-upgrade test states for starts, restated here as the premise of what follows.
        PipelineRunTicket onTheSurvivor = await cluster.DeclareAsync(pipeline, "probe", everyElements: 1);

        Assert.NotEqual(declared.CatalogFingerprint, onTheSurvivor.CatalogFingerprint);
        Assert.NotEqual(string.Empty, onTheSurvivor.CatalogFingerprint);

        // And the resume happens anyway, on that very silo. A resume compares the *document's* fingerprint
        // with the checkpoint's; the catalog's identity is a fact about the host and is not one of the two
        // numbers being compared. What a resume needs of a host is that every stage resolves there, which is
        // a weaker thing than the two vocabularies being equal — and this is the difference, by value.
        long resumed = await run.EnsureStartedAsync(declared.Epoch);

        Assert.True(
            resumed > epoch,
            $"The resumed attempt claimed {resumed} and the attempt it followed held {epoch}, so the resume did not take a fresh claim.");

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the resumed attempt replayed the window between the stored cursor and the kill");

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 4L, 5L], TestDeliveries.Of(Log));
        Assert.NotEqual(killed, await cluster.SiloOfAsync(run));

        await run.ShutdownAsync(resumed);

        await Poll.UntilAsync(
            async () => (await run.GetStatusAsync(resumed)).Phase is RunPhase.Completed,
            "the resumed run drained and completed");
    }
}
