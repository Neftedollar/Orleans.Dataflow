using System.Globalization;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a cluster whose silos publish different stage vocabularies does with a document, and whether it
/// says enough about the disagreement to act on.
/// </summary>
/// <remarks>
/// <para>
/// Three facts, in increasing order of how uncomfortable they are. A silo that does not know a stage
/// refuses the document and names the stage. A silo that does know it can still be handed a run whose
/// grain lands on one that does not, because a coordinator's acceptance is not a promise about where the
/// run will execute. And two silos that both accept one document still report different catalog
/// identities for it, which is the only signal a client gets that its runs are not all being validated
/// against the same vocabulary.
/// </para>
/// <para>
/// The second of those is the one worth stating plainly, because it is the shape of a real outage: half a
/// deployment upgraded, a document that validates, and a run that fails on a coin flip.
/// </para>
/// </remarks>
[Collection(RollingUpgradeClusterCollectionDefinition.Name)]
public sealed class RollingUpgradeTests(RollingUpgradeCluster cluster)
{
    /// <summary>How many runs the placement-hazard test starts before concluding the coin is not a coin.</summary>
    /// <remarks>
    /// Twenty, over two silos: seeing only one of the two outcomes twenty times running has a probability
    /// of about one in five hundred thousand, so a failure is a placement that is not random or a refusal
    /// that no longer happens — either of which is worth knowing — rather than a run of bad luck.
    /// </remarks>
    private const int PlacementAttempts = 20;

    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AMaterializationValidatedByTheStaleSiloIsRefusedAndNamesWhatItDoesNotKnow()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("upgrade-refused", 2);

        _ = await cluster.PlaceCoordinatorAsync(pipeline, RollingUpgradeCluster.StaleSilo);

        PipelineRejectedException refused =
            await Assert.ThrowsAsync<PipelineRejectedException>(() => cluster.MaterializeAsync(pipeline));

        // The whole report and not the first line of it, and specific enough to act on: which node, which
        // stage reference, and that the problem is the catalog rather than the document. A deployment
        // reading this knows which package version its silos are missing.
        Assert.Contains("does not validate", refused.Message, StringComparison.Ordinal);
        Assert.Contains("stage catalog", refused.Message, StringComparison.Ordinal);
        Assert.Contains("test/double@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("doubled", refused.Message, StringComparison.Ordinal);
        Assert.Contains("does not register", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AcceptanceByTheCoordinatorDoesNotPromiseTheRunGrainsSiloAcceptsItToo()
    {
        // The coordinator is pinned to the silo that knows every stage, so every one of these documents is
        // accepted where it is validated. The run grain is placed by its own key, which contains a fresh
        // run identifier each time, so where it executes is a fresh draw — and the run grain validates the
        // document again, against its own host's catalog.
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("upgrade-placement", 2);

        _ = await cluster.PlaceCoordinatorAsync(pipeline, RollingUpgradeCluster.UpgradedSilo);

        int accepted = 0;
        int refusedByTheRunGrain = 0;
        string firstRefusal = string.Empty;

        for (int attempt = 0; attempt < PlacementAttempts; attempt++)
        {
            try
            {
                await using OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

                await Deadline.Within(handle.Completion, $"the run {handle.RunId} completed");

                Assert.Equal(6L, await handle.GetValueAsync(slot, Token));

                accepted++;
            }
            catch (PipelineRejectedException refusal)
            {
                refusedByTheRunGrain++;

                if (firstRefusal.Length == 0)
                {
                    firstRefusal = refusal.Message;
                }
            }
        }

        Assert.True(
            accepted > 0,
            string.Create(
                CultureInfo.InvariantCulture,
                $"None of {PlacementAttempts} runs executed, so the upgraded silo never hosted one and this test proved nothing about the split."));

        Assert.True(
            refusedByTheRunGrain > 0,
            string.Create(
                CultureInfo.InvariantCulture,
                $"All {PlacementAttempts} runs executed, so no run grain was ever placed on the silo whose catalog lacks the stage — either placement stopped being random or the run grain stopped validating against its own host."));

        // The refusal a run grain produces reads the same as the coordinator's, because it is the same
        // check against a different host's catalog. That is what makes the failure diagnosable: it names
        // the stage rather than reporting that a run could not be built.
        Assert.Contains("test/double@v1", firstRefusal, StringComparison.Ordinal);
        Assert.Contains("does not register", firstRefusal, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoSilosReportDifferentCatalogFingerprintsForOneDocumentBothAccept()
    {
        // A document naming no stage the upgrade touched, so neither silo has any reason to refuse it: what
        // differs is not whether the run starts but which vocabulary it was accepted against. That is the
        // one cross-silo fact the definition plane cannot check on its own, and reporting it on the ticket
        // is the whole mechanism a client has for noticing a half-upgraded cluster.
        (PipelineDefinition upgraded, ResultSlot<long> upgradedSlot) =
            TestPipelines.Failing("upgrade-fingerprint-new", 3, failAt: 99);
        (PipelineDefinition stale, ResultSlot<long> staleSlot) =
            TestPipelines.Failing("upgrade-fingerprint-old", 3, failAt: 99);

        _ = await cluster.PlaceCoordinatorAsync(upgraded, RollingUpgradeCluster.UpgradedSilo);
        _ = await cluster.PlaceCoordinatorAsync(stale, RollingUpgradeCluster.StaleSilo);

        await using OrleansRunHandle fromUpgraded = await cluster.MaterializeAsync(upgraded);
        await using OrleansRunHandle fromStale = await cluster.MaterializeAsync(stale);

        await Deadline.Within(fromUpgraded.Completion, $"the run {fromUpgraded.RunId} completed");
        await Deadline.Within(fromStale.Completion, $"the run {fromStale.RunId} completed");

        // Both ran, and both produced the same total, so the difference reported below is about the
        // vocabulary and not about the documents or about what they computed.
        Assert.Equal(6L, await fromUpgraded.GetValueAsync(upgradedSlot, Token));
        Assert.Equal(6L, await fromStale.GetValueAsync(staleSlot, Token));

        Assert.NotEqual(fromUpgraded.Ticket.CatalogFingerprint, fromStale.Ticket.CatalogFingerprint);
        Assert.NotEqual(string.Empty, fromUpgraded.Ticket.CatalogFingerprint);
        Assert.NotEqual(string.Empty, fromStale.Ticket.CatalogFingerprint);
    }
}
