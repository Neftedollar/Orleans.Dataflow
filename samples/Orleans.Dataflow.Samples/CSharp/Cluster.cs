using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// The same pipeline, materialized on a real silo through the ordinary hosting API.
/// </summary>
/// <remarks>
/// <para>
/// Nothing here is a test facility. <c>SampleCluster</c> builds a silo with the .NET generic host, registers
/// this library on it with <c>AddOrleansDataflow</c>, and resolves the client host the ordinary way; what
/// this class does is author a pipeline and hand it over. That is the whole deployment story, and it is
/// short because the interesting part of it is the vocabulary rather than the plumbing.
/// </para>
/// <para>
/// <b>A pipeline is not a graph with a name on it.</b> Declaring an identity re-closes the document under
/// that identity, so a pipeline's fingerprint differs from its graph's by design — a pipeline's fingerprint
/// is the fingerprint of the deployable document. It is also why every stage here is a registered one: a
/// graph holding a delegate declares itself nondeployable, and <c>AsPipeline</c> refuses it by name rather
/// than shipping a document a silo could not resolve.
/// </para>
/// <para>
/// The slot is recovered from the pipeline rather than kept from the closing call, which is why the
/// authoring call's out parameter is discarded below. A closed graph's slot binds to that built instance; a
/// pipeline's binds to the fingerprint and the lineage, which is what lets a run started by one process be
/// read by another.
/// </para>
/// </remarks>
internal static class Cluster
{
    /// <summary>The lineage this pipeline belongs to.</summary>
    private const string Lineage = "sample-orders";

    /// <summary>Which revision of that lineage this is.</summary>
    private const int Revision = 1;

    /// <summary>The percentage the discounting stage takes off every order.</summary>
    private const int DiscountPercent = 10;

    /// <summary>The smallest amount a discounted document has to be worth to be counted.</summary>
    private const int MinimumAmount = 20;

    /// <summary>Authors the pipeline, runs it on the silo, and reports what the run produced.</summary>
    /// <param name="sample">The run this scenario belongs to, which carries the cluster's client host.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The pipeline's fingerprint, the tally, and the run's snapshot counters.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        int orders = sample.Scale.Pick(full: 12, smokeSize: 4);

        RunnableGraph graph = Source
            .FromRegistered(SampleVocabulary.Feed, "feed", SampleVocabulary.FeedParameters(orders))
            .Via(SampleVocabulary.Discount, "discount", SampleVocabulary.DiscountParameters(DiscountPercent))
            .To(
                SampleVocabulary.Tally,
                "tally",
                SampleVocabulary.TallyParameters("accepted-orders", MinimumAmount),
                "accepted",
                out ResultSlot<long> _);

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(Lineage), GraphRevision.Create(Revision));
        ResultSlot<long> accepted = pipeline.ResultSlot("accepted", SampleVocabulary.TallyContract);

        await using OrleansRunHandle run = await sample.Cluster.MaterializeAsync(pipeline, cancellationToken);

        // Watching termination is how a client learns a remote run is over: the handle answers with the
        // ending the run reached rather than with an exception, so a completed run and a failed one are told
        // apart by reading rather than by catching.
        RunEnding ending = await run.WatchTermination;
        long tally = await run.GetValueAsync(accepted, cancellationToken);
        RunSnapshot snapshot = await run.SnapshotAsync(cancellationToken);

        return ScenarioOutcome.Of(
            [GraphReading.Of("pipeline", pipeline)],
            [
                Observation.Of("orders-the-feed-emitted", orders),
                Observation.Of("declared-discount-percent", DiscountPercent),
                Observation.Of("declared-minimum-amount", MinimumAmount),
                Observation.Of("orders-the-silo-accepted", tally),
                Observation.Of("run-ending", ending.Kind.ToString()),
                Observation.Of("run-status", snapshot.Status.ToString()),
                Observation.Of("dropped-elements", snapshot.DroppedElements),
                Observation.Of("supervised-failures", snapshot.SupervisedFailures),
                Observation.Of("checkpoints", snapshot.Checkpoints),
            ]);
    }
}
