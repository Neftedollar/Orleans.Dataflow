namespace Orleans.Dataflow.Samples.FSharp

open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>The same pipeline, materialized on a real silo through the ordinary hosting API.</summary>
/// <remarks>
/// <para>
/// Nothing here is a test facility. The console application builds a silo with the .NET generic host,
/// registers this library on it with <c>AddOrleansDataflow</c>, and resolves the client host the ordinary
/// way; what this module does is author a pipeline and hand it over. That is the whole deployment story,
/// and it is short because the interesting part of it is the vocabulary rather than the plumbing.
/// </para>
/// <para>
/// <b>A pipeline is not a graph with a name on it.</b> Declaring an identity re-closes the document under
/// that identity, so a pipeline's fingerprint differs from its graph's by design — a pipeline's fingerprint
/// is the fingerprint of the deployable document. It is also why every stage here is a registered one: a
/// graph holding a delegate declares itself nondeployable, and <c>Pipeline.define</c> refuses it by name
/// rather than shipping a document a silo could not resolve.
/// </para>
/// <para>
/// The slot is recovered from the pipeline rather than kept from the closing call. A closed graph's slot
/// binds to that built instance; a pipeline's binds to the fingerprint and the lineage, which is what lets
/// a run started by one process be read by another.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Cluster =

    /// <summary>The lineage this pipeline belongs to.</summary>
    let private lineage = "sample-orders"

    /// <summary>Which revision of that lineage this is.</summary>
    let private revision = 1

    /// <summary>The percentage the discounting stage takes off every order.</summary>
    let private discountPercent = 10

    /// <summary>The smallest amount a discounted document has to be worth to be counted.</summary>
    let private minimumAmount = 20

    /// <summary>Authors the pipeline, runs it on the silo, and reports what the run produced.</summary>
    /// <param name="sample">The run this scenario belongs to, which carries the cluster's client host.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The pipeline's fingerprint, the tally, and the run's snapshot counters.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = sample.Scale.Pick(full = 12, smokeSize = 4)

            let graph, _ =
                Source.ofRegistered SampleVocabulary.Feed "feed" (SampleVocabulary.FeedParameters orders)
                |> Source.viaRegistered
                    SampleVocabulary.Discount
                    "discount"
                    (SampleVocabulary.DiscountParameters discountPercent)
                |> Source.toRegisteredResult
                    "accepted"
                    SampleVocabulary.Tally
                    "tally"
                    (SampleVocabulary.TallyParameters "accepted-orders" minimumAmount)

            let pipeline = graph |> Pipeline.define lineage revision
            let accepted = pipeline.ResultSlot("accepted", SampleVocabulary.TallyContract)

            use! run = sample.Cluster.MaterializeAsync(pipeline, cancellationToken)

            // Watching termination is how a client learns a remote run is over: the handle answers with the
            // ending the run reached rather than with an exception, so a completed run and a failed one are
            // told apart by reading rather than by catching.
            let! ending = run.WatchTermination
            let! tally = run.GetValueAsync(accepted, cancellationToken)
            let! snapshot = run.SnapshotAsync cancellationToken

            return
                ScenarioOutcome.Of(
                    [ GraphReading.Of("pipeline", pipeline) ],
                    [ Observation.Of("orders-the-feed-emitted", orders)
                      Observation.Of("declared-discount-percent", discountPercent)
                      Observation.Of("declared-minimum-amount", minimumAmount)
                      Observation.Of("orders-the-silo-accepted", tally)
                      Observation.Of("run-ending", string ending.Kind)
                      Observation.Of("run-status", string snapshot.Status)
                      Observation.Of("dropped-elements", snapshot.DroppedElements)
                      Observation.Of("supervised-failures", snapshot.SupervisedFailures)
                      Observation.Of("checkpoints", snapshot.Checkpoints) ]
                )
        }
