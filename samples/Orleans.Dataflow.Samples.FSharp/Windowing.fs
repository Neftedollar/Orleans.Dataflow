namespace Orleans.Dataflow.Samples.FSharp

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>Bounded grouping by count and time, and a group-by that refuses a key past its bound.</summary>
/// <remarks>
/// <para>
/// Two graphs, and the pair is the lesson. The first closes a group when either four orders have arrived or
/// a window has elapsed, whichever comes first, so the memory it holds is bounded by the count even when the
/// feed goes quiet. The second keeps one running substream per region and declares how many regions it is
/// willing to keep at once; the feed has three and the bound is two, so the third region is refused.
/// </para>
/// <para>
/// <b>The refusal is a designed outcome and not a crash.</b> The run fails with a named exception whose
/// message quotes both the bound the author declared and the key that exceeded it, which is what makes the
/// alternative — a keyed operator that quietly grows until the process dies — the thing this library does
/// not do. Choosing the other policy, evicting the least recently used key instead, is one field on the
/// same options record.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Windowing =

    /// <summary>How many orders close a group.</summary>
    let private groupSize = 4

    /// <summary>How long a group stays open once its first order has arrived.</summary>
    /// <remarks>
    /// Long enough that the count is always what closes a group in this sample, so the batch sizes below are
    /// arithmetic rather than timing. A feed that went quiet mid-group would see the window close it.
    /// </remarks>
    let private window = TimeSpan.FromSeconds 30.0

    /// <summary>How many regions the keyed graph is willing to keep substreams for.</summary>
    let private maxActiveRegions = 2

    /// <summary>Authors and runs both graphs.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Both fingerprints, the batch sizes, and the refusal.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = SampleOrders.Take(sample.Scale.Pick(full = 12, smokeSize = 6))
            let host = Orleans.Dataflow.LocalDataflowHost()
            let graphs = ResizeArray<GraphReading>()
            let observations = ResizeArray<Observation>()

            observations.Add(Observation.Of("orders-in-the-feed", orders.Count))
            observations.Add(Observation.Of("declared-group-size", groupSize))

            let batched, batches =
                Source.ofSeq orders
                |> Source.map OrderDocument.ofEvent
                |> Source.groupedWithin groupSize window
                |> Source.toResult "batches" (Sink.collect (Orleans.Dataflow.CollectOptions(MaxElements = 32)))

            use! batchRun = host.MaterializeAsync(batched, cancellationToken)
            let! groups = batchRun |> Run.value batches cancellationToken

            do! batchRun.Completion

            graphs.Add(GraphReading.Of("grouped-within", batched))
            observations.Add(Observation.Of("groups-emitted", groups.Count))

            observations.Add(
                Observation.Of(
                    "group-sizes",
                    groups
                    |> Seq.map (fun (group: IReadOnlyList<OrderDocument>) -> string group.Count)
                    |> String.concat " "
                )
            )

            // The second graph. One substream per region, two regions allowed, three regions in the feed.
            let keyed =
                Source.ofSeq orders
                |> Source.groupBy
                    (Orleans.Dataflow.GroupByOptions(MaxActiveKeys = maxActiveRegions))
                    (fun (order: OrderEvent) -> order.Region)
                    Flow.identity
                |> Source.toSink Sink.ignore

            graphs.Add(GraphReading.Of("bounded-keys", keyed))
            observations.Add(Observation.Of("declared-max-active-regions", maxActiveRegions))

            use! keyedRun = host.MaterializeAsync(keyed, cancellationToken)

            let refusal =
                task {
                    try
                        do! keyedRun.Completion

                        return "the run completed, which means the bound was never reached"
                    with :? Orleans.Dataflow.TrackedKeyOverflowException as overflow ->
                        return overflow.Message
                }

            let! message = refusal

            observations.Add(Observation.Of("regions-in-the-feed", orders |> Seq.map _.Region |> Seq.distinct |> Seq.length))
            observations.Add(Observation.Of("bounded-keys-refusal", message))

            return ScenarioOutcome.Of(graphs, observations)
        }
