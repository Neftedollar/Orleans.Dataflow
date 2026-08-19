namespace Orleans.Dataflow.Samples.FSharp

open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>An asynchronous mapping with a declared concurrency bound, ordered and unordered.</summary>
/// <remarks>
/// <para>
/// Two things are on show and they are independent of each other. The first is that the concurrency a graph
/// declares is exactly the concurrency it gets: the mapping holds every invocation until the declared number
/// of them are inside it together, so a run whose bound was not honored would wait rather than print a
/// number that was not true. The second is what ordering means — the first order's work is arranged to
/// finish after the rest of its concurrent batch, so an ordered mapping still emits it first and an
/// unordered one emits it after them.
/// </para>
/// <para>
/// Ordered and unordered are two documents, because which one a graph is is written down rather than chosen
/// at run time.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module AsyncWork =

    /// <summary>How many invocations of the mapping may be in flight at once.</summary>
    let private declared = 4

    /// <summary>Authors and runs the mapping once ordered and once unordered.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Both fingerprints, the peak concurrency each run reached, and what ordering did.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = SampleOrders.Take(sample.Scale.Pick(full = 8, smokeSize = 8))
            let options = Orleans.Dataflow.ParallelismOptions(MaxConcurrency = declared)
            let host = Orleans.Dataflow.LocalDataflowHost()
            let graphs = ResizeArray<GraphReading>()
            let observations = ResizeArray<Observation>()

            observations.Add(Observation.Of("declared-max-concurrency", declared))
            observations.Add(Observation.Of("orders-mapped", orders.Count))

            // One authoring, run twice: everything except the operator's name is shared, so what the two
            // runs differ by is the operator and not the arrangement around it.
            let attempt (name: string) (unordered: bool) =
                task {
                    let concurrency = Concurrency declared

                    // The rest of the first concurrent batch, and not the rest of the feed: see the note on
                    // Countdown for why waiting past the declared window would deadlock an ordered mapping.
                    let others = Countdown(declared - 1)
                    let arrived = ResizeArray<string>()

                    let accept (order: OrderEvent) (token: CancellationToken) : Task<OrderDocument> =
                        task {
                            do! concurrency.EnterAsync token

                            if order.Sequence = 0 then
                                do! others.WaitAsync token
                            else
                                others.Signal()

                            return OrderDocument.ofEvent order
                        }

                    let mapping =
                        if unordered then
                            Source.mapTaskUnordered options accept
                        else
                            Source.mapTask options accept

                    let graph =
                        Source.ofSeq orders
                        |> mapping
                        |> Source.toSink (Sink.forEach (fun (document: OrderDocument) -> arrived.Add document.OrderId))

                    let! run = host.MaterializeAsync(graph, cancellationToken)

                    do! run.Completion
                    do! run.DisposeAsync()

                    let inFeedOrder =
                        Seq.forall2 (fun (order: OrderEvent) (seen: string) -> order.OrderId = seen) orders arrived

                    graphs.Add(GraphReading.Of(name, graph))
                    observations.Add(Observation.Of($"{name}/peak-invocations-in-flight", concurrency.Peak))
                    observations.Add(Observation.Of($"{name}/orders-emitted", arrived.Count))
                    observations.Add(Observation.Of($"{name}/emitted-in-feed-order", inFeedOrder))
                    observations.Add(
                        Observation.Of($"{name}/first-order-emitted-first", arrived[0] = orders[0].OrderId)
                    )
                }

            do! attempt "ordered" false
            do! attempt "unordered" true

            return ScenarioOutcome.Of(graphs, observations)
        }
