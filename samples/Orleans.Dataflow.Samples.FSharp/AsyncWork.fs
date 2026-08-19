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

            // One authoring, run twice: the operator is what the two runs differ by, together with the one
            // part of the arrangement the operator's own contract forces to differ with it — which of the
            // two announces the rest of the batch. The note above the graph below is where that is argued.
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
                            elif not unordered then
                                // Ordered only. See the sink below for who announces these orders in the
                                // unordered run, and why it cannot be this line.
                                others.Signal()

                            return OrderDocument.ofEvent order
                        }

                    let mapping =
                        if unordered then
                            Source.mapTaskUnordered options accept
                        else
                            Source.mapTask options accept

                    // Where the two runs stop being one arrangement, and the reason the callback above has a
                    // branch in it. What the first order has to outlast is the rest of its batch being
                    // *emitted*, and a callback returning is not that: its result is still on its way to the
                    // sink, so an arrangement that counted returns would be counting the wrong event, and
                    // would flip whenever the first order's result overtook one still in flight.
                    //
                    // Unordered: the sink announces each order as it emits it, which is the event the
                    // observation is about, so the first order cannot be emitted first however the machine
                    // schedules the batch.
                    //
                    // Ordered: the callbacks announce themselves instead, and they must. An ordered mapping
                    // holds a finished result until everything before it has been emitted, so a first order
                    // waiting to see the rest of its batch emitted would be waiting for emissions that
                    // cannot happen until it is emitted itself — the same deadlock the note on Countdown
                    // warns about, one step further in. Nothing is lost by it: an ordered mapping emits the
                    // first order first because that is what ordered means, so this run's answer is the
                    // operator's guarantee rather than the arrangement's.
                    let graph =
                        Source.ofSeq orders
                        |> mapping
                        |> Source.toSink (
                            Sink.forEach (fun (document: OrderDocument) ->
                                arrived.Add document.OrderId

                                if unordered then
                                    others.Signal())
                        )

                    use! run = host.MaterializeAsync(graph, cancellationToken)

                    do! run.Completion

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
