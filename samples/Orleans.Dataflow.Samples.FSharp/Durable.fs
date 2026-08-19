namespace Orleans.Dataflow.Samples.FSharp

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Identity
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>A durable run that dies, and a second host that continues it from where it got to.</summary>
/// <remarks>
/// <para>
/// A durable run writes its position into a checkpoint store on a cadence its options declare — here, every
/// few orders. When the first attempt dies, the store still holds the last position that was written down;
/// a second host handed the same document, the same run identity, and the same store continues from there
/// rather than from the beginning.
/// </para>
/// <para>
/// <b>The window between the last checkpoint and the crash is delivered twice, and that is the contract.</b>
/// This is at-least-once delivery, stated as a number the sample prints rather than as a footnote: the
/// orders between the stored position and the moment the attempt died are exactly the orders both attempts
/// saw. Narrowing the window is what the cadence is for, and it is never zero.
/// </para>
/// <para>
/// Both attempts run the same document, which is what makes continuing one legal at all — and which is why
/// the crash is a parameter of the authoring function rather than a difference in the graph. A delegate
/// never enters a document, so a mapping that raises and a mapping that does not are one graph.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Durable =

    /// <summary>The name the two attempts of this run share.</summary>
    /// <remarks>
    /// What separates two durable runs of one graph. A local graph is anonymous, so without a run identity
    /// there would be nothing for a store to key a checkpoint by.
    /// </remarks>
    let private runId = RunId.Create "orders-of-the-day"

    /// <summary>Authors the graph, kills the first attempt, and continues it on a second host.</summary>
    /// <param name="sample">The run this scenario belongs to, which supplies the checkpoint store.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The fingerprint, what each attempt delivered, and the replay window between them.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = SampleOrders.Take(sample.Scale.Pick(full = 12, smokeSize = 6))
            let crashAt = sample.Scale.Pick(full = 8, smokeSize = 5)
            let everyElements = sample.Scale.Pick(full = 3, smokeSize = 2)

            // The sample's own store, which the console application implements in fifty lines against the
            // published interface. Nothing test-only is involved: this is the contract a deployment writes.
            let store = sample.NewCheckpointStore()

            let durable () =
                Orleans.Dataflow.DurableRunOptions(
                    Store = store,
                    RunId = runId,
                    EveryElements = Nullable everyElements
                )

            let firstAttempt = ResizeArray<string>()
            let secondAttempt = ResizeArray<string>()

            let build (failAt: int) (seen: ResizeArray<string>) =
                Source.ofSeq orders
                |> Source.map (fun order ->
                    if order.Sequence = failAt then
                        raise (
                            InvalidOperationException(
                                $"The host died while handling {order.OrderId}. This is the sample's deliberate crash."
                            )
                        )

                    order)
                |> Source.toSink (Sink.forEach (fun (order: OrderEvent) -> seen.Add order.OrderId))

            let crashing = build crashAt firstAttempt
            let continuing = build -1 secondAttempt

            let firstHost = Orleans.Dataflow.LocalDataflowHost()
            let! attempt = firstHost.MaterializeDurableAsync(crashing, durable (), cancellationToken)

            let! failure =
                task {
                    try
                        do! attempt.Completion

                        return "the attempt completed, which means the crash never happened"
                    with :? InvalidOperationException as crash ->
                        return crash.Message
                }

            let afterCrash = attempt.Snapshot()

            // Written out rather than bound with `use!`, and the order is the scenario: the first host's
            // handle has to be gone before the second host picks the same run identity up, and `use!` would
            // hold it to the end of this expression — past the resume it is supposed to precede.
            do! attempt.DisposeAsync()

            // A second host, standing in for a second process: it is handed the same document, the same run
            // identity and the same store, and nothing else passes between them.
            let secondHost = Orleans.Dataflow.LocalDataflowHost()
            use! continued = secondHost.MaterializeFromCheckpointAsync(continuing, durable (), cancellationToken)

            do! continued.Completion

            let afterResume = continued.Snapshot()

            let replayed = HashSet<string>(firstAttempt)

            replayed.IntersectWith secondAttempt

            let delivered = HashSet<string>(firstAttempt)

            delivered.UnionWith secondAttempt

            return
                ScenarioOutcome.Of(
                    [ GraphReading.Of("main", crashing) ],
                    [ Observation.Of("orders-in-the-feed", orders.Count)
                      Observation.Of("checkpoint-every-orders", everyElements)
                      Observation.Of("both-attempts-are-one-document", crashing.Fingerprint = continuing.Fingerprint)
                      Observation.Of("first-attempt/delivered", String.concat " " firstAttempt)
                      Observation.Of("first-attempt/status", string afterCrash.Status)
                      Observation.Of("first-attempt/checkpoints-written", afterCrash.Checkpoints)
                      Observation.Of("first-attempt/failure", failure)
                      Observation.Of("second-attempt/delivered", String.concat " " secondAttempt)
                      Observation.Of("second-attempt/status", string afterResume.Status)
                      Observation.Of(
                          "delivered-twice-the-at-least-once-window",
                          replayed |> Seq.sort |> String.concat " "
                      )
                      Observation.Of("every-order-delivered-at-least-once", delivered.Count = orders.Count) ]
                )
        }
