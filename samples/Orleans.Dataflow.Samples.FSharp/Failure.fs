namespace Orleans.Dataflow.Samples.FSharp

open System
open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>A stage that throws, inside a supervision scope, twice over.</summary>
/// <remarks>
/// <para>
/// A supervision scope is a section of a graph that answers the failures raised inside it, and the answer is
/// written into the document. Two of the four answers are shown here because they are the two an operator
/// reaches for, and they are two graphs rather than one for a reason worth knowing: <b>this library refuses
/// a scope inside a scope</b>, on the grounds that which of two nested policies wins is a contract nobody
/// has written yet. So "retry, and if that runs out substitute a fallback" is not one scope with two
/// answers; it is a choice between them.
/// </para>
/// <para>
/// The retrying graph offers the failing order again with a declared ladder of waits, and the third offer
/// succeeds, so nothing is lost. The recovering graph meets an order that fails every time, emits a declared
/// fallback in its place, and ends the scope's stream successfully — so everything below the scope drains
/// and the run reports success with fewer orders than it started with.
/// </para>
/// <para>
/// Both runs print the same three counters afterwards, read from the run's snapshot, because "the run
/// succeeded" and "nothing went wrong" are two different readings and the counters are where the difference
/// lives.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Failure =

    /// <summary>How many times the retrying scope offers one order before giving up.</summary>
    /// <remarks>Attempts and not retries, so three means one offer and two re-offers.</remarks>
    let private attempts = 3

    /// <summary>How long the retrying scope waits before each re-offer.</summary>
    /// <remarks>
    /// A ladder rather than a base and a factor, because a ladder is what a document can state exactly: a
    /// reader of the payload sees the waits the run will take. The last rung repeats.
    /// </remarks>
    let private backoff = [| TimeSpan.FromMilliseconds 5.0; TimeSpan.FromMilliseconds 20.0 |]

    /// <summary>The order the retrying graph's stage refuses, twice, before letting it through.</summary>
    let private flakyOrder = 1

    /// <summary>The order the recovering graph's stage refuses every single time.</summary>
    let private poisonOrder = 2

    /// <summary>Authors and runs both graphs.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Both fingerprints, what each run delivered, and the counters afterwards.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = SampleOrders.Take(sample.Scale.Pick(full = 6, smokeSize = 4))
            let host = Orleans.Dataflow.LocalDataflowHost()
            let graphs = ResizeArray<GraphReading>()
            let observations = ResizeArray<Observation>()

            observations.Add(Observation.Of("orders-in-the-feed", orders.Count))
            observations.Add(Observation.Of("declared-attempts", attempts))
            observations.Add(Observation.Of("declared-backoff-rungs", backoff.Length))

            // The retrying graph. Two failures inside a scope that allows three attempts, so the third offer
            // of the order the stage dislikes is the one that succeeds.
            let flaky = FlakyStage(flakyOrder, 2)
            let retried = ResizeArray<string>()

            let retrying =
                Source.ofSeq orders
                |> Source.supervised
                    (Orleans.Dataflow.SupervisionOptions(
                        Form = Orleans.Dataflow.SupervisionForm.Retry,
                        MaxAttempts = attempts,
                        Backoff = backoff,
                        OnExhaustion = Orleans.Dataflow.RetryExhaustion.Fail
                    ))
                    (Flow.map (fun order -> flaky.Pass order))
                |> Source.map OrderDocument.ofEvent
                |> Source.toSink (Sink.forEach (fun (document: OrderDocument) -> retried.Add document.OrderId))

            let! retryRun = host.MaterializeAsync(retrying, cancellationToken)

            do! retryRun.Completion

            let afterRetries = retryRun.Snapshot()

            do! retryRun.DisposeAsync()

            graphs.Add(GraphReading.Of("retry", retrying))
            observations.Add(Observation.Of("retry/times-the-stage-threw", flaky.Raised))
            observations.Add(Observation.Of("retry/orders-delivered", String.concat " " retried))
            observations.Add(Observation.Of("retry/run-status", string afterRetries.Status))
            observations.Add(Observation.Of("retry/supervised-failures", afterRetries.SupervisedFailures))
            observations.Add(Observation.Of("retry/poison-elements", afterRetries.PoisonElements))
            observations.Add(Observation.Of("retry/dropped-elements", afterRetries.DroppedElements))

            // The recovering graph. The stage refuses one order for ever, so the scope substitutes the
            // declared fallback and ends its stream there.
            let poison = FlakyStage.AlwaysAt poisonOrder
            let recovered = ResizeArray<string>()

            let fallback =
                { Sequence = -1
                  OrderId = "order-fallback"
                  Region = "none"
                  Amount = 0m
                  IsValid = false }

            let recovering =
                Source.ofSeq orders
                |> Source.supervisedRecovering
                    (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Recover))
                    fallback
                    (Flow.map (fun order -> poison.Pass order))
                |> Source.map OrderDocument.ofEvent
                |> Source.toSink (Sink.forEach (fun (document: OrderDocument) -> recovered.Add document.OrderId))

            let! recoverRun = host.MaterializeAsync(recovering, cancellationToken)

            do! recoverRun.Completion

            let afterRecovery = recoverRun.Snapshot()

            do! recoverRun.DisposeAsync()

            graphs.Add(GraphReading.Of("recover", recovering))
            observations.Add(Observation.Of("recover/times-the-stage-threw", poison.Raised))
            observations.Add(Observation.Of("recover/orders-delivered", String.concat " " recovered))
            observations.Add(Observation.Of("recover/run-status", string afterRecovery.Status))
            observations.Add(Observation.Of("recover/supervised-failures", afterRecovery.SupervisedFailures))
            observations.Add(Observation.Of("recover/poison-elements", afterRecovery.PoisonElements))
            observations.Add(Observation.Of("recover/dropped-elements", afterRecovery.DroppedElements))

            return ScenarioOutcome.Of(graphs, observations)
        }
