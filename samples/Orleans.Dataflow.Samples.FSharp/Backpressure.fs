namespace Orleans.Dataflow.Samples.FSharp

open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>A fast source, a slow sink, and a bounded buffer between them, run under two policies.</summary>
/// <remarks>
/// <para>
/// The same shape twice, so that the overflow policy is the only thing that differs and the two kept sets
/// are therefore a statement about the policy. The buffer is declared to hold three elements, the sink
/// stops dead on the first one it is given, and the source keeps going until it has offered everything it
/// has. What is left when the sink is let go is what the policy chose to keep.
/// </para>
/// <para>
/// Two policies mean two documents and therefore two fingerprints, because a declared bound is part of the
/// graph rather than part of the run. That is exactly why the memory a run costs can be read off its
/// document.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Backpressure =

    /// <summary>How many elements the declared buffer holds.</summary>
    let private capacity = 3

    /// <summary>The two policies this scenario contrasts, in the order it runs them.</summary>
    let private policies =
        [ "drop-oldest", Orleans.Dataflow.OverflowPolicy.DropOldest
          "drop-newest", Orleans.Dataflow.OverflowPolicy.DropNewest ]

    /// <summary>Authors and runs the shape once per policy.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>One fingerprint and one kept set per policy.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = SampleOrders.Take(sample.Scale.Pick(full = 9, smokeSize = 6))
            let host = Orleans.Dataflow.LocalDataflowHost()
            let graphs = ResizeArray<GraphReading>()
            let observations = ResizeArray<Observation>()

            observations.Add(Observation.Of("declared-buffer-capacity", capacity))
            observations.Add(Observation.Of("orders-offered", orders.Count))

            for name, policy in policies do
                // A gate the sink stops at, and a feed that waits for it to be stood at before running
                // ahead. Together they make "the source got ahead of the sink" a fact rather than a hope.
                let gate = Gate()
                let feed = PacedFeed<OrderEvent>(orders, gate)
                let kept = ResizeArray<string>()

                let graph =
                    Source.ofSeq feed.Elements
                    |> Source.buffer (Orleans.Dataflow.BufferOptions(Capacity = capacity, OverflowPolicy = policy))
                    |> Source.toSink (
                        Sink.forEach (fun (order: OrderEvent) ->
                            kept.Add order.OrderId
                            gate.Wait())
                    )

                let! run = host.MaterializeAsync(graph, cancellationToken)

                // Everything has now been offered to a buffer that could hold three of it, and the sink is
                // still standing on the first element it was given.
                do! feed.Exhausted.WaitAsync cancellationToken

                gate.Open()

                do! run.Completion

                let snapshot = run.Snapshot()

                do! run.DisposeAsync()

                graphs.Add(GraphReading.Of(name, graph))
                observations.Add(Observation.Of($"{name}/orders-the-sink-saw", String.concat " " kept))
                observations.Add(Observation.Of($"{name}/orders-dropped", snapshot.DroppedElements))

            return ScenarioOutcome.Of(graphs, observations)
        }
