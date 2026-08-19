namespace Orleans.Dataflow.Samples.FSharp

open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: see the note in FirstPipeline.fs.

/// <summary>One stream broadcast into two branches, each with a result of its own.</summary>
/// <remarks>
/// <para>
/// A branch is a flow that ends in a terminal, and a junction call is what turns a list of them into a
/// closed graph. Every element reaches every branch, so the two counts below are two readings of the same
/// twelve orders rather than a partition of them — which is the difference between a broadcast and the
/// balance and partition junctions beside it.
/// </para>
/// <para>
/// Each branch names the slot its result resolves under, so one run answers two questions. The slots are in
/// the author's hand before the junction call is written, which is why the junction call itself answers only
/// the graph.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Junctions =

    /// <summary>What an order has to be worth to count as large.</summary>
    let private large = 50m

    /// <summary>Authors the broadcast, runs it, and reports both branches' results.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The graph's fingerprint and the two counts.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orders = SampleOrders.Take(sample.Scale.Pick(full = 12, smokeSize = 6))

            let largeBranch, largeSlot =
                Flow.filter (fun (document: OrderDocument) -> document.Amount >= large)
                |> Branch.toResult "large" Sink.count

            let northBranch, northSlot =
                Flow.filter (fun (document: OrderDocument) -> document.Region = "north")
                |> Branch.toResult "north" Sink.count

            let graph =
                Source.ofSeq orders
                |> Source.map OrderDocument.ofEvent
                |> Source.broadcastTo [ largeBranch; northBranch ]

            let host = Orleans.Dataflow.LocalDataflowHost()
            use! run = host.MaterializeAsync(graph, cancellationToken)
            let! largeOrders = run |> Run.value largeSlot cancellationToken
            let! northOrders = run |> Run.value northSlot cancellationToken

            do! run.Completion

            return
                ScenarioOutcome.Of(
                    [ GraphReading.Of("main", graph) ],
                    [ Observation.Of("orders-broadcast", orders.Count)
                      Observation.Of("orders-worth-50-or-more", largeOrders)
                      Observation.Of("orders-from-the-north", northOrders) ]
                )
        }
