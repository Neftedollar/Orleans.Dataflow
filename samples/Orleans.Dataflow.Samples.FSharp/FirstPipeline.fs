namespace Orleans.Dataflow.Samples.FSharp

open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Samples

// Orleans.Dataflow itself is deliberately not opened: its Source, Flow and Sink are the C# facade's
// spellings of the concepts this module authors with, and an open would put two of each name in scope with
// errors that do not say so. Everything from it is written out in full.

/// <summary>A source, a filter, a map, and a fold, run locally, with one typed result slot.</summary>
/// <remarks>
/// <para>
/// This is the repository README's F# snippet, complete and running. Nothing about it is elaborate on
/// purpose: the four lines below are the whole authoring vocabulary a reader needs before any of the seven
/// scenarios after it makes sense, and the twin in the C# project is the README's other snippet, unchanged.
/// </para>
/// <para>
/// The two spellings build the same document, which is what the runner checks and what the rest of this
/// sample exists to keep true.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module FirstPipeline =

    /// <summary>Authors the pipeline in F#, runs it, and reports what it produced.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The graph's fingerprint and the count the fold resolved.</returns>
    [<CompiledName("RunAsync")>]
    let runAsync (sample: SampleRun) (cancellationToken: CancellationToken) : Task<ScenarioOutcome> =
        task {
            let orderEvents = SampleOrders.Take(sample.Scale.Pick(full = 12, smokeSize = 4))

            // The README's snippet. `toResult` answers a tuple where the C# facade answers through an out
            // parameter, because F# already has the shape C# has to reach for a keyword to express.
            let graph, processed =
                Source.ofSeq orderEvents
                |> Source.filter (fun order -> order.IsValid)
                |> Source.map OrderDocument.ofEvent
                |> Source.toResult "processed" (Sink.aggregate 0L (fun count _ -> count + 1L))

            let host = Orleans.Dataflow.LocalDataflowHost()

            // A run handle is IAsyncDisposable, which `use!` binds and disposes at the end of the scope —
            // on the way out of an exception as well as on the way out of the last line. Disposing stops
            // the run and waits for it to be stopped.
            use! run = host.MaterializeAsync(graph, cancellationToken)

            let! count = run |> Run.value processed cancellationToken

            do! run.Completion

            return
                ScenarioOutcome.Of(
                    [ GraphReading.Of("main", graph) ],
                    [ Observation.Of("orders-in-the-feed", orderEvents.Count)
                      Observation.Of("orders-the-filter-kept", count) ]
                )
        }
