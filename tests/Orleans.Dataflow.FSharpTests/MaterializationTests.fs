namespace Orleans.Dataflow.FSharpTests

open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Xunit

/// <summary>
/// An F#-authored graph runs: the same host, the same handle, the same outcomes.
/// </summary>
/// <remarks>
/// <para>
/// The frontend produces the shared closed-graph value, so materialization is the public host's and needs
/// no F# shim; what these tests pin is that a graph authored here actually executes — the delegates stored
/// by the F# modules are the shapes the runtime's delegate adapter expects — and that the run's outcome
/// surfaces (completion, the watch, the snapshot) answer for it exactly as they answer for a C#-authored
/// one.
/// </para>
/// <para>
/// A handle is <see cref="T:System.IAsyncDisposable"/>, which <c>use!</c> binds and disposes at the end of
/// its scope — on the way out of a failed assertion as well as on the way out of the last line. Disposing
/// stops the run and waits for it to be stopped; every run here has completed on its own before the scope
/// ends, so the disposal is a release rather than a stop.
/// </para>
/// </remarks>
type MaterializationTests() =

    static let host = Orleans.Dataflow.LocalDataflowHost()

    static let token () = TestContext.Current.CancellationToken

    [<Fact>]
    member _.``An F#-authored slice materializes and resolves its result``() : Task =
        task {
            let graph, total =
                Source.ofSeq [ 1; 2; 3; 4 ]
                |> Source.map (fun value -> value + 1)
                |> Source.filter (fun value -> value % 2 = 0)
                |> Source.toResult "total" (Sink.aggregate 0L (fun state value -> state + int64 value))

            use! run = host.MaterializeAsync(graph, token ())

            let! sum = run.GetValueAsync(total, token ())

            // 1..4 mapped to 2..5, evens kept: 2 + 4 = 6.
            Assert.Equal(6L, sum)

            do! run.Completion
            Assert.Equal(Orleans.Dataflow.RunSnapshotStatus.Completed, run.Snapshot().Status)
        }

    [<Fact>]
    member _.``Elements reach a forEach sink in order``() : Task =
        task {
            let observed = ResizeArray<int>()

            let graph =
                Source.ofSeq [ 3; 1; 4; 1; 5 ]
                |> Source.toSink (Sink.forEach observed.Add)

            use! run = host.MaterializeAsync(graph, token ())
            do! run.Completion

            Assert.Equal<int>([ 3; 1; 4; 1; 5 ], observed)
        }

    [<Fact>]
    member _.``A failing F# lambda faults the run with the very exception and the watch reads it``() : Task =
        task {
            let failure = System.InvalidOperationException "the mapping refuses the third element"

            let graph =
                Source.ofSeq [ 1; 2; 3; 4 ]
                |> Source.map (fun value -> if value = 3 then raise failure else value)
                |> Source.toSink Sink.ignore

            use! run = host.MaterializeAsync(graph, token ())

            let! thrown = Assert.ThrowsAsync<System.InvalidOperationException>(fun () -> run.Completion)

            // The instance, not a wrapper: the discipline the C# suite pins holds for an F# lambda too.
            Assert.Same(failure, thrown)

            let! ending = run.WatchTermination

            Assert.Equal(Orleans.Dataflow.RunEndingKind.Failed, ending.Kind)
            Assert.Equal(typeof<System.InvalidOperationException>.FullName, ending.FailureType)
        }

    [<Fact>]
    member _.``Two materializations of one F#-authored graph are two independent runs``() : Task =
        task {
            let graph, total =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.toResult "total" (Sink.aggregate 0 (fun state value -> state + value))

            use! first = host.MaterializeAsync(graph, token ())
            use! second = host.MaterializeAsync(graph, token ())

            let! firstSum = first.GetValueAsync(total, token ())
            let! secondSum = second.GetValueAsync(total, token ())

            Assert.Equal(6, firstSum)
            Assert.Equal(6, secondSum)
        }
