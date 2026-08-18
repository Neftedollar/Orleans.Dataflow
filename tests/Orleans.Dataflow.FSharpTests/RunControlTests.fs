namespace Orleans.Dataflow.FSharpTests

open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Xunit
open Orleans.Dataflow.FSharpTests.Fixtures

/// <summary>A one-shot hold a test puts inside a stage, so a run can be stopped at a known point.</summary>
/// <remarks>
/// <para>
/// The F# spelling of the C# suite's own gate, and it exists for the same reason: a test that asserted "the
/// pause has not taken effect yet" by waiting would be hoping rather than measuring.
/// <see cref="P:Orleans.Dataflow.FSharpTests.Gate.Reached"/> completes only once the run is inside the stage,
/// and the run stays there until <see cref="M:Orleans.Dataflow.FSharpTests.Gate.Open"/> is called.
/// </para>
/// <para>
/// The wait blocks the calling thread on purpose. A local stage is a synchronous author function, so a stage
/// that takes a long time is a stage that blocks, and holding the run any other way would test something the
/// runtime does not do. It blocks the run's own dedicated thread and no other.
/// </para>
/// </remarks>
type internal Gate() =

    let reached = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let opened = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    /// <summary>Gets the task that completes when a run first reaches this gate.</summary>
    member _.Reached = reached.Task

    /// <summary>Holds the calling thread until the gate is opened.</summary>
    member _.Wait() =
        reached.TrySetResult() |> ignore
        opened.Task.GetAwaiter().GetResult()

    /// <summary>Opens the gate, releasing whoever is held and everyone after them.</summary>
    member _.Open() = opened.TrySetResult() |> ignore

/// <summary>
/// What an F# author does to a run that is already running: hold it, release it, stop it gracefully, push into
/// it, and read a slot out of it.
/// </summary>
/// <remarks>
/// <para>
/// These are the run handle's own members, and that is the finding rather than an omission: a handle is public
/// runtime surface with no receiver-threading to smooth over, so the F# package wraps exactly one of them —
/// <see cref="M:Orleans.Dataflow.FSharp.Run.value``1"/>, whose argument order is worth reversing for a
/// pipeline — and every other call here is written directly. A suite that could only be written through
/// wrappers would be the argument for adding them.
/// </para>
/// <para>
/// Nothing here waits on a clock. "The pause has not taken effect yet" is asserted only where it is a fact —
/// a run inside an author's function is one no pause can be quiescent for — and every test releases or stops
/// the run it held, because a run left paused is a run whose completion never arrives.
/// </para>
/// </remarks>
type RunControlTests() =

    [<Fact>]
    member _.``A pause takes effect between elements, and resuming loses none``() : Task =
        task {
            let gate = Gate()
            let observed = ResizeArray<int>()

            let graph, total =
                Source.ofSeq [ 1; 2; 3; 4; 5 ]
                |> Source.toResult
                    "total"
                    (Sink.aggregate 0L (fun state value ->
                        observed.Add value
                        gate.Wait()
                        state + int64 value))

            let! run = host.MaterializeAsync(graph, token ())
            do! gate.Reached

            let paused = run.PauseAsync(token ())

            // A fact and not a hope: the run is inside the author's fold with an element in its hands, and a
            // pause that reported quiescence there would be reporting something untrue.
            Assert.False paused.IsCompleted

            gate.Open()
            do! paused

            Assert.True run.IsPaused

            // The element the run was holding was finished and no other was started: the park point is
            // between elements on both sides.
            Assert.Equal<int>([ 1 ], observed)

            do! run.ResumeAsync()
            do! run.Completion

            // The whole sequence, once each, in order: a pause loses no element and repeats none.
            Assert.Equal<int>([ 1; 2; 3; 4; 5 ], observed)

            let! sum = run |> Run.value total (token ())

            Assert.Equal(15L, sum)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``A shutdown drains the run and resolves the aggregate with the state so far``() : Task =
        task {
            let gate = Gate()

            let graph, total =
                Source.ofSeq [ 1; 2; 3; 4 ]
                |> Source.toResult
                    "total"
                    (Sink.aggregate 0L (fun state value ->
                        gate.Wait()
                        state + int64 value))

            let! run = host.MaterializeAsync(graph, token ())
            do! gate.Reached

            let shutdown = run.ShutdownAsync().AsTask()

            gate.Open()
            do! shutdown
            do! run.Completion

            Assert.Equal(Orleans.Dataflow.RunSnapshotStatus.Completed, run.Snapshot().Status)

            // A graceful stop completes the run as if the source had ended, so the aggregate resolves with the
            // state it had reached rather than with nothing. Only the first element was folded.
            let! sum = run |> Run.value total (token ())

            Assert.Equal(1L, sum)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``An ingress queue is resolved as a control and its elements reach the terminal``() : Task =
        task {
            let queued: Source<int> = Source.queue (bounded 4) "orders"

            let graph, seen = queued |> Source.toResult "seen" (collecting ())

            let control = graph.Control<Orleans.Dataflow.IIngressQueue<int>>("orders")

            let! run = host.MaterializeAsync(graph, token ())

            // A control is a run-start value: it is already resolved when the handle is handed over, because
            // nothing could be offered to a run that had to end first.
            let resolving = run |> Run.value control (token ())

            Assert.True resolving.IsCompletedSuccessfully

            let! queue = resolving
            let! first = queue.OfferAsync(10, token ())
            let! second = queue.OfferAsync(20, token ())

            Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, first)
            Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, second)

            queue.Complete()

            do! run.Completion

            let! values = run |> Run.value seen (token ())

            Assert.Equal<int>([ 10; 20 ], values)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``A control asked for under the wrong type is refused by name``() =
        let queued: Source<int> = Source.queue (bounded 4) "orders"

        let graph = queued |> Source.toSink Sink.ignore

        let refused =
            Assert.Throws<System.ArgumentException>(fun () ->
                graph.Control<Orleans.Dataflow.IIngressQueue<string>>("orders") |> ignore)

        // The type check is exact rather than assignable, and the message names both types, so a wrong type
        // argument is a diagnostic here rather than a cast that fails inside a run.
        Assert.Contains("IIngressQueue`1[System.Int32]", refused.Message)

    [<Fact>]
    member _.``The token given to a pipeline-shaped read cancels the wait and not the run``() : Task =
        task {
            let gate = Gate()
            use waiting = new System.Threading.CancellationTokenSource()

            let graph, total =
                Source.ofSeq [ 1; 2 ]
                |> Source.toResult
                    "total"
                    (Sink.aggregate 0L (fun state value ->
                        gate.Wait()
                        state + int64 value))

            let! run = host.MaterializeAsync(graph, token ())
            do! gate.Reached

            let reading = run |> Run.value total waiting.Token

            Assert.False reading.IsCompleted

            waiting.Cancel()

            // The one behavior this wrapper adds beyond argument order is passing the token through, so the
            // token has to reach the wait: a helper that quietly dropped it would leave this hanging until
            // the gate opened, and the test would time out rather than fail.
            do! Assert.ThrowsAnyAsync<System.OperationCanceledException>(fun () -> reading :> Task) :> Task

            // And the run is untouched by the caller's cancellation: it is still held, and a later read of the
            // same slot answers.
            Assert.False run.Completion.IsCompleted

            gate.Open()
            do! run.Completion

            let! sum = run |> Run.value total (token ())

            Assert.Equal(3L, sum)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``Reading a slot of another graph is refused rather than answered``() : Task =
        task {
            let _, foreign =
                Source.ofSeq [ 1; 2 ]
                |> Source.toResult "total" (Sink.aggregate 0L (fun state value -> state + int64 value))

            let graph, _ =
                Source.ofSeq [ 1; 2 ]
                |> Source.toResult "total" (Sink.aggregate 0L (fun state value -> state + int64 value))

            let! run = host.MaterializeAsync(graph, token ())

            // Two graphs of one shape share a fingerprint, so the instance identity is what separates them —
            // and the pipeline-shaped read carries that guard rather than softening it.
            let refused =
                Assert.Throws<System.ArgumentException>(fun () -> run |> Run.value foreign (token ()) |> ignore)

            Assert.Contains("instance", refused.Message)

            do! run.Completion
            do! run.DisposeAsync()
        }
