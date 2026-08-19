namespace Orleans.Dataflow.FSharpTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.FSharpTests.Fixtures
open Xunit

/// <summary>Every operator this frontend adds actually runs, and produces the elements it promises.</summary>
/// <remarks>
/// <para>
/// A fingerprint says two frontends wrote one document; it says nothing about whether the delegate an F#
/// operator stored is a shape the runtime's delegate adapter can read. That is what these tests are for, and
/// it is why every assertion is about values rather than counts: a count passes for a stage that produced the
/// wrong elements the right number of times.
/// </para>
/// <para>
/// Nothing here reads a clock. The operators configured by a duration are asserted where their other bound
/// closes them — a weight, a count — or in the timing suite, which measures on a clock a test moves by hand.
/// Handles are disposed with a trailing <c>DisposeAsync</c> rather than <c>use</c>, because the task
/// expression's <c>use</c> does not accept a type that is only <c>IAsyncDisposable</c>.
/// </para>
/// </remarks>
type OperatorBehaviorTests() =

    [<Fact>]
    member _.``choose transforms and drops in one pass``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3; 4; 5 ]
                |> Source.choose (fun value -> if value % 2 = 0 then ValueSome(value * 10) else ValueNone)
                |> elementsOf

            Assert.Equal<int>([ 20; 40 ], observed)
        }

    [<Fact>]
    member _.``chooseOption transforms and drops in one pass``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3; 4; 5 ]
                |> Source.chooseOption (fun value -> if value % 2 = 0 then Some(value * 10) else None)
                |> elementsOf

            Assert.Equal<int>([ 20; 40 ], observed)
        }

    [<Fact>]
    member _.``collect flattens each element's sequence in order``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.collect (fun value -> seq { value; value * 10 })
                |> elementsOf

            Assert.Equal<int>([ 1; 10; 2; 20 ], observed)
        }

    [<Fact>]
    member _.``mergeMap emits every inner element, each inner sequence in its own order``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.mergeMap (parallelism 2) (fun value -> seq { value; value * 10 })
                |> elementsOf

            // Emission is unordered across inner sequences, so the claim is the multiset.
            Assert.Equal<int>([ 1; 2; 10; 20 ], observed |> Seq.sort)
        }

    [<Fact>]
    member _.``mergeMapAsyncEnumerable emits every inner element``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.mergeMapAsyncEnumerable (parallelism 2) (fun value ->
                    asyncEnumerableOf [ value; value * 10 ])
                |> elementsOf

            Assert.Equal<int>([ 1; 2; 10; 20 ], observed |> Seq.sort)
        }

    [<Fact>]
    member _.``scan emits one running state per element and never the seed``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.scan 100 (fun state value -> state + value)
                |> elementsOf

            Assert.Equal<int>([ 101; 103; 106 ], observed)
        }

    [<Fact>]
    member _.``scanDurable emits the same states as an ordinary scan``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.scanDurable
                    100
                    (fun state value -> state + value)
                    (fun state -> Orleans.Dataflow.Serialization.CanonicalJsonValue.Parse(string state))
                    (fun value -> value.ToElement().GetInt32())
                |> elementsOf

            Assert.Equal<int>([ 101; 103; 106 ], observed)
        }

    [<Fact>]
    member _.``scanTask folds through a task-returning function``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.scanTask 0 (fun state value _ -> Task.FromResult(state + value))
                |> elementsOf

            Assert.Equal<int>([ 1; 3; 6 ], observed)
        }

    [<Fact>]
    member _.``scanAsync folds through an asynchronous computation``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.scanAsync 0 (fun state value -> async { return state + value })
                |> elementsOf

            Assert.Equal<int>([ 1; 3; 6 ], observed)
        }

    [<Fact>]
    member _.``mapTask transforms every element and preserves input order``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.mapTask (parallelism 4) (fun value _ -> Task.FromResult(value * 10))
                |> elementsOf

            Assert.Equal<int>([ 10; 20; 30 ], observed)
        }

    [<Fact>]
    member _.``mapAsync runs the author's computation with the run's own token``() : Task =
        task {
            let observedTokens = ResizeArray<bool>()

            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.mapAsync (parallelism 4) (fun value ->
                    async {
                        // The run's token starts the computation, so the workflow's own token is the run's
                        // and is cancellable rather than CancellationToken.None.
                        let! runToken = Async.CancellationToken
                        observedTokens.Add runToken.CanBeCanceled

                        return value * 10
                    })
                |> elementsOf

            Assert.Equal<int>([ 10; 20; 30 ], observed)
            Assert.Equal<bool>([ true; true; true ], observedTokens)
        }

    [<Fact>]
    member _.``an asynchronous computation sees the very token a task-shaped callback is handed``() : Task =
        task {
            let fromCallback = ResizeArray<CancellationToken>()
            let fromComputation = ResizeArray<CancellationToken>()

            let! observed =
                Source.ofSeq [ 1 ]
                |> Source.mapTask (parallelism 1) (fun value callbackToken ->
                    fromCallback.Add callbackToken
                    Task.FromResult value)
                |> Source.mapAsync (parallelism 1) (fun value ->
                    async {
                        let! workflowToken = Async.CancellationToken
                        fromComputation.Add workflowToken

                        return value
                    })
                |> elementsOf

            Assert.Equal<int>([ 1 ], observed)

            // The adaptation is what this pins: the token the stage hands over is the token the workflow is
            // started with, so Async.CancellationToken inside an author's computation is the run's own and
            // not a fresh one. Both stages of one run read the same token, which is what makes the
            // comparison meaningful rather than a coincidence of shape.
            Assert.True(
                fromCallback[0] = fromComputation[0],
                "The workflow was started with a token other than the one the stage handed over.")
        }

    [<Fact>]
    member _.``a parked mapAsync computation observes the cancellation of the run that started it``() : Task =
        task {
            let entered = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let parked = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)

            let graph =
                Source.ofSeq [ 1 ]
                |> Source.mapAsync (parallelism 1) (fun value ->
                    async {
                        // Async.OnCancel rather than a `with` clause, and the difference is the whole point:
                        // cancellation in an F# workflow travels on a continuation of its own that no
                        // exception handler sees, so a try/with here would pass by never running. A
                        // registration on the workflow's own token fires when that token is cancelled, and
                        // firing is the observation this test is about.
                        let! registration = Async.OnCancel(fun () -> observed.TrySetResult() |> ignore)
                        use _ = registration

                        entered.TrySetResult() |> ignore

                        let! held = Async.AwaitTask parked.Task

                        return value + held
                    })
                |> Source.toSink Sink.ignore

            let! run = host.MaterializeAsync(graph, token ())

            // The computation is inside its own await with nothing to complete it, which is the one state a
            // test can cancel a workflow in and know that what it observed was observed while parked.
            do! entered.Task

            let disposing = run.DisposeAsync().AsTask()

            // The two tests above prove the run's token reaches the author's computation; this proves the
            // computation is actually told when that token is cancelled. A stage that started the workflow
            // beside the token rather than with it would leave this waiting forever.
            do! observed.Task

            // Released only now, and only so that nothing is left holding a continuation on a task that
            // would never complete. The observation above had already happened, and it happened while the
            // computation was still parked on this very task.
            parked.TrySetResult 0 |> ignore

            do! disposing

            do! Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> run.Completion) :> Task
        }

    [<Fact>]
    member _.``mapValueTask transforms every element and preserves input order``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.mapValueTask (parallelism 4) (fun value _ -> ValueTask<int>(value * 10))
                |> elementsOf

            Assert.Equal<int>([ 10; 20; 30 ], observed)
        }

    [<Fact>]
    member _.``the unordered mapping families emit every element``() : Task =
        task {
            let! throughTask =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.mapTaskUnordered (parallelism 3) (fun value _ -> Task.FromResult(value * 10))
                |> elementsOf

            let! throughValueTask =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.mapValueTaskUnordered (parallelism 3) (fun value _ -> ValueTask<int>(value * 10))
                |> elementsOf

            let! throughAsync =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.mapAsyncUnordered (parallelism 3) (fun value -> async { return value * 10 })
                |> elementsOf

            // The output order is the order the callbacks completed in, so the claim is the multiset.
            Assert.Equal<int>([ 10; 20; 30 ], throughTask |> Seq.sort)
            Assert.Equal<int>([ 10; 20; 30 ], throughValueTask |> Seq.sort)
            Assert.Equal<int>([ 10; 20; 30 ], throughAsync |> Seq.sort)
        }

    [<Fact>]
    member _.``take and skip bound a stream from either end``() : Task =
        task {
            let! taken = Source.ofSeq [ 1; 2; 3; 4; 5 ] |> Source.take 2 |> elementsOf
            let! skipped = Source.ofSeq [ 1; 2; 3; 4; 5 ] |> Source.skip 3 |> elementsOf

            Assert.Equal<int>([ 1; 2 ], taken)
            Assert.Equal<int>([ 4; 5 ], skipped)
        }

    [<Fact>]
    member _.``takeWhile is exclusive and takeThrough is inclusive of the element that ends the stream``() : Task =
        task {
            let! exclusive = Source.ofSeq [ 1; 2; 3; 4 ] |> Source.takeWhile (fun value -> value < 3) |> elementsOf
            let! inclusive = Source.ofSeq [ 1; 2; 3; 4 ] |> Source.takeThrough (fun value -> value < 3) |> elementsOf

            // One predicate, two operators, and the difference is exactly the element that ended the stream:
            // the exclusive spelling drops the 3 and the inclusive one delivers it.
            Assert.Equal<int>([ 1; 2 ], exclusive)
            Assert.Equal<int>([ 1; 2; 3 ], inclusive)
        }

    [<Fact>]
    member _.``skipWhile stops dropping at the first element the predicate rejects``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3; 1; 2 ]
                |> Source.skipWhile (fun value -> value < 3)
                |> elementsOf

            // Everything after the first rejection passes, whether or not the predicate would accept it again.
            Assert.Equal<int>([ 3; 1; 2 ], observed)
        }

    [<Fact>]
    member _.``distinct remembers every element and deduplicateConsecutive remembers one``() : Task =
        task {
            let! remembered =
                Source.ofSeq [ 1; 2; 1; 3; 2 ]
                |> Source.distinct (Orleans.Dataflow.DistinctOptions(MaxTrackedKeys = 8))
                |> elementsOf

            let! adjacent =
                Source.ofSeq [ 1; 1; 2; 2; 1 ]
                |> Source.deduplicateConsecutive
                |> elementsOf

            Assert.Equal<int>([ 1; 2; 3 ], remembered)
            Assert.Equal<int>([ 1; 2; 1 ], adjacent)
        }

    [<Fact>]
    member _.``grouped fills groups and emits a partial last one``() : Task =
        task {
            let! observed = Source.ofSeq [ 1; 2; 3; 4; 5 ] |> Source.grouped 2 |> elementsOf

            Assert.Equal<int list>(
                [ [ 1; 2 ]; [ 3; 4 ]; [ 5 ] ],
                observed |> Seq.map List.ofSeq |> List.ofSeq)
        }

    [<Fact>]
    member _.``batching answers a typed list for a reference element type too``() : Task =
        task {
            // The group projection is the one piece of the C# facade this package re-states rather than
            // calls, and it unboxes into the element type the author declared. A value type proves the
            // unboxing; a reference type proves the cast, and the two are different instructions.
            let! observed = Source.ofSeq [ "a"; "b"; "c" ] |> Source.grouped 2 |> elementsOf

            Assert.Equal<string list>(
                [ [ "a"; "b" ]; [ "c" ] ],
                observed |> Seq.map List.ofSeq |> List.ofSeq)
        }

    [<Fact>]
    member _.``sliding overlaps its windows by the difference of size and step``() : Task =
        task {
            let! observed = Source.ofSeq [ 1; 2; 3; 4 ] |> Source.sliding 2 1 |> elementsOf

            Assert.Equal<int list>(
                [ [ 1; 2 ]; [ 2; 3 ]; [ 3; 4 ] ],
                observed |> Seq.map List.ofSeq |> List.ofSeq)
        }

    [<Fact>]
    member _.``groupedWeightedWithin closes a group before the element that would break its weight``() : Task =
        task {
            let observedCosts = ResizeArray<int>()

            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                // A window an hour long and a count bound of ten: the weight is the only bound that closes
                // anything here, so no clock has to move for this to be deterministic.
                |> Source.groupedWeightedWithin 10 3 (TimeSpan.FromHours 1.0) (fun value ->
                    observedCosts.Add value
                    value)
                |> elementsOf

            Assert.Equal<int list>([ [ 1; 2 ]; [ 3 ] ], observed |> Seq.map List.ofSeq |> List.ofSeq)
            Assert.Equal<int>([ 1; 2; 3 ], observedCosts)
        }

    [<Fact>]
    member _.``groupBy gives every key its own instance of the group flow``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3; 4 ]
                |> Source.groupBy
                    (Orleans.Dataflow.GroupByOptions(MaxActiveKeys = 4))
                    (fun value -> value % 2)
                    (Flow.scan 0 (fun state value -> state + value))
                |> elementsOf

            // Odds fold 1 then 3 into 1 and 4; evens fold 2 then 4 into 2 and 6. Every key keeps its own
            // running state, and emission is merged in the order the elements arrived.
            Assert.Equal<int>([ 1; 2; 4; 6 ], observed)
        }

    [<Fact>]
    member _.``buffer passes every element through unchanged``() : Task =
        task {
            let! observed = Source.ofSeq [ 1; 2; 3 ] |> Source.buffer (bounded 2) |> elementsOf

            Assert.Equal<int>([ 1; 2; 3 ], observed)
        }

    [<Fact>]
    member _.``throttleBy charges the rate by what the cost function answers``() : Task =
        task {
            let observedCosts = ResizeArray<int>()

            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                // A rate far above what three elements need, so the bucket never empties and nothing waits.
                |> Source.throttleBy
                    (Orleans.Dataflow.ThrottleOptions(Elements = 1_000_000, Per = second))
                    (fun value ->
                        observedCosts.Add value
                        1)
                |> elementsOf

            Assert.Equal<int>([ 1; 2; 3 ], observed)
            Assert.Equal<int>([ 1; 2; 3 ], observedCosts)
        }

    [<Fact>]
    member _.``throttle passes a stream that is already below its rate``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.throttle (Orleans.Dataflow.ThrottleOptions(Elements = 1_000_000, Per = second))
                |> elementsOf

            Assert.Equal<int>([ 1; 2; 3 ], observed)
        }

    [<Fact>]
    member _.``a valve that starts closed holds the stream until the run's control opens it``() : Task =
        task {
            let observed = ResizeArray<int>()

            let graph =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.valve "gate" Orleans.Dataflow.ValveMode.Closed
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())

            let! gate = run.GetValueAsync(graph.Control<Orleans.Dataflow.IValve>("gate"), token ())

            Assert.False gate.IsOpen

            gate.Open()

            do! run.Completion

            Assert.Equal<int>([ 1; 2; 3 ], observed)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``a resuming supervision scope drops the failing element and keeps the run``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.supervised
                    (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Resume))
                    (Flow.map (fun value ->
                        if value = 2 then
                            failwith "the mapping refuses the second element"
                        else
                            value * 10))
                |> elementsOf

            Assert.Equal<int>([ 10; 30 ], observed)
        }

    [<Fact>]
    member _.``a recovering supervision scope emits its fallback and ends its stream``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.supervisedRecovering
                    (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Recover))
                    -1
                    (Flow.map (fun value ->
                        if value = 2 then
                            failwith "the mapping refuses the second element"
                        else
                            value * 10))
                |> elementsOf

            Assert.Equal<int>([ 10; -1 ], observed)
        }

    [<Fact>]
    member _.``a durable scope is transparent to the stream it holds``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.durable (Flow.map (fun value -> value * 10))
                |> elementsOf

            Assert.Equal<int>([ 10; 20; 30 ], observed)
        }

    [<Fact>]
    member _.``the asynchronous sinks receive every element``() : Task =
        task {
            let throughTask = ResizeArray<int>()
            let throughAsync = ResizeArray<int>()

            do!
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.toSink (
                    Sink.forEachTask (parallelism 1) (fun value _ ->
                        throughTask.Add value
                        Task.CompletedTask))
                |> runToEnd

            do!
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.toSink (
                    Sink.forEachAsync (parallelism 1) (fun value -> async { throughAsync.Add value }))
                |> runToEnd

            Assert.Equal<int>([ 1; 2; 3 ], throughTask)
            Assert.Equal<int>([ 1; 2; 3 ], throughAsync)
        }

    [<Fact>]
    member _.``the asynchronous folding sinks resolve the folded state``() : Task =
        task {
            let taskGraph, taskTotal =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.toResult
                    "total"
                    (Sink.aggregateTask 0 (fun state value _ -> Task.FromResult(state + value)))

            let asyncGraph, asyncTotal =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.toResult "total" (Sink.aggregateAsync 0 (fun state value -> async { return state + value }))

            let! throughTask = taskGraph |> resultOf taskTotal
            let! throughAsync = asyncGraph |> resultOf asyncTotal

            Assert.Equal(6, throughTask)
            Assert.Equal(6, throughAsync)
        }

    [<Fact>]
    member _.``the positional sinks resolve the element they name``() : Task =
        task {
            let firstGraph, firstValue = Source.ofSeq [ 7; 8; 9 ] |> Source.toResult "answer" Sink.first
            let lastGraph, lastValue = Source.ofSeq [ 7; 8; 9 ] |> Source.toResult "answer" Sink.last
            let countGraph, counted = Source.ofSeq [ 7; 8; 9 ] |> Source.toResult "answer" Sink.count

            let! observedFirst = firstGraph |> resultOf firstValue
            let! observedLast = lastGraph |> resultOf lastValue
            let! observedCount = countGraph |> resultOf counted

            Assert.Equal(7, observedFirst)
            Assert.Equal(9, observedLast)
            Assert.Equal(3L, observedCount)
        }

    [<Fact>]
    member _.``the default-valued sinks resolve the element type's default over an empty stream``() : Task =
        task {
            let firstGraph, firstValue =
                Source.empty<string> |> Source.toResult "answer" Sink.firstOrDefault

            let lastGraph, lastValue = Source.empty<string> |> Source.toResult "answer" Sink.lastOrDefault

            let! observedFirst = firstGraph |> resultOf firstValue
            let! observedLast = lastGraph |> resultOf lastValue

            // The honest consequence of mirroring the C# vocabulary: an empty stream resolves the default,
            // which for a reference type is null. Sink.first is the spelling that refuses an empty stream.
            Assert.Null observedFirst
            Assert.Null observedLast
        }

    [<Fact>]
    member _.``the collecting sink resolves a snapshot of every element``() : Task =
        task {
            let graph, collected =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.toResult "answer" (Sink.collect (Orleans.Dataflow.CollectOptions(MaxElements = 8)))

            let! observed = graph |> resultOf collected

            Assert.Equal<int>([ 1; 2; 3 ], observed)
        }

    [<Fact>]
    member _.``the channel sink writes every element into the author's channel``() : Task =
        task {
            let channel = Channel.CreateUnbounded<int>()

            do! Source.ofSeq [ 1; 2; 3 ] |> Source.toSink (Sink.toChannel channel.Writer) |> runToEnd

            // The run completes the writer itself when it ends, so nothing here has to — and a second
            // completion would throw.
            Assert.False(channel.Writer.TryWrite 4)

            let observed = ResizeArray<int>()

            let mutable pending = true

            while pending do
                match channel.Reader.TryRead() with
                | true, value -> observed.Add value
                | false, _ -> pending <- false

            Assert.Equal<int>([ 1; 2; 3 ], observed)
        }

    [<Fact>]
    member _.``the finite constructors emit exactly the elements they declare``() : Task =
        task {
            let! ofEmpty = Source.empty<int> |> elementsOf
            let! ofSingle = Source.single 7 |> elementsOf
            let! ofRepeat = Source.repeat 3 7 |> elementsOf
            let! ofRange = Source.range 5 4 |> elementsOf
            let! ofCycle = Source.cycle [ 1; 2 ] |> Source.take 5 |> elementsOf
            let! ofNever = Source.never<int> |> Source.take 0 |> elementsOf

            Assert.Equal<int>([], ofEmpty)
            Assert.Equal<int>([ 7 ], ofSingle)
            Assert.Equal<int>([ 7; 7; 7 ], ofRepeat)
            Assert.Equal<int>([ 5; 6; 7; 8 ], ofRange)
            Assert.Equal<int>([ 1; 2; 1; 2; 1 ], ofCycle)
            Assert.Equal<int>([], ofNever)
        }

    [<Fact>]
    member _.``a task source replays its value and a cold source is started per run``() : Task =
        task {
            let! fromTask = Source.ofTask (Task.FromResult 7) |> elementsOf

            let mutable started = 0

            let cold =
                Source.ofAsync (
                    async {
                        started <- started + 1
                        return started
                    })

            let! first = cold |> elementsOf
            let! second = cold |> elementsOf

            Assert.Equal<int>([ 7 ], fromTask)

            // An Async is cold, so it is started once per run rather than shared between runs — which is
            // exactly what separates Source.ofAsync from Source.ofTask.
            Assert.Equal<int>([ 1 ], first)
            Assert.Equal<int>([ 2 ], second)
        }

    [<Fact>]
    member _.``the factory sources compute their element once per run``() : Task =
        task {
            let mutable synchronous = 0
            let mutable asynchronous = 0

            let fromFactory =
                Source.ofFactory (fun () ->
                    synchronous <- synchronous + 1
                    synchronous)

            let fromTaskFactory =
                Source.ofTaskFactory (fun _ ->
                    asynchronous <- asynchronous + 1
                    Task.FromResult asynchronous)

            let! firstSynchronous = fromFactory |> elementsOf
            let! secondSynchronous = fromFactory |> elementsOf
            let! firstAsynchronous = fromTaskFactory |> elementsOf
            let! secondAsynchronous = fromTaskFactory |> elementsOf

            Assert.Equal<int>([ 1 ], firstSynchronous)
            Assert.Equal<int>([ 2 ], secondSynchronous)
            Assert.Equal<int>([ 1 ], firstAsynchronous)
            Assert.Equal<int>([ 2 ], secondAsynchronous)
        }

    [<Fact>]
    member _.``a failing source faults the run with the very exception it was given``() : Task =
        task {
            let failure = InvalidOperationException "the source refuses to produce"

            let graph = (Source.failed failure: Source<int>) |> Source.toSink Sink.ignore

            let! run = host.MaterializeAsync(graph, token ())
            let! thrown = Assert.ThrowsAsync<InvalidOperationException>(fun () -> run.Completion)

            Assert.Same(failure, thrown)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``the unfolding sources produce their elements from the state they carry``() : Task =
        task {
            let step state =
                if state > 3 then ValueNone else ValueSome(state * 10, state + 1)

            let! synchronous = Source.unfold step 1 |> elementsOf

            let! throughTask =
                Source.unfoldTask
                    (fun state _ ->
                        Task.FromResult(if state > 3 then None else Some(state * 10, state + 1)))
                    1
                |> elementsOf

            let! throughAsync =
                Source.unfoldAsync
                    (fun state -> async { return (if state > 3 then None else Some(state * 10, state + 1)) })
                    1
                |> elementsOf

            Assert.Equal<int>([ 10; 20; 30 ], synchronous)
            Assert.Equal<int>([ 10; 20; 30 ], throughTask)
            Assert.Equal<int>([ 10; 20; 30 ], throughAsync)
        }

    [<Fact>]
    member _.``the sequence sources drain what they were handed``() : Task =
        task {
            let! fromAsyncEnumerable = Source.ofAsyncEnumerable (asyncEnumerableOf [ 1; 2; 3 ]) |> elementsOf

            let channel = Channel.CreateUnbounded<int>()

            for value in [ 4; 5; 6 ] do
                Assert.True(channel.Writer.TryWrite value)

            channel.Writer.Complete()

            let! fromChannel = Source.ofChannel channel.Reader |> elementsOf

            Assert.Equal<int>([ 1; 2; 3 ], fromAsyncEnumerable)
            Assert.Equal<int>([ 4; 5; 6 ], fromChannel)
        }

    [<Fact>]
    member _.``a queue source emits what its run's control was offered``() : Task =
        task {
            let observed = ResizeArray<int>()

            let graph =
                (Source.queue (bounded 8) "ingress": Source<int>)
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())

            let! ingress =
                run.GetValueAsync(graph.Control<Orleans.Dataflow.IIngressQueue<int>>("ingress"), token ())

            for value in [ 1; 2; 3 ] do
                let! outcome = ingress.OfferAsync(value, token ())
                Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, outcome)

            ingress.Complete()

            do! run.Completion

            Assert.Equal<int>([ 1; 2; 3 ], observed)

            do! run.DisposeAsync()
        }
