namespace Orleans.Dataflow.FSharpTests

open System
open System.Collections.Generic
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.FSharpTests.Fixtures
open Xunit

/// <summary>
/// What the F# spellings of the clock-reading operators promise, measured on a clock a test moves by hand.
/// </summary>
/// <remarks>
/// <para>
/// The clock is the host's and is resolved at materialization, so nothing is threaded through an authoring
/// call and these tests differ from the C# suite's in exactly one respect: which frontend wrote the graph.
/// A host per test rather than one shared instance, because the clock is the host's — two tests sharing one
/// would share the moment as well.
/// </para>
/// <para>
/// Nothing here sleeps. Where a stage arms a timer, the arming is awaited before the clock moves: advancing
/// time before the run has reached its wait would arm that wait after the moment it was waiting for, and
/// the run would sit there until the test advanced again — a flake that reads as a hang. Where a stage arms
/// none, the ordering is the test thread's own: an element cannot arrive before it is offered, so a clock
/// moved before the offer has moved before the arrival.
/// </para>
/// </remarks>
type TimingBehaviorTests() =

    static let timed () =
        let clock = Orleans.Dataflow.Testing.TestClock()

        Orleans.Dataflow.LocalDataflowHost(clock), clock

    static let ingressOf (graph: Orleans.Dataflow.RunnableGraph) (run: Orleans.Dataflow.RunHandle) =
        run.GetValueAsync(graph.Control<Orleans.Dataflow.IIngressQueue<int>>("in"), token ())

    [<Fact>]
    member _.``groupedWithin emits a group when its window closes with nothing arriving``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<IReadOnlyList<int>>()

            let graph =
                (Source.queue (bounded 4) "in": Source<int>)
                |> Source.groupedWithin 10 second
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())
            let! ingress = ingressOf graph run

            let! outcome = ingress.OfferAsync(1, token ())
            Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, outcome)

            // One element and a bound of ten: nothing a count could close, so the window is what closes it.
            // Acceptance into the ingress says nothing about arrival at the stage, so the fact awaited
            // before the clock moves is the arming of the window itself.
            do! advance clock 1 second (token ())
            do! reaches "the window closing" (fun () -> observed.Count = 1) (token ())

            Assert.Equal<int>([ 1 ], observed[0])

            ingress.Complete()
            do! run.Completion
            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``delay holds every element for its own duration``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<int>()

            let graph =
                (Source.queue (bounded 4) "in": Source<int>)
                |> Source.delay second (bounded 4)
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())
            let! ingress = ingressOf graph run

            let! outcome = ingress.OfferAsync(7, token ())
            Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, outcome)

            do! advance clock 1 second (token ())
            do! reaches "the delayed element" (fun () -> observed.Count = 1) (token ())

            Assert.Equal<int>([ 7 ], observed)

            ingress.Complete()
            do! run.Completion
            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``initialDelay holds the whole stream until its moment has passed``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<int>()

            let graph =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.initialDelay second
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())

            // The armed timer is the run having reached its wait, so what is asserted at this moment is a
            // fact rather than a race: the stream that would otherwise have finished at once has not
            // delivered anything and the run has not ended.
            do! clock.WaitForTimersAsync(1, token ())

            Assert.Empty observed
            Assert.False run.Completion.IsCompleted

            clock.Advance second

            do! run.Completion

            Assert.Equal<int>([ 1; 2; 3 ], observed)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``timeout faults the run when nothing arrives inside its gap``() : Task =
        task {
            let host, clock = timed ()

            let graph =
                (Source.queue (bounded 4) "in": Source<int>)
                |> Source.timeout second
                |> Source.toSink Sink.ignore

            let! run = host.MaterializeAsync(graph, token ())

            // The gap before the first element is counted from the moment the run started, so a stream that
            // never produces anything at all fails rather than hanging.
            do! advance clock 1 second (token ())

            let! thrown =
                Assert.ThrowsAsync<Orleans.Dataflow.StreamTimeoutException>(fun () -> run.Completion)

            Assert.NotNull thrown

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``takeWithin keeps what arrived before its deadline and ends the stream at it``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<int64>()

            let graph =
                Source.tick second second
                |> Source.takeWithin (TimeSpan.FromMilliseconds 2500.0)
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())

            // Both timers before every advance: the window's, armed when the run starts, and the tick
            // source's, armed at its first pull and re-armed after every tick. Advancing while the source
            // has not yet armed its next wait would move time past a tick it had not asked for, and a
            // missed tick is a skipped tick — which is the source's contract and would be this test's flake.
            for tick in 1..2 do
                do! advance clock 2 second (token ())
                do! reaches $"tick {tick - 1} reaching the sink" (fun () -> observed.Count = tick) (token ())

            do! advance clock 2 second (token ())

            do! run.Completion

            // Ticks zero and one are inside the window and tick two is not: it arrives half a second past
            // the deadline and ends the stream instead of being emitted. The run ends successfully, the way
            // reaching a count bound ends one.
            Assert.Equal<int64>([ 0L; 1L ], observed)
            Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``takeWithin ends a silent stream, and the source above it learns when it next wakes``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<int64>()

            let graph =
                // The source's first tick is far past the window, so nothing ever arrives at the stage:
                // what ends this stream is its own timer and not an arrival. The buffer is load-bearing —
                // it puts the window in a segment of its own, so that its wait really is a wait for an
                // element; fused into the segment above it the stage could only act while that segment was
                // running, and a stage parked inside a source's pull is a stage that is parked.
                Source.tick (second * 10.0) (second * 10.0)
                |> Source.buffer (bounded 1)
                |> Source.takeWithin (second * 3.0)
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())

            // Both timers before the advance: the window's, armed when the run starts, and the tick
            // source's, armed at its first pull. Moving time before the source has armed would leave the
            // test unable to say when its next tick is due.
            do! advance clock 2 (second * 3.0) (token ())

            // The stream at that stage has ended and the source above it has not learned yet: it is asleep
            // in a wait of this runtime's own, which a completion below does not release. It learns at its
            // next tick, when the boundary it offers into refuses the element — so a run whose source is
            // parked outlives the deadline that ended its stream, and a test that awaited completion here
            // would hang rather than fail.
            Assert.False run.Completion.IsCompleted

            clock.Advance(second * 7.0)

            do! run.Completion

            Assert.Empty observed
            Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``skipWithin drops every element that arrives inside its window``() : Task =
        task {
            let host, _ = timed ()
            let observed = ResizeArray<int>()

            let graph =
                (Source.queue (bounded 4) "in": Source<int>)
                |> Source.skipWithin second
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())
            let! ingress = ingressOf graph run

            // The clock never moves in this test, so whenever these elements reach the stage they are inside
            // the window — there is no moment at which they could be anything else.
            for value in [ 1; 2; 3 ] do
                let! outcome = ingress.OfferAsync(value, token ())
                Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, outcome)

            ingress.Complete()
            do! run.Completion

            Assert.Empty observed

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``skipWithin passes every element that arrives after its window``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<int>()

            let graph =
                (Source.queue (bounded 4) "in": Source<int>)
                |> Source.skipWithin second
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())
            let! ingress = ingressOf graph run

            // The stage arms no timer — it has an answer for every element the moment it arrives — so the
            // ordering that makes this deterministic is the test thread's own: nothing has been offered yet,
            // so nothing can have arrived before the clock moved past the window.
            clock.Advance second

            for value in [ 1; 2; 3 ] do
                let! outcome = ingress.OfferAsync(value, token ())
                Assert.Equal(Orleans.Dataflow.QueueOfferOutcome.Accepted, outcome)

            ingress.Complete()
            do! run.Completion

            Assert.Equal<int>([ 1; 2; 3 ], observed)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``tick emits its numbers from zero, at its interval, after its initial delay``() : Task =
        task {
            let host, clock = timed ()
            let observed = ResizeArray<int64>()

            let graph =
                Source.tick second second
                |> Source.take 3
                |> Source.toSink (Sink.forEach observed.Add)

            let! run = host.MaterializeAsync(graph, token ())

            do! advance clock 1 second (token ())
            do! reaches "the first tick" (fun () -> observed.Count = 1) (token ())
            do! advance clock 1 second (token ())
            do! reaches "the second tick" (fun () -> observed.Count = 2) (token ())
            do! advance clock 1 second (token ())

            do! run.Completion

            // The first element is the first tick and not a count of the ticks so far.
            Assert.Equal<int64>([ 0L; 1L; 2L ], observed)

            do! run.DisposeAsync()
        }
