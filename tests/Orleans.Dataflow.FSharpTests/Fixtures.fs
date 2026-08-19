namespace Orleans.Dataflow.FSharpTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Xunit

/// <summary>What every test in this suite needs: a closing spelling per frontend, and the option values.</summary>
/// <remarks>
/// <para>
/// The C# side of every twin is written fully qualified. Only <c>Orleans.Dataflow.FSharp</c> is opened,
/// because the C# package's <c>Source</c>, <c>Flow</c>, and <c>Sink</c> are the facade's spellings of these
/// very concepts and an <c>open</c> of that namespace would put two of each name in scope — the same rule
/// the authoring modules follow, for the same reason.
/// </para>
/// <para>
/// The option constructors are here rather than inline for one reason: they are the C# package's own
/// records, constructed with F#'s property-initializer syntax, and writing that syntax once is what keeps a
/// test about the operator rather than about the record.
/// </para>
/// </remarks>
module internal Fixtures =

    /// <summary>The running test's own cancellation token.</summary>
    let token () = TestContext.Current.CancellationToken

    /// <summary>The ordinary duration a test's timing operators are configured by.</summary>
    let second = TimeSpan.FromSeconds 1.0

    /// <summary>The smallest amount of time the controlled clock can move.</summary>
    let instant = TimeSpan.FromTicks 1L

    /// <summary>Builds the concurrency bound an asynchronous stage declares.</summary>
    let parallelism (maxConcurrency: int) =
        Orleans.Dataflow.ParallelismOptions(MaxConcurrency = maxConcurrency)

    /// <summary>Builds a bounded buffer's capacity and its default overflow policy.</summary>
    let bounded (capacity: int) = Orleans.Dataflow.BufferOptions(Capacity = capacity)

    /// <summary>Closes an F#-authored source with the sink that keeps nothing.</summary>
    let closeFSharp (source: Source<'T>) : Orleans.Dataflow.RunnableGraph =
        source |> Source.toSink Sink.ignore

    /// <summary>Closes a C#-authored source with the sink that keeps nothing.</summary>
    let closeCSharp (source: Orleans.Dataflow.Source<'T>) : Orleans.Dataflow.RunnableGraph =
        source.To(Orleans.Dataflow.Sink.Ignore<'T>())

    /// <summary>Closes an F#-authored source with a result-bearing sink, under one fixed slot name.</summary>
    let resultFSharp (sink: SinkWithResult<'T, 'Result>) (source: Source<'T>) : Orleans.Dataflow.RunnableGraph =
        let graph, _ = source |> Source.toResult "answer" sink

        graph

    /// <summary>Closes a C#-authored source with a result-bearing sink, under the same fixed slot name.</summary>
    let resultCSharp
        (sink: Orleans.Dataflow.SinkWithResult<'T, 'Result>)
        (source: Orleans.Dataflow.Source<'T>)
        : Orleans.Dataflow.RunnableGraph =
        let struct (graph, _) = source.To(sink, "answer")

        graph

    /// <summary>Asserts that every named pair of graphs is one document, reporting every pair that is not.</summary>
    /// <remarks>
    /// The whole list is walked and the failure names every operator that diverged, rather than only the
    /// first: what a parity suite is asked at a review is "which operators are not equal", and a run that
    /// stops at the first answer makes that question cost one run per operator.
    /// </remarks>
    let assertParity
        (cases: (string * (unit -> Orleans.Dataflow.RunnableGraph) * (unit -> Orleans.Dataflow.RunnableGraph)) list)
        =
        let diverged =
            cases
            |> List.filter (fun (_, fsharpSide, csharpSide) ->
                (fsharpSide ()).Fingerprint <> (csharpSide ()).Fingerprint)
            |> List.map (fun (name, _, _) -> name)

        Assert.Equal<string>(List.empty<string>, diverged)

    /// <summary>The host every behavior test materializes on, which is stateless and holds no run.</summary>
    let host = Orleans.Dataflow.LocalDataflowHost()

    /// <summary>Runs a source to its end on the shared host and answers every element it produced.</summary>
    /// <remarks>
    /// The bounded collecting sink is what makes a behavior assertion about values rather than about counts:
    /// what a stage did is the list it produced, and a count would pass for a stage that produced the wrong
    /// elements the right number of times. The bound is generous and every test here is far below it.
    /// </remarks>
    let elementsOf (source: Source<'T>) : Task<IReadOnlyList<'T>> =
        task {
            let graph, collected =
                source
                |> Source.toResult
                    "collected"
                    (Sink.collect (Orleans.Dataflow.CollectOptions(MaxElements = 256)))

            use! run = host.MaterializeAsync(graph, token ())
            let! values = run.GetValueAsync(collected, token ())

            do! run.Completion

            return values
        }

    /// <summary>Runs a closed graph to its end on the shared host.</summary>
    let runToEnd (graph: Orleans.Dataflow.RunnableGraph) : Task =
        task {
            use! run = host.MaterializeAsync(graph, token ())

            do! run.Completion
        }

    /// <summary>Runs a closed graph to its end and answers the result of one slot.</summary>
    let resultOf (slot: Orleans.Dataflow.ResultSlot<'Result>) (graph: Orleans.Dataflow.RunnableGraph) : Task<'Result> =
        task {
            use! run = host.MaterializeAsync(graph, token ())
            let! value = run.GetValueAsync(slot, token ())

            do! run.Completion

            return value
        }

    /// <summary>Runs a closed graph to its end and answers the results of two of its slots.</summary>
    /// <remarks>
    /// What a junction graph needs and a chain never does: a fan-out declares one result per result-bearing
    /// branch, so the question a test asks is what two branches produced rather than what one terminal did.
    /// Both values are read from the one run, because two runs would be two answers to a question about one.
    /// </remarks>
    let bothResultsOf
        (first: Orleans.Dataflow.ResultSlot<'First>)
        (second: Orleans.Dataflow.ResultSlot<'Second>)
        (graph: Orleans.Dataflow.RunnableGraph)
        : Task<'First * 'Second> =
        task {
            use! run = host.MaterializeAsync(graph, token ())
            let! firstValue = run.GetValueAsync(first, token ())
            let! secondValue = run.GetValueAsync(second, token ())

            do! run.Completion

            return firstValue, secondValue
        }

    /// <summary>The collecting terminal every behavior assertion about a branch's elements is made through.</summary>
    /// <remarks>
    /// A function rather than a value, because a value of this shape would need to be generalized by hand and
    /// says nothing more for it. The bound is generous and every test here is far below it.
    /// </remarks>
    let collecting () : SinkWithResult<'T, IReadOnlyList<'T>> =
        Sink.collect (Orleans.Dataflow.CollectOptions(MaxElements = 256))

    /// <summary>The same collecting terminal, spelled in the other frontend.</summary>
    let collectingCSharp () : Orleans.Dataflow.SinkWithResult<'T, IReadOnlyList<'T>> =
        Orleans.Dataflow.Sink.Collect<'T>(Orleans.Dataflow.CollectOptions(MaxElements = 256))

    /// <summary>Builds an asynchronous sequence over a fixed list of elements.</summary>
    /// <remarks>
    /// A completed unbounded channel is the shortest honest <see cref="T:System.Collections.Generic.IAsyncEnumerable`1"/>
    /// this repository can build without a package that supplies one. It is single-use, so every caller
    /// builds its own.
    /// </remarks>
    let asyncEnumerableOf (values: 'T list) : IAsyncEnumerable<'T> =
        let channel = Channel.CreateUnbounded<'T>()

        for value in values do
            channel.Writer.TryWrite value |> ignore

        channel.Writer.Complete()

        channel.Reader.ReadAllAsync()

    /// <summary>Waits for a condition a run reaches on its own thread.</summary>
    /// <remarks>
    /// A controlled clock makes waiting cheap and nothing else: the segments of a run are real threads, so a
    /// test that has advanced the clock still has to wait for the run to act on it. The poll is short and
    /// the deadline is generous, because what is being waited for is a thread being scheduled rather than
    /// time passing.
    /// </remarks>
    let reaches (what: string) (reached: unit -> bool) (cancellationToken: CancellationToken) : Task =
        task {
            let deadline = DateTime.UtcNow.AddSeconds 30.0

            while not (reached ()) do
                Assert.True(DateTime.UtcNow < deadline, $"The run never reached {what}.")
                do! Task.Delay(TimeSpan.FromMilliseconds 2.0, cancellationToken)
        }

    /// <summary>Waits until the run is holding a given number of timers, then advances the clock.</summary>
    /// <remarks>
    /// The one thing a virtual clock makes a test responsible for: advancing time before the run has reached
    /// its wait would arm that wait after the moment it was waiting for, and the run would then sit there
    /// until the test advanced again — a flake that reads as a hang.
    /// </remarks>
    let advance
        (clock: Orleans.Dataflow.Testing.TestClock)
        (timers: int)
        (delta: TimeSpan)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            do! clock.WaitForTimersAsync(timers, cancellationToken)
            clock.Advance delta
        }
