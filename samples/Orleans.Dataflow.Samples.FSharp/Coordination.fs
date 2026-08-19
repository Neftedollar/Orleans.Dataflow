namespace Orleans.Dataflow.Samples

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// <summary>A place one thread waits until another lets it through.</summary>
/// <remarks>
/// <para>
/// Three of the scenarios are about <em>when</em> something happens rather than about what it computes, and
/// a sample that demonstrated those by sleeping would be a sample that fails on a loaded machine. A gate
/// turns "the sink is slower than the source" into a fact the run cannot get wrong: the sink stops dead
/// until the runner has seen the source run out, and only then is it let go.
/// </para>
/// <para>
/// Blocking is the point rather than an oversight. A sink's fold runs on the run's own thread, so blocking
/// there is exactly what a slow consumer looks like to the stage above it.
/// </para>
/// </remarks>
[<Sealed>]
type Gate() =
    let opened = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let reached = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    /// <summary>Gets a task that completes the first time anything waits at this gate.</summary>
    /// <remarks>
    /// The other half of the interlock. A source that ran ahead before the sink had taken anything at all
    /// would be measuring a race rather than a policy, so the feed holds after its first order until this
    /// says the sink is standing here.
    /// </remarks>
    member _.Reached: Task = reached.Task

    /// <summary>Blocks the calling thread until the gate is opened, and returns at once afterwards.</summary>
    member _.Wait() =
        reached.TrySetResult() |> ignore

        opened.Task.GetAwaiter().GetResult()

    /// <summary>Lets everything through, now and from now on.</summary>
    member _.Open() = opened.TrySetResult() |> ignore

/// <summary>A feed that hands over one order, waits for the sink, and then runs ahead of it.</summary>
/// <typeparam name="T">What the feed carries.</typeparam>
/// <remarks>
/// <para>
/// What the backpressure scenario needs and a plain list cannot give, in two parts. The feed holds after its
/// first element until the sink is standing at its gate, so the sink is provably holding that element and
/// the buffer below is provably empty; then it runs the rest of the sequence out as fast as the buffer will
/// take it, and announces the moment the run asked it for an element it did not have. Without the first
/// part the sink might have taken nothing at all and the kept set would be a race; without the second the
/// runner could not know that every element had been offered.
/// </para>
/// <para>
/// The announcement happens on the pull that finds nothing left rather than on the last element, because
/// those are different moments and only the second one means "the source is finished".
/// </para>
/// </remarks>
[<Sealed>]
type PacedFeed<'T>(elements: IReadOnlyList<'T>, sink: Gate) =
    let exhausted = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    /// <summary>Gets a task that completes the first time the feed is read past its last element.</summary>
    member _.Exhausted: Task = exhausted.Task

    /// <summary>Gets the sequence a source is built over.</summary>
    /// <remarks>
    /// A fresh enumeration per materialization, exactly as any sequence handed to a source is, so a graph
    /// built over this feed may be run more than once.
    /// </remarks>
    member _.Elements: seq<'T> =
        seq {
            let mutable handedOver = 0

            for element in elements do
                yield element

                handedOver <- handedOver + 1

                if handedOver = 1 then
                    sink.Reached.GetAwaiter().GetResult()

            exhausted.TrySetResult() |> ignore
        }

/// <summary>Counts how many invocations of an asynchronous stage are inside it at once.</summary>
/// <remarks>
/// <para>
/// The evidence the asynchronous scenario exists to produce. Every invocation reports itself and then waits
/// until the declared number of them are waiting together; the last arrival releases all of them, and every
/// later invocation passes straight through. So the peak this records is exactly the bound the graph
/// declared — it cannot be lower, because nothing is released until that many have arrived, and it cannot
/// be higher, because the runtime admits no more.
/// </para>
/// <para>
/// A run whose declared bound the runtime did not honor would therefore hang rather than print a wrong
/// number, which is why the scenario passes the run's own cancellation token in: the wait ends when the
/// run's budget does.
/// </para>
/// </remarks>
[<Sealed>]
type Concurrency(declared: int) =
    let padlock = obj ()
    let reached = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable inFlight = 0
    let mutable peak = 0

    /// <summary>Gets the bound the graph declared.</summary>
    member _.Declared = declared

    /// <summary>Gets the greatest number of invocations seen inside the stage at one time.</summary>
    member _.Peak =
        Monitor.Enter padlock

        try
            peak
        finally
            Monitor.Exit padlock

    /// <summary>Enters the stage, and waits there until the declared number of invocations have.</summary>
    /// <param name="cancellationToken">The run's own token, which ends the wait if the run does.</param>
    /// <returns>A task that completes once the declared number of invocations are in flight.</returns>
    member _.EnterAsync(cancellationToken: CancellationToken) : Task =
        task {
            Monitor.Enter padlock

            try
                inFlight <- inFlight + 1

                if inFlight > peak then
                    peak <- inFlight

                if inFlight >= declared then
                    reached.TrySetResult() |> ignore
            finally
                Monitor.Exit padlock

            do! reached.Task.WaitAsync cancellationToken

            Monitor.Enter padlock

            try
                inFlight <- inFlight - 1
            finally
                Monitor.Exit padlock
        }

/// <summary>A tally that completes once a declared number of things have announced themselves.</summary>
/// <remarks>
/// <para>
/// How the asynchronous scenario turns the difference between an ordered and an unordered mapping into a
/// fact rather than a race. The first order's work waits until the rest of its concurrent batch has gone
/// past, so an ordered mapping emits the first order first regardless — ordering is about emission and not
/// about completion — and an unordered one emits it after them. Neither answer depends on how the machine
/// was feeling.
/// </para>
/// <para>
/// <b>Who announces themselves is the whole subtlety, and it differs by run.</b> "A callback returned" and
/// "that callback's element reached the sink" are two events with a gap between them: the result is handed
/// to the stage's loop, which emits it on its own thread. An unordered run measures the second event, so the
/// sink is what announces each order there; a first order that waited on callbacks returning instead would
/// be racing that gap, and would occasionally be emitted first — which is the opposite of what the run says
/// it demonstrates. This is not a scheduling detail a keener
/// <see cref="T:System.Threading.Tasks.TaskCompletionSource"/> could close: no completion of a callback
/// makes its element already emitted.
/// </para>
/// <para>
/// <b>An ordered run must not do that.</b> An ordered mapping holds a finished result until everything
/// before it has been emitted, so a first order waiting for the rest of its batch to be emitted would be
/// waiting for emissions that cannot happen until it is emitted itself. There the callbacks announce
/// themselves and nothing is lost by it, because the answer that run reports is the operator's guarantee
/// rather than the arrangement's.
/// </para>
/// <para>
/// <b>The batch and not the whole feed, deliberately.</b> The same hazard one step out: making the first
/// order wait for an order outside the declared concurrency window would wait for an order that can never be
/// admitted. That is a deadlock rather than a demonstration, and it is the shape of mistake a bound this
/// explicit is good at catching.
/// </para>
/// <para>
/// A tally of nothing is already complete, so a scenario that runs one element does not hang.
/// </para>
/// </remarks>
[<Sealed>]
type Countdown(count: int) =
    let padlock = obj ()
    let reached = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable remaining = count

    do
        if count <= 0 then
            reached.TrySetResult() |> ignore

    /// <summary>Announces one thing.</summary>
    /// <remarks>
    /// Called from inside a sink as readily as from inside a callback, which is to say from a run's own
    /// segment thread as readily as from the thread pool. The tally is taken under the lock and the wait
    /// completes asynchronously, so a sink that announces the last of a tally hands whatever was waiting on
    /// it to the pool rather than running it inline on the thread that is in the middle of emitting.
    /// </remarks>
    member _.Signal() =
        Monitor.Enter padlock

        try
            if remaining > 0 then
                remaining <- remaining - 1

                if remaining = 0 then
                    reached.TrySetResult() |> ignore
        finally
            Monitor.Exit padlock

    /// <summary>Waits until everything has announced itself.</summary>
    /// <param name="cancellationToken">The run's own token, which ends the wait if the run does.</param>
    /// <returns>A task that completes when the tally reaches zero.</returns>
    member _.WaitAsync(cancellationToken: CancellationToken) : Task = reached.Task.WaitAsync cancellationToken

/// <summary>A stage that raises for one chosen order, for as long as it has failures left to spend.</summary>
/// <remarks>
/// <para>
/// The failure scenario's whole moving part, shared by both authorings so that the two runs fail in exactly
/// the same places. A stage that failed at random would make a sample that sometimes disagrees with itself.
/// </para>
/// <para>
/// The count is of raises rather than of arrivals, so a retrying scope's re-offers are what spend the
/// budget: give it two failures inside a scope that allows three attempts and the third offer succeeds,
/// which is the shape the retry graph is built to show.
/// </para>
/// </remarks>
[<Sealed>]
type FlakyStage(sequence: int, failures: int) =
    let padlock = obj ()
    let mutable raised = 0

    /// <summary>Gets the order this stage raises for.</summary>
    member _.Sequence = sequence

    /// <summary>Gets how many times it has raised.</summary>
    member _.Raised =
        Monitor.Enter padlock

        try
            raised
        finally
            Monitor.Exit padlock

    /// <summary>Passes an order through, raising for the chosen one until its budget is spent.</summary>
    /// <param name="order">The order.</param>
    /// <returns>The same order, when it is not the chosen one or the budget is spent.</returns>
    /// <exception cref="T:System.InvalidOperationException">This is the chosen order and a failure is left.</exception>
    member _.Pass(order: OrderEvent) : OrderEvent =
        let raising =
            if order.Sequence <> sequence then
                false
            else
                Monitor.Enter padlock

                try
                    if raised < failures then
                        raised <- raised + 1
                        true
                    else
                        false
                finally
                    Monitor.Exit padlock

        if raising then
            raise (
                InvalidOperationException(
                    $"The downstream system rejected {order.OrderId}. This is the sample's deliberate failure."
                )
            )

        order

    /// <summary>Builds a stage that raises for one order every single time it sees it.</summary>
    /// <param name="sequence">The order to raise for.</param>
    /// <returns>The stage.</returns>
    static member AlwaysAt(sequence: int) = FlakyStage(sequence, Int32.MaxValue)
