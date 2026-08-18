namespace Orleans.Dataflow.FSharp

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Orleans.Dataflow.Authoring

// Orleans.Dataflow itself is deliberately not opened: see the note in Source.fs.

/// <summary>Constructs the terminals a graph is closed with.</summary>
/// <remarks>
/// <para>
/// A sink that declares a result is a distinct type, so dropping a result is spelled by choosing
/// <see cref="M:Orleans.Dataflow.FSharp.Sink.ignore``1"/> and never by an inference accident. Qualified
/// access is required, which is also what lets <c>Sink.ignore</c> coexist with the core library's
/// <c>ignore</c> without shadowing it.
/// </para>
/// <para>
/// A result-bearing sink names nothing: the name a run resolves it under is written at the close, by
/// <see cref="M:Orleans.Dataflow.FSharp.Source.toResult``2"/>, because a chain has one closing call and a
/// sink is a reusable value that may be closed into several graphs.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Sink =

    /// <summary>The sink that consumes every element and keeps nothing.</summary>
    /// <remarks>What a graph is closed with when only its side effects matter.</remarks>
    [<GeneralizableValue>]
    let ignore<'T> : Sink<'T> = Sink<'T>(LocalStageChain.Of(LocalStageDescriptor.Ignore()))

    /// <summary>Creates a sink that hands every element to a synchronous callback.</summary>
    /// <param name="action">The callback receiving each element in order.</param>
    /// <returns>The sink.</returns>
    let forEach (action: 'T -> unit) : Sink<'T> =
        Sink<'T>(LocalStageChain.Of(LocalStageDescriptor.ForEach(Action<'T> action)))

    /// <summary>Creates a sink that hands every element to a task-returning callback.</summary>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="action">The callback receiving each element, which receives the run's own token.</param>
    /// <returns>The sink.</returns>
    /// <remarks>
    /// The callbacks are independent of each other, which is why this declares a bound and the folding
    /// terminals do not: a fold's next call needs the previous one's answer, and a side effect's does not.
    /// </remarks>
    let forEachTask
        (options: Orleans.Dataflow.ParallelismOptions)
        (action: 'T -> CancellationToken -> Task)
        : Sink<'T> =
        Sink<'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.ForEachAsync(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'T, CancellationToken, Task>(fun element token -> action element token))))

    /// <summary>Creates a sink that hands every element to an asynchronous computation.</summary>
    /// <param name="options">The greatest number of computations in flight at one time.</param>
    /// <param name="action">The computation built for each element.</param>
    /// <returns>The sink.</returns>
    /// <remarks>
    /// The F# effect over the very stage <see cref="M:Orleans.Dataflow.FSharp.Sink.forEachTask``1"/> writes,
    /// with the run's own token starting the computation.
    /// </remarks>
    let forEachAsync
        (options: Orleans.Dataflow.ParallelismOptions)
        (action: 'T -> Async<unit>)
        : Sink<'T> =
        Sink<'T>(
            LocalStageChain.Of(
                LocalStageDescriptor.ForEachAsync(
                    LocalOptionGuard.Parallelism(options, nameof options),
                    Func<'T, CancellationToken, Task>(fun element token ->
                        Bindings.asTask (action element) token :> Task))))

    /// <summary>Creates a sink that folds every element into a result the run resolves at its end.</summary>
    /// <param name="seed">The initial state.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The state is allocated per run: one sink value closed into two graphs is two independent folds. The
    /// result resolves through the slot named at the close, faults with the run, and resolves the state
    /// accumulated so far when a graceful shutdown drains the run.
    /// </remarks>
    let aggregate (seed: 'State) (folder: 'State -> 'T -> 'State) : SinkWithResult<'T, 'State> =
        SinkWithResult<'T, 'State>(
            LocalStageChain.Of(
                LocalStageDescriptor.Fold(seed, Func<'State, 'T, 'State> folder)))

    /// <summary>Creates a sink that folds every element through a task-returning function.</summary>
    /// <param name="seed">The initial state.</param>
    /// <param name="folder">The callback combining the running state with the next element.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// No bound to declare, and the absence is the contract: the state the next element folds into is this
    /// fold's answer, so one fold runs at a time by construction. That is what separates it from
    /// <see cref="M:Orleans.Dataflow.FSharp.Sink.forEachTask``1"/>, which declares one.
    /// </remarks>
    let aggregateTask
        (seed: 'State)
        (folder: 'State -> 'T -> CancellationToken -> Task<'State>)
        : SinkWithResult<'T, 'State> =
        SinkWithResult<'T, 'State>(
            LocalStageChain.Of(
                LocalStageDescriptor.FoldAsync(
                    seed,
                    Func<'State, 'T, CancellationToken, Task<'State>>(fun state element token ->
                        folder state element token))))

    /// <summary>Creates a sink that folds every element through an asynchronous computation.</summary>
    /// <param name="seed">The initial state.</param>
    /// <param name="folder">The computation built from the running state and the next element.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The F# effect over the very stage <see cref="M:Orleans.Dataflow.FSharp.Sink.aggregateTask``2"/>
    /// writes, with the run's own token starting the computation.
    /// </remarks>
    let aggregateAsync
        (seed: 'State)
        (folder: 'State -> 'T -> Async<'State>)
        : SinkWithResult<'T, 'State> =
        SinkWithResult<'T, 'State>(
            LocalStageChain.Of(
                LocalStageDescriptor.FoldAsync(
                    seed,
                    Func<'State, 'T, CancellationToken, Task<'State>>(fun state element token ->
                        Bindings.asTask (folder state element) token))))

    /// <summary>The sink that resolves the first element and requires one.</summary>
    /// <remarks>
    /// A stream that ends with no element faults the run rather than resolving anything, which is what
    /// "requires one" means. Taking the first element ends the stream the way a count bound does: everything
    /// upstream stops and is released.
    /// </remarks>
    [<GeneralizableValue>]
    let first<'T> : SinkWithResult<'T, 'T> =
        SinkWithResult<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.First()))

    /// <summary>The sink that resolves the first element, or the element type's default value.</summary>
    /// <remarks>
    /// The honest spelling of "there may be no element": an empty stream resolves
    /// <c>Unchecked.defaultof&lt;'T&gt;</c>, which is zero for a numeric type and <see langword="null"/> for
    /// a reference type. That is the C# vocabulary's own shape and is mirrored rather than improved on —
    /// a sink resolving an option would be a result type only F#-authored graphs could have, and the value
    /// a sink resolves is binding rather than payload, so such a graph would share its fingerprint with a
    /// C# one that resolves something else. <see cref="P:Orleans.Dataflow.FSharp.Sink.first``1"/> is the
    /// spelling that refuses an empty stream instead.
    /// </remarks>
    [<GeneralizableValue>]
    let firstOrDefault<'T> : SinkWithResult<'T, 'T> =
        SinkWithResult<'T, 'T>(
            // The boxed default is carried rather than computed, because the runtime works in boxed elements
            // and has no type argument to take a default of. This is the same value C#'s default(T) produces.
            LocalStageChain.Of(LocalStageDescriptor.FirstOrDefault(Unchecked.defaultof<'T>)))

    /// <summary>The sink that resolves the last element and requires one.</summary>
    /// <remarks>
    /// A stream that ends with no element faults the run. Unlike the first-element sink this one reads the
    /// stream to its end, because which element is the last is not known until there are no more.
    /// </remarks>
    [<GeneralizableValue>]
    let last<'T> : SinkWithResult<'T, 'T> =
        SinkWithResult<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.Last()))

    /// <summary>The sink that resolves the last element, or the element type's default value.</summary>
    /// <remarks>
    /// Everything <see cref="P:Orleans.Dataflow.FSharp.Sink.firstOrDefault``1"/> states about the default
    /// value holds here unchanged.
    /// </remarks>
    [<GeneralizableValue>]
    let lastOrDefault<'T> : SinkWithResult<'T, 'T> =
        SinkWithResult<'T, 'T>(
            // See firstOrDefault: the boxed default is the runtime's only way to have one.
            LocalStageChain.Of(LocalStageDescriptor.LastOrDefault(Unchecked.defaultof<'T>)))

    /// <summary>The sink that resolves how many elements it saw.</summary>
    /// <remarks>
    /// The count starts from zero on every run, so two runs of one graph each count their own stream. It is
    /// a 64-bit count because a stream is not bounded by anything this sink knows about.
    /// </remarks>
    [<GeneralizableValue>]
    let count<'T> : SinkWithResult<'T, int64> =
        SinkWithResult<'T, int64>(LocalStageChain.Of(LocalStageDescriptor.Count()))

    /// <summary>Creates a sink that resolves a bounded list of every element it saw.</summary>
    /// <param name="options">The greatest number of elements the sink may hold.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The bound is required and there is no unbounded spelling: what a long stream would accumulate is
    /// unbounded memory. The list is a snapshot copied out when the result resolves, so nothing shares
    /// storage with the run.
    /// </remarks>
    let collect (options: Orleans.Dataflow.CollectOptions) : SinkWithResult<'T, IReadOnlyList<'T>> =
        SinkWithResult<'T, IReadOnlyList<'T>>(
            LocalStageChain.Of(
                LocalStageDescriptor.Collect(
                    LocalOptionGuard.Collect(options, nameof options),
                    Bindings.groupOf<'T> ())))

    /// <summary>Creates a sink that writes every element into a channel the author owns.</summary>
    /// <param name="writer">The writer to offer elements to.</param>
    /// <returns>The sink.</returns>
    /// <remarks>
    /// The channel's own bound is the backpressure. The run does not complete the writer and does not own
    /// it, because a run does not own what it was handed.
    /// </remarks>
    let toChannel (writer: ChannelWriter<'T>) : Sink<'T> =
        Sink<'T>(LocalStageChain.Of(LocalStageDescriptor.ToChannel writer))
