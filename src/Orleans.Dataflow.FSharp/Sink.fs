namespace Orleans.Dataflow.FSharp

open System
open Orleans.Dataflow.Authoring

/// <summary>Constructs the terminals a graph is closed with.</summary>
/// <remarks>
/// A sink that declares a result is a distinct type, so dropping a result is spelled by choosing
/// <see cref="M:Orleans.Dataflow.FSharp.Sink.ignore``1"/> and never by an inference accident. Qualified
/// access is required, which is also what lets <c>Sink.ignore</c> coexist with the core library's
/// <c>ignore</c> without shadowing it.
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
