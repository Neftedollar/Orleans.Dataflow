namespace Orleans.Dataflow.FSharp

open System
open Orleans.Dataflow.Authoring

/// <summary>Constructs and composes reusable element transformations.</summary>
/// <remarks>
/// Every function answers a new immutable value and touches nothing it was given. The delegates an author
/// writes are stored typed — the runtime's own delegate adapter is the single owner of how a typed lambda
/// meets a boxed element — so this module converts an F# function to its <see cref="T:System.Func`2"/>
/// shape and nothing more, exactly as the C# facade stores what it receives. One named function per
/// operation, never an overload family: overloads are what degrade F# diagnostics to a candidate dump
/// (F-SHARP-API.md).
/// </remarks>
[<RequireQualifiedAccess>]
module Flow =

    /// <summary>The flow that changes nothing: it contributes no occurrence to any graph.</summary>
    /// <remarks>
    /// The unit of <see cref="M:Orleans.Dataflow.FSharp.Flow.andThen``3"/>, and the F# spelling of the C#
    /// <c>Flow.For&lt;'T&gt;()</c> anchor: a source composed through it is the source it was, byte for byte.
    /// </remarks>
    [<GeneralizableValue>]
    let identity<'T> : Flow<'T, 'T> = Flow<'T, 'T>(LocalStageChain.Empty)

    /// <summary>Transforms every element through a function.</summary>
    /// <param name="mapping">The function applied to each element.</param>
    /// <returns>The flow.</returns>
    let map (mapping: 'In -> 'Out) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(LocalStageChain.Of(LocalStageDescriptor.Select(Func<'In, 'Out> mapping)))

    /// <summary>Keeps the elements a predicate answers true for.</summary>
    /// <param name="predicate">The predicate deciding each element.</param>
    /// <returns>The flow.</returns>
    let filter (predicate: 'T -> bool) : Flow<'T, 'T> =
        Flow<'T, 'T>(LocalStageChain.Of(LocalStageDescriptor.Where(Func<'T, bool> predicate)))

    /// <summary>Composes two flows into one that applies them in order.</summary>
    /// <param name="next">The flow applied second.</param>
    /// <param name="current">The flow applied first.</param>
    /// <returns>The composed flow.</returns>
    /// <remarks>
    /// The value being extended is the final argument, so composition reads forward under
    /// <c>|&gt;</c>: <c>Flow.filter isValid |&gt; Flow.andThen (Flow.map normalize)</c> filters and then
    /// maps. This is the primary readable composition function; no symbolic operator stands in for it.
    /// </remarks>
    let andThen (next: Flow<'Middle, 'Out>) (current: Flow<'In, 'Middle>) : Flow<'In, 'Out> =
        Flow<'In, 'Out>(LocalStageChain.Concat(current.Stages, next.Stages))
