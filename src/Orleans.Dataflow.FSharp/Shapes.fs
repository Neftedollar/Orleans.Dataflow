namespace Orleans.Dataflow.FSharp

open System.Collections.Generic
open Orleans.Dataflow.Authoring

/// <summary>An open stream of elements: the front of a graph, before anything closes it.</summary>
/// <remarks>
/// The F# spelling of the same algebra state the C# <c>Source&lt;T&gt;</c> carries — a partial graph shape —
/// and deliberately not a wrapper over that type: the fluent facade is one language's spelling, and this
/// package binds to the shape itself (F-SHARP-API.md, binding rule). The type parameter is the element
/// type of the one open output, tracked by the compiler and never by the shape: composing through a typed
/// flow is what moves it, exactly as it moves through the C# facade.
/// </remarks>
[<Sealed; NoEquality; NoComparison>]
type Source<'T> internal (shape: LocalGraphShape) =
    /// <summary>Gets the algebra state this value carries.</summary>
    member internal _.State = shape

/// <summary>A reusable transformation from one element type to another.</summary>
/// <remarks>
/// A flow is a chain of stage occurrences and nothing more — no shape, because a value with one input and
/// one output cannot branch. It is immutable: composing it into two graphs reads it twice, so one flow in
/// two graphs is two sets of stages rather than one shared by both.
/// </remarks>
[<Sealed; NoEquality; NoComparison>]
type Flow<'In, 'Out> internal (stages: IReadOnlyList<StageOccurrence>) =
    /// <summary>Gets the occurrence chain this value carries.</summary>
    member internal _.Stages = stages

/// <summary>A terminal that consumes a stream and declares no result.</summary>
[<Sealed; NoEquality; NoComparison>]
type Sink<'T> internal (stages: IReadOnlyList<StageOccurrence>) =
    /// <summary>Gets the occurrence chain this value carries.</summary>
    member internal _.Stages = stages

/// <summary>A terminal that consumes a stream and produces one named result when the run completes.</summary>
/// <remarks>
/// Deliberately not convertible to <see cref="T:Orleans.Dataflow.FSharp.Sink`1"/> by inference: dropping a
/// result is spelled by choosing the resultless sink, never by an overload accident. This mirrors the C#
/// facade's guard, whose explicit <c>ToSink()</c> conversion F# has no spelling for and therefore omits
/// rather than approximates.
/// </remarks>
[<Sealed; NoEquality; NoComparison>]
type SinkWithResult<'T, 'Result> internal (stages: IReadOnlyList<StageOccurrence>) =
    /// <summary>Gets the occurrence chain this value carries.</summary>
    member internal _.Stages = stages
