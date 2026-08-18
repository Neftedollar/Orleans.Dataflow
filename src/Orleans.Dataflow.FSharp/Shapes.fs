namespace Orleans.Dataflow.FSharp

open System.Collections.Generic
open Orleans.Dataflow.Authoring
open Orleans.Dataflow.Identity

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

/// <summary>One leg of a junction, complete: everything the elements that take it go through, and the
/// terminal that consumes them.</summary>
/// <remarks>
/// <para>
/// A branch is a chain and not a shape, for the reason a flow is not one: a leg has one input and ends in a
/// terminal, so nothing inside it can branch. It exists as a type rather than as a pair of arguments because
/// a leg has no receiver to hang off — element types travel left to right from sources, and a leg is built
/// right to left from its terminal, so <see cref="T:Orleans.Dataflow.FSharp.Flow`2"/> is what fixes the type
/// it consumes.
/// </para>
/// <para>
/// A branch that declares no result is reusable exactly as a flow is: composing it into two graphs
/// contributes its occurrences to both. A branch that does declare one closes exactly one graph, because its
/// slot binds to the graph that closed it; a second junction call over the same branch is refused rather than
/// silently repointing the first graph's slot.
/// </para>
/// <para>
/// The result is carried as one option over the name and the binding together, so a reader either sees a
/// branch with a complete result or a branch with none, and never a name beside a binding that no slot
/// shares. The C# facade spells the same fact as two independently nullable properties.
/// </para>
/// </remarks>
[<Sealed; NoEquality; NoComparison>]
type Branch<'In>
    internal (stages: IReadOnlyList<StageOccurrence>, result: (ResultSlotId * BranchSlotBinding) voption) =
    /// <summary>Gets the occurrence chain this value carries, terminal included.</summary>
    member internal _.Stages = stages

    /// <summary>Gets the name and the waiting binding of the result this branch declares, when it declares one.</summary>
    member internal _.Result = result

/// <summary>A diamond in flight: one stream broadcast through two flows, waiting for the call that rejoins
/// them.</summary>
/// <remarks>
/// <para>
/// The one authoring value in this package with two open ends, and that is the whole reason it exists.
/// A <see cref="T:Orleans.Dataflow.FSharp.Source`1"/> is one stream, a <see cref="T:Orleans.Dataflow.FSharp.Branch`1"/>
/// is one leg ending in a terminal, and a junction call takes a graph from one shape to another without ever
/// leaving two ends dangling. Re-convergence — the same elements going two ways and meeting again — is not a
/// tree, so it gets a carrier rather than a builder (ADR 0006).
/// </para>
/// <para>
/// The rejoin is total and positional, and it is legal without a buffer between the halves precisely because
/// both descend from one broadcast and therefore advance together. A fork is a value like every other:
/// rejoining one twice builds two graphs and neither disturbs the other.
/// </para>
/// </remarks>
[<Sealed; NoEquality; NoComparison>]
type Fork<'T1, 'T2> internal (shape: LocalGraphShape) =
    /// <summary>Gets the algebra state this value carries, with the two derived streams still open.</summary>
    member internal _.State = shape
