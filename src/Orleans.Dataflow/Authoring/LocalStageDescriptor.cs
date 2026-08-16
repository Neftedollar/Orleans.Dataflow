using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One occurrence of a local, lambda-implemented stage as an authoring value holds it: what the stage is,
/// and the runtime behavior bound to it.
/// </summary>
/// <remarks>
/// <para>
/// A descriptor is not yet a node. It has no <see cref="NodeId"/>, because ADR 0004 allocates identifiers
/// at graph closure and not at value creation: a reusable <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/>
/// occupies a different position in every graph it is composed into, so a position fixed at creation would
/// be wrong in all but one of them. An authoring value therefore holds an ordered list of descriptors and
/// composes by concatenation; <see cref="LocalGraphBuilder"/> turns that list into fragments, nodes, and a
/// document exactly once, at <c>To</c>.
/// </para>
/// <para>
/// <see cref="Behavior"/> and <see cref="Seed"/> are the two halves of the authoring-side binding, and
/// neither ever reaches a document. The values are held as <see cref="object"/> because one descriptor list
/// spans a chain whose element types change at every mapping stage; the delegates keep their original
/// constructed types, so the local runtime can recover them without ever having widened them.
/// </para>
/// <para>
/// <see cref="Parameters"/> is the other half of the split and goes the other way. A buffer's capacity and
/// policy, and an asynchronous stage's concurrency bound, are configuration rather than behavior: they are
/// numbers and names a document can state, they change what a graph observably does, and they therefore
/// belong in the payload and in the fingerprint. Every other shape carries the empty object, because a
/// delegate is all it has and a delegate is never durable topology.
/// </para>
/// </remarks>
internal sealed class LocalStageDescriptor
{
    /// <summary>Initializes a new instance of the <see cref="LocalStageDescriptor"/> class.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <param name="behavior">The bound delegate or sequence, or <see langword="null"/> when the shape has none.</param>
    /// <param name="seed">The fold seed, which is meaningful only for <see cref="LocalStageKind.Fold"/>.</param>
    /// <param name="parameters">The parameter payload the node carries.</param>
    private LocalStageDescriptor(
        LocalStageKind kind,
        object? behavior,
        object? seed,
        CanonicalJsonValue parameters)
    {
        Kind = kind;
        Behavior = behavior;
        Seed = seed;
        Parameters = parameters;
    }

    /// <summary>Gets the stage shape.</summary>
    internal LocalStageKind Kind { get; }

    /// <summary>
    /// Gets the bound behavior: the sequence for a source, the selector, the predicate, or the folder.
    /// </summary>
    /// <value>
    /// <see langword="null"/> only for <see cref="LocalStageKind.Ignore"/>, which has nothing to do to an
    /// element, and <see cref="LocalStageKind.Buffer"/>, whose whole behavior is stated by its parameters.
    /// </value>
    internal object? Behavior { get; }

    /// <summary>Gets the initial state of a fold.</summary>
    /// <value>
    /// The seed for <see cref="LocalStageKind.Fold"/>, which may itself legitimately be
    /// <see langword="null"/> when the state type is a nullable one; <see langword="null"/> for every other
    /// shape. <see cref="Kind"/> and not this value decides whether a seed exists.
    /// </value>
    internal object? Seed { get; }

    /// <summary>Gets the parameter payload this occurrence writes into its node.</summary>
    /// <value>
    /// The buffer's capacity and policy, the asynchronous stage's concurrency bound, or the empty object
    /// for every shape whose behavior is only a delegate.
    /// </value>
    internal CanonicalJsonValue Parameters { get; }

    /// <summary>Gets the stage reference this occurrence declares in a document.</summary>
    internal StageRef Stage => LocalVocabulary.StageOf(Kind);

    /// <summary>Gets the parameter contract this occurrence declares in a document.</summary>
    internal ContractReference ParameterContract => LocalVocabulary.ParameterContractOf(Kind);

    /// <summary>Gets a value indicating whether this occurrence declares an input port.</summary>
    internal bool HasInput => Kind is not LocalStageKind.FromEnumerable;

    /// <summary>Gets a value indicating whether this occurrence declares an output port.</summary>
    internal bool HasOutput => Kind is not (LocalStageKind.Fold or LocalStageKind.Ignore);

    /// <summary>Creates a source over an in-memory sequence.</summary>
    /// <param name="elements">The sequence, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor FromEnumerable(object elements) =>
        new(LocalStageKind.FromEnumerable, elements, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a mapping stage.</summary>
    /// <param name="selector">The mapping delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Select(object selector) =>
        new(LocalStageKind.Select, selector, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a filtering stage.</summary>
    /// <param name="predicate">The predicate delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Where(object predicate) =>
        new(LocalStageKind.Where, predicate, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a bounded buffer.</summary>
    /// <param name="options">The validated capacity and overflow policy.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// A buffer has no delegate at all: the whole of it is in the payload, which is why it is the one shape
    /// whose behavior a document states completely.
    /// </remarks>
    internal static LocalStageDescriptor Buffer(BufferOptions options) =>
        new(LocalStageKind.Buffer, behavior: null, seed: null, LocalBufferParameters.Write(options));

    /// <summary>Creates an order-preserving asynchronous mapping stage.</summary>
    /// <param name="options">The validated concurrency bound.</param>
    /// <param name="selector">The asynchronous callback, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor SelectAsync(ParallelismOptions options, object selector) =>
        new(LocalStageKind.SelectAsync, selector, seed: null, LocalParallelismParameters.Write(options));

    /// <summary>Creates an asynchronous mapping stage that emits in completion order.</summary>
    /// <param name="options">The validated concurrency bound.</param>
    /// <param name="selector">The asynchronous callback, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor SelectAsyncUnordered(ParallelismOptions options, object selector) =>
        new(LocalStageKind.SelectAsyncUnordered, selector, seed: null, LocalParallelismParameters.Write(options));

    /// <summary>Creates a folding sink.</summary>
    /// <param name="seed">The initial state, which may be <see langword="null"/>.</param>
    /// <param name="folder">The folding delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Fold(object? seed, object folder) =>
        new(LocalStageKind.Fold, folder, seed, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a discarding sink.</summary>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// A new instance rather than a shared one, because the binding table is keyed by node identifier and a
    /// shared instance would make two occurrences indistinguishable in a debugger for no gain.
    /// </remarks>
    internal static LocalStageDescriptor Ignore() =>
        new(LocalStageKind.Ignore, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Returns a one-line diagnostic summary of this occurrence.</summary>
    /// <returns>The stage reference text, such as <c>local:select@1</c>.</returns>
    /// <remarks>The bound behavior is deliberately not rendered: a closure has no useful text form.</remarks>
    public override string ToString() => Stage.ToString();
}
