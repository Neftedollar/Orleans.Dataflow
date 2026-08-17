using Orleans.Dataflow.Definition;
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
/// be wrong in all but one of them. An authoring value therefore holds an ordered list of occurrences and
/// composes by concatenation; <see cref="LocalGraphBuilder"/> turns that list into fragments, nodes, and a
/// document exactly once, at <c>To</c>.
/// </para>
/// <para>
/// <see cref="Name"/> is therefore always <see langword="null"/> here, and that is not an omission: a name
/// on a lambda stage would promise an edit-stable identity the delegate behind it cannot honor, so a graph
/// holding one declares <see cref="CapabilityToken.EphemeralIdentity"/>. Explicit names are the
/// registered surface's, where the behavior is in the catalog and the identity means something.
/// </para>
/// <para>
/// <see cref="Behavior"/> and <see cref="Seed"/> are the two halves of the authoring-side binding, and
/// neither ever reaches a document. The values are held as <see cref="object"/> because one descriptor list
/// spans a chain whose element types change at every mapping stage; the delegates keep their original
/// constructed types, so the local runtime can recover them without ever having widened them.
/// </para>
/// <para>
/// <see cref="Parameters"/> is the other half of the split and goes the other way. A buffer's capacity and
/// policy, an asynchronous stage's concurrency bound, a count of elements, a range's bounds, and a
/// deduplication key bound are configuration rather than behavior: they are numbers and names a document
/// can state, they change what a graph observably does, and they therefore belong in the payload and in the
/// fingerprint. Every other shape carries the empty object, because a delegate is all it has and a delegate
/// is never durable topology.
/// </para>
/// </remarks>
internal sealed class LocalStageDescriptor : StageOccurrence
{
    /// <summary>Initializes a new instance of the <see cref="LocalStageDescriptor"/> class.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <param name="behavior">The bound delegate, sequence, or value, or <see langword="null"/> when the shape has none.</param>
    /// <param name="seed">The initial state, which is meaningful only for the shapes that carry one.</param>
    /// <param name="parameters">The parameter payload the node carries.</param>
    /// <param name="controlSlot">The name of the runtime control the shape produces, when it produces one.</param>
    /// <param name="controlType">The type of that control, when there is one.</param>
    private LocalStageDescriptor(
        LocalStageKind kind,
        object? behavior,
        object? seed,
        CanonicalJsonValue parameters,
        ResultSlotId? controlSlot = null,
        Type? controlType = null)
    {
        Kind = kind;
        Behavior = behavior;
        Seed = seed;
        Parameters = parameters;
        ControlSlot = controlSlot;
        ControlType = controlType;
    }

    /// <summary>Gets the stage shape.</summary>
    internal LocalStageKind Kind { get; }

    /// <summary>
    /// Gets the bound behavior: the sequence, task, exception, or value a source carries, or the selector,
    /// the predicate, the folder, the generator, the comparer, or the callback of everything else.
    /// </summary>
    /// <value>
    /// <see langword="null"/> for the shapes whose whole behavior is stated by their parameters or by their
    /// stage reference — <see cref="LocalStageKind.Buffer"/>, <see cref="LocalStageKind.Empty"/>,
    /// <see cref="LocalStageKind.Range"/>, <see cref="LocalStageKind.Ignore"/>,
    /// <see cref="LocalStageKind.First"/>, <see cref="LocalStageKind.FirstOrDefault"/>, and
    /// <see cref="LocalStageKind.Count"/> — and legitimately <see langword="null"/> for a source bound to a
    /// null element. <see cref="Kind"/> and not this value decides what a binding has to be.
    /// </value>
    internal object? Behavior { get; }

    /// <summary>Gets the initial state of a stage that carries one.</summary>
    /// <value>
    /// The seed of a fold, a scan, or an unfold, the count a counting sink starts from, and the default
    /// value the honest first-element sink resolves when it saw nothing; any of them may itself
    /// legitimately be <see langword="null"/> when the state type is a nullable one.
    /// <see cref="Kind"/> and not this value decides whether a seed exists.
    /// </value>
    internal object? Seed { get; }

    /// <inheritdoc/>
    /// <value>
    /// The numbers the shape declares, or the empty object for every shape whose behavior is only a
    /// delegate.
    /// </value>
    internal override CanonicalJsonValue Parameters { get; }

    /// <inheritdoc/>
    /// <value>
    /// Always <see langword="null"/>: this surface has no spelling for naming a lambda occurrence, and
    /// deliberately so.
    /// </value>
    internal override NodeId? Name => null;

    /// <inheritdoc/>
    internal override StageRef Stage => LocalVocabulary.StageOf(Kind);

    /// <inheritdoc/>
    internal override ContractReference ParameterContract => LocalVocabulary.ParameterContractOf(Kind);

    /// <inheritdoc/>
    /// <value>
    /// The one local input port name for every shape that consumes elements; <see langword="null"/> for a
    /// source, which consumes none.
    /// </value>
    internal override PortId? InputPort =>
        LocalVocabulary.InputPortsOf(Kind) is [{ } only] ? only.Id : null;

    /// <inheritdoc/>
    /// <value>
    /// The one local output port name for every shape that produces one stream; <see langword="null"/> for
    /// a terminal, which produces none, and for a junction, which produces several.
    /// </value>
    /// <remarks>
    /// A junction answers <see langword="null"/> here rather than naming its first leg, because this member
    /// is what the chain-composing builder connects to and a junction is not something a chain can hold: it
    /// is authored as a graph, and the graph surface that spells one is a later checkpoint. Deriving the
    /// answer from the declared port list rather than from a place is what makes that true by construction
    /// instead of by a rule written twice.
    /// </remarks>
    internal override PortId? OutputPort =>
        LocalVocabulary.OutputPortsOf(Kind) is [{ } only] ? only.Id : null;

    /// <inheritdoc/>
    /// <value>
    /// The result port of a result-bearing sink, the control port of an ingress queue, or
    /// <see langword="null"/> for every other shape.
    /// </value>
    internal override ResultPortSpecification? ResultPort => LocalVocabulary.ResultPortOf(Kind);

    /// <inheritdoc/>
    internal override ResultSlotId? ControlSlot { get; }

    /// <inheritdoc/>
    internal override Type? ControlType { get; }

    /// <inheritdoc/>
    /// <value>
    /// <see cref="CapabilityToken.Nondeployable"/>, for every local shape without exception. A delegate is
    /// not durable topology, and neither is a buffer whose only implementation lives in this process'
    /// local provider: the token is a statement about where the stage can run, not about whether the
    /// author happened to write a lambda for it.
    /// </value>
    internal override IReadOnlyList<CapabilityToken> RequiredCapabilities =>
        LocalVocabulary.RequiredCapabilities;

    /// <summary>Creates a source over an in-memory sequence.</summary>
    /// <param name="elements">The sequence, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor FromEnumerable(object elements) =>
        new(LocalStageKind.FromEnumerable, elements, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source that emits nothing.</summary>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Empty() =>
        new(LocalStageKind.Empty, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source over one element.</summary>
    /// <param name="value">The element, which may legitimately be <see langword="null"/>.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Single(object? value) =>
        new(LocalStageKind.Single, value, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source that emits one element a declared number of times.</summary>
    /// <param name="value">The element, which may legitimately be <see langword="null"/>.</param>
    /// <param name="count">The validated number of times to emit it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The count is payload and the element is binding, which is the split everywhere else too: how many
    /// times is a number a document can state, and what is repeated is a value of an element type the
    /// document knows nothing about.
    /// </remarks>
    internal static LocalStageDescriptor Repeat(object? value, int count) =>
        new(LocalStageKind.Repeat, value, seed: null, LocalCountParameters.Write(count));

    /// <summary>Creates a source over a run of consecutive integers.</summary>
    /// <param name="start">The validated first element.</param>
    /// <param name="count">The validated number of elements.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The second shape with no delegate at all, after the buffer: its elements are integers, and a
    /// document can state exactly which ones.
    /// </remarks>
    internal static LocalStageDescriptor Range(int start, int count) =>
        new(LocalStageKind.Range, behavior: null, seed: null, LocalRangeParameters.Write(start, count));

    /// <summary>Creates a source over the value of one task.</summary>
    /// <param name="task">The task, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor FromTask(object task) =>
        new(LocalStageKind.FromTask, task, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source that fails the run.</summary>
    /// <param name="exception">The exception the run faults with.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Failed(Exception exception) =>
        new(LocalStageKind.Failed, exception, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source driven by a generator over its own state.</summary>
    /// <param name="seed">The initial state, which may legitimately be <see langword="null"/>.</param>
    /// <param name="generator">The generator delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Unfold(object? seed, object generator) =>
        new(LocalStageKind.Unfold, generator, seed, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source over an asynchronous sequence.</summary>
    /// <param name="open">
    /// The opener the authoring surface built, already closed over the element type.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The binding is an opener rather than the author's own sequence, because
    /// <see cref="IAsyncEnumerable{T}"/> is an interface and one class may implement it for two element
    /// types; nothing in a document names which of them the graph means, and the type argument the author
    /// wrote is the only statement of it. This is the same reason a deduplicating stage is bound to the
    /// element type's comparer rather than to the element type.
    /// </remarks>
    internal static LocalStageDescriptor FromAsyncEnumerable(object open) =>
        new(LocalStageKind.FromAsyncEnumerable, open, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source over a factory of one element.</summary>
    /// <param name="factory">The factory delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor FromFactory(object factory) =>
        new(LocalStageKind.FromFactory, factory, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source over an asynchronous factory of one element.</summary>
    /// <param name="factory">The factory delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor FromAsyncFactory(object factory) =>
        new(LocalStageKind.FromAsyncFactory, factory, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source that emits nothing and never ends.</summary>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Never() =>
        new(LocalStageKind.Never, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source that repeats an in-memory sequence endlessly.</summary>
    /// <param name="elements">The sequence, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Cycle(object elements) =>
        new(LocalStageKind.Cycle, elements, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source driven by an asynchronous generator over its own state.</summary>
    /// <param name="seed">The initial state, which may legitimately be <see langword="null"/>.</param>
    /// <param name="generator">The generator delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor UnfoldAsync(object? seed, object generator) =>
        new(LocalStageKind.UnfoldAsync, generator, seed, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a source over a bounded ingress queue of its own.</summary>
    /// <param name="options">The validated capacity and overflow policy.</param>
    /// <param name="controlSlot">The validated name the control is declared under.</param>
    /// <param name="controlType">The closed generic type of the control an author receives.</param>
    /// <param name="facade">The factory that wraps a run's queue into that typed control.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The payload is a buffer's payload, under a buffer's contract, because a queue's capacity and
    /// overflow policy are a buffer's capacity and overflow policy seen from the other side of a graph. The
    /// stage reference is what says which of them a node is, exactly as it does for the three stages that
    /// share a count.
    /// </remarks>
    internal static LocalStageDescriptor Queue(
        BufferOptions options,
        ResultSlotId controlSlot,
        Type controlType,
        object facade) =>
        new(
            LocalStageKind.Queue,
            facade,
            seed: null,
            LocalBufferParameters.Write(options),
            controlSlot,
            controlType);

    /// <summary>Creates a source over a channel the author owns.</summary>
    /// <param name="reader">The reader, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor FromChannel(object reader) =>
        new(LocalStageKind.FromChannel, reader, seed: null, LocalVocabulary.EmptyParameters);

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

    /// <summary>Creates a running fold that emits its intermediate states.</summary>
    /// <param name="seed">The initial state, which may be <see langword="null"/>.</param>
    /// <param name="folder">The folding delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Scan(object? seed, object folder) =>
        new(LocalStageKind.Scan, folder, seed, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that passes a declared number of elements.</summary>
    /// <param name="count">The validated number of elements to pass.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Take(int count) =>
        new(LocalStageKind.Take, behavior: null, seed: null, LocalCountParameters.Write(count));

    /// <summary>Creates a stage that drops a declared number of elements.</summary>
    /// <param name="count">The validated number of elements to drop.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Skip(int count) =>
        new(LocalStageKind.Skip, behavior: null, seed: null, LocalCountParameters.Write(count));

    /// <summary>Creates a stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The predicate delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor TakeWhile(object predicate) =>
        new(LocalStageKind.TakeWhile, predicate, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that passes elements up to and including the one a predicate accepts.</summary>
    /// <param name="predicate">The predicate delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor TakeThrough(object predicate) =>
        new(LocalStageKind.TakeThrough, predicate, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that drops elements while a predicate holds.</summary>
    /// <param name="predicate">The predicate delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor SkipWhile(object predicate) =>
        new(LocalStageKind.SkipWhile, predicate, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that drops repeated elements.</summary>
    /// <param name="options">The validated key bound.</param>
    /// <param name="comparer">
    /// The element type's default equality, which is an <see cref="System.Collections.IEqualityComparer"/>.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The comparer is the binding, because equality belongs to an element type the document cannot name;
    /// the bound is the payload, because it is a number that changes what the graph does.
    /// </remarks>
    internal static LocalStageDescriptor Distinct(DistinctOptions options, object comparer) =>
        new(LocalStageKind.Distinct, comparer, seed: null, LocalDistinctParameters.Write(options));

    /// <summary>Creates a bounded buffer.</summary>
    /// <param name="options">The validated capacity and overflow policy.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// A buffer has no delegate at all: the whole of it is in the payload, which is why it was the first
    /// shape whose behavior a document states completely.
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

    /// <summary>Creates an order-preserving asynchronous mapping stage over value tasks.</summary>
    /// <param name="options">The validated concurrency bound.</param>
    /// <param name="selector">The asynchronous callback, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// A stage of its own rather than a flavour of <see cref="SelectAsync"/>, because the shape of the
    /// callback is not something a document could state: a binding is not durable topology, so two stages
    /// whose only difference is the type of the thing they await would be one node with two possible
    /// meanings. The runtime converts the two shapes into one at the callback boundary and drives them with
    /// one implementation; what is written down is which of them the author wrote.
    /// </remarks>
    internal static LocalStageDescriptor SelectValueTaskAsync(ParallelismOptions options, object selector) =>
        new(LocalStageKind.SelectValueTaskAsync, selector, seed: null, LocalParallelismParameters.Write(options));

    /// <summary>Creates a value-task mapping stage that emits in completion order.</summary>
    /// <param name="options">The validated concurrency bound.</param>
    /// <param name="selector">The asynchronous callback, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor SelectValueTaskAsyncUnordered(ParallelismOptions options, object selector) =>
        new(
            LocalStageKind.SelectValueTaskAsyncUnordered,
            selector,
            seed: null,
            LocalParallelismParameters.Write(options));

    /// <summary>Creates a junction that delivers every element to every live output.</summary>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// No behavior and no payload at all: a broadcast is decided entirely by the edges its legs carry, so
    /// there is nothing left for a binding to say and nothing a document could state twice.
    /// </remarks>
    internal static LocalStageDescriptor Broadcast() =>
        new(LocalStageKind.Broadcast, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a junction that delivers each element to exactly one output with room.</summary>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Balance() =>
        new(LocalStageKind.Balance, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a junction that delivers the two halves of a row to two outputs.</summary>
    /// <param name="left">The projection of a row onto its left half, as the authoring value received it.</param>
    /// <param name="right">The projection of a row onto its right half, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The two projections are behavior for the reason every projection is: which member of a row is its
    /// left half is a statement about an element type, and an element type never appears in a local
    /// document. What the document states is that this node splits one stream into two, which is the part
    /// that is topology.
    /// </remarks>
    internal static LocalStageDescriptor Unzip(object left, object right) =>
        new(LocalStageKind.Unzip, new[] { left, right }, seed: null, LocalVocabulary.EmptyParameters);

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

    /// <summary>Creates a sink that hands every element to a synchronous callback.</summary>
    /// <param name="callback">The callback delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor ForEach(object callback) =>
        new(LocalStageKind.ForEach, callback, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a sink that hands every element to an asynchronous callback.</summary>
    /// <param name="options">The validated concurrency bound.</param>
    /// <param name="callback">The asynchronous callback, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor ForEachAsync(ParallelismOptions options, object callback) =>
        new(LocalStageKind.ForEachAsync, callback, seed: null, LocalParallelismParameters.Write(options));

    /// <summary>Creates a sink that takes the first element and requires one.</summary>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor First() =>
        new(LocalStageKind.First, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a sink that takes the first element or the element type's default value.</summary>
    /// <param name="defaultValue">
    /// The value to resolve when the sink saw no element, which is <c>default(T)</c> boxed by the caller.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The default value is carried rather than computed, because the runtime works in boxed elements and
    /// has no type argument to take a default of; the authoring surface has one and hands the answer over.
    /// </remarks>
    internal static LocalStageDescriptor FirstOrDefault(object? defaultValue) =>
        new(LocalStageKind.FirstOrDefault, behavior: null, defaultValue, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a counting sink.</summary>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The seed is the zero the count starts from, held here rather than in the runtime for the same reason
    /// a fold's is: a run's state starts from the value the authoring surface fixed and never from where
    /// another run left off.
    /// </remarks>
    internal static LocalStageDescriptor Count() =>
        new(LocalStageKind.Count, behavior: null, 0L, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a sink that keeps the last element and requires one.</summary>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Last() =>
        new(LocalStageKind.Last, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a sink that keeps the last element or the element type's default value.</summary>
    /// <param name="defaultValue">
    /// The value to resolve when the sink saw no element, which is <c>default(T)</c> boxed by the caller.
    /// </param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor LastOrDefault(object? defaultValue) =>
        new(LocalStageKind.LastOrDefault, behavior: null, defaultValue, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a bounded collecting sink.</summary>
    /// <param name="options">The validated element bound.</param>
    /// <param name="freeze">
    /// The projection the authoring surface built, which turns the run's boxed elements into the typed list
    /// the author asked for.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The bound is payload, because it is a number that changes what the graph does; the projection is
    /// binding, because it is the one part that needs an element type, and a document never names one.
    /// </remarks>
    internal static LocalStageDescriptor Collect(CollectOptions options, object freeze) =>
        new(LocalStageKind.Collect, freeze, seed: null, LocalCollectParameters.Write(options));

    /// <summary>Creates a sink that writes every element into a channel the author owns.</summary>
    /// <param name="writer">The writer, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor ToChannel(object writer) =>
        new(LocalStageKind.ToChannel, writer, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a sink that hands every element to a receiver that asks for it.</summary>
    /// <param name="controlSlot">The validated name the control is declared under.</param>
    /// <param name="controlType">The closed generic type of the control an author receives.</param>
    /// <param name="facade">The factory that wraps a run's rendezvous into that typed control.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The control end of a chain rather than its head, and otherwise the very shape a queue source has: a
    /// per-run object, named on the stage that produces it because a chain has one closing call and it is
    /// not here, resolved when the run starts because a receiver has to exist while the run is still
    /// running. It carries no payload at all, because a rendezvous has nothing to configure: its capacity
    /// is neither one nor a number the author chose, it is the demand itself.
    /// </remarks>
    internal static LocalStageDescriptor SinkProbe(ResultSlotId controlSlot, Type controlType, object facade) =>
        new(LocalStageKind.SinkProbe, facade, seed: null, LocalVocabulary.EmptyParameters, controlSlot, controlType);

    /// <summary>Returns a one-line diagnostic summary of this occurrence.</summary>
    /// <returns>The stage reference text, such as <c>local:select@1</c>.</returns>
    /// <remarks>The bound behavior is deliberately not rendered: a closure has no useful text form.</remarks>
    public override string ToString() => Stage.ToString();
}
