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
    /// The one local input port name for every shape that consumes one stream; <see langword="null"/> for a
    /// source, which consumes none, and for a fan-in junction, which consumes several.
    /// </value>
    /// <remarks>
    /// A fan-in answers <see langword="null"/> here for the reason a fan-out answers it for its outputs:
    /// this member is what the chain-composing builder connects to, and a junction is not something a chain
    /// can hold. Deriving the answer from the declared port list rather than from a place is what makes that
    /// true by construction instead of by a rule written twice.
    /// </remarks>
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
        LocalVocabulary.RequiredCapabilitiesOf(Kind);

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

    /// <summary>Creates a source that emits the number of every tick of an interval.</summary>
    /// <param name="initialDelay">The validated delay before the first tick.</param>
    /// <param name="interval">The validated interval between ticks.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The third shape with no delegate at all, after the buffer and the range: its elements are the tick
    /// numbers, and two durations say exactly which ones and when. What it does not say is which clock
    /// measures them, because a clock is a property of the run and never of the document — the host's
    /// <see cref="TimeProvider"/> is resolved at materialization (ADR 0005).
    /// </remarks>
    internal static LocalStageDescriptor Tick(TimeSpan initialDelay, TimeSpan interval) =>
        new(
            LocalStageKind.Tick,
            behavior: null,
            seed: null,
            LocalTickParameters.Write(initialDelay, interval));

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

    /// <summary>Creates a running fold whose state a durable scope can write into a checkpoint.</summary>
    /// <param name="seed">The initial state, which may be <see langword="null"/>.</param>
    /// <param name="folder">The folding delegate, as the authoring value received it.</param>
    /// <param name="export">The projection of the boxed state into a canonical value.</param>
    /// <param name="restore">The projection back, already closed over the state type.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The same stage and the same payload as <see cref="Scan(object?, object)"/> — a codec is behavior, so
    /// two graphs whose scans differ only in carrying one have one fingerprint — with a binding of three
    /// values instead of one. That is the split every stage of this vocabulary makes, applied to the one
    /// question a document could not answer: what a state of an unnamed type looks like written down.
    /// </remarks>
    internal static LocalStageDescriptor Scan(
        object? seed,
        object folder,
        Func<object?, CanonicalJsonValue> export,
        Func<CanonicalJsonValue, object?> restore) =>
        new(LocalStageKind.Scan, new object?[] { folder, export, restore }, seed, LocalVocabulary.EmptyParameters);

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

    /// <summary>Creates a stage that drops an element equal to the one immediately before it.</summary>
    /// <param name="comparer">
    /// The element type's default equality, which is an <see cref="System.Collections.IEqualityComparer"/>.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// No payload at all, unlike <see cref="Distinct"/>: this stage's bound is one element and is a fact
    /// about the shape rather than a number an author chose, so there is nothing for a document to state.
    /// </remarks>
    internal static LocalStageDescriptor DeduplicateConsecutive(object comparer) =>
        new(LocalStageKind.DeduplicateConsecutive, comparer, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that replaces every element with the sequence a function answers.</summary>
    /// <param name="selector">The function, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The whole of the stage is the function, so the payload is empty. What it does to the shape of the
    /// stream — one element in, a sequence out — is the stage reference's to say.
    /// </remarks>
    internal static LocalStageDescriptor SelectMany(object selector) =>
        new(LocalStageKind.SelectMany, selector, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that merges the sequences a function answers, several at a time.</summary>
    /// <param name="options">The validated bound on how many of those sequences are open at once.</param>
    /// <param name="selector">The function, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The parallelism contract, shared with the asynchronous stages for the reason they share it with each
    /// other: a bound on concurrent work is a number a document can state, and what that work is is the
    /// stage reference's to say. Both spellings of the function — an asynchronous inner sequence and an
    /// ordinary one — write this same node, because what the author's sequence does to produce its elements
    /// is behavior in exactly the way the body of a mapping function is.
    /// </remarks>
    internal static LocalStageDescriptor MergeMap(ParallelismOptions options, object selector) =>
        new(LocalStageKind.MergeMap, selector, seed: null, LocalParallelismParameters.Write(options));

    /// <summary>Creates a running fold whose function is asynchronous.</summary>
    /// <param name="seed">The initial state, which may be <see langword="null"/>.</param>
    /// <param name="folder">The folding delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// No payload, and the absence is the contract: one fold of this stage runs at a time because the next
    /// one folds this one's answer, so there is no bound for an author to declare and none for a document to
    /// carry.
    /// </remarks>
    internal static LocalStageDescriptor ScanAsync(object? seed, object folder) =>
        new(LocalStageKind.ScanAsync, folder, seed, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a stage that collects a declared number of elements into one list.</summary>
    /// <param name="size">The validated group size.</param>
    /// <param name="freeze">The projection of a group into the typed list the author declared.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The count contract, shared with <c>take</c>, <c>skip</c>, and <c>repeat</c> for the reason those
    /// three share it: a count is a count, and which of them a node is is the stage reference's job to say.
    /// </remarks>
    internal static LocalStageDescriptor Grouped(int size, object freeze) =>
        new(LocalStageKind.Grouped, freeze, seed: null, LocalCountParameters.Write(size));

    /// <summary>Creates a stage that emits a window of a declared size, advancing by a declared step.</summary>
    /// <param name="size">The validated window size.</param>
    /// <param name="step">The validated step.</param>
    /// <param name="freeze">The projection of a window into the typed list the author declared.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Sliding(int size, int step, object freeze) =>
        new(LocalStageKind.Sliding, freeze, seed: null, LocalWindowParameters.Write(size, step));

    /// <summary>Creates a stage that closes a group by a declared count or by a declared window.</summary>
    /// <param name="maxElements">The validated element bound.</param>
    /// <param name="window">The validated window.</param>
    /// <param name="freeze">The projection of a group into the typed list the author declared.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor GroupedWithin(int maxElements, TimeSpan window, object freeze) =>
        new(
            LocalStageKind.GroupedWithin,
            freeze,
            seed: null,
            LocalGroupedWithinParameters.Write(maxElements, window));

    /// <summary>Creates a stage that closes a group by a count, a weight, or a window.</summary>
    /// <param name="maxElements">The validated element bound.</param>
    /// <param name="maxWeight">The validated weight bound.</param>
    /// <param name="window">The validated window.</param>
    /// <param name="cost">The cost function, as the authoring value received it.</param>
    /// <param name="freeze">The projection of a group into the typed list the author declared.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The three bounds are the payload and the cost function is the binding, which is the same split a
    /// throttle by cost makes and for the same reason: a bound is a number a document can state and a
    /// function is never durable topology.
    /// </remarks>
    internal static LocalStageDescriptor GroupedWeightedWithin(
        int maxElements,
        int maxWeight,
        TimeSpan window,
        object cost,
        object freeze) =>
        new(
            LocalStageKind.GroupedWeightedWithin,
            new object?[] { cost, freeze },
            seed: null,
            LocalGroupedWeightedParameters.Write(maxElements, maxWeight, window));

    /// <summary>Creates a stage that runs one instance of a chain of element stages per key.</summary>
    /// <param name="options">The validated bound on active keys and the policy past it.</param>
    /// <param name="keySelector">The key function, as the authoring value received it.</param>
    /// <param name="comparer">The key type's own equality.</param>
    /// <param name="group">The validated stages of the group flow, in flow order.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The one descriptor whose payload carries other descriptors' payloads. The split is the usual one
    /// read one level down: what a key is, what the key type's equality is, and what each stage of the
    /// group flow does are behavior; how many keys may be active, what the key past that costs, and
    /// <em>which stages the group flow is</em> are configuration a document states. The binding holds the
    /// descriptors themselves rather than only their delegates, because the runtime needs both halves of
    /// each of them and reading the payload against the binding is what makes the two planes agree.
    /// </remarks>
    internal static LocalStageDescriptor GroupBy(
        GroupByOptions options,
        object keySelector,
        object comparer,
        IReadOnlyList<LocalStageDescriptor> group) =>
        new(
            LocalStageKind.GroupBy,
            new object?[] { keySelector, comparer, group },
            seed: null,
            LocalGroupByParameters.Write(options, group));

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

    /// <summary>Creates a stage that holds every element for a declared duration.</summary>
    /// <param name="delay">The validated duration each element is held for.</param>
    /// <param name="holdback">The validated bound on how many are held at once, and its overflow policy.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// No behavior at all: a delay is decided entirely by its numbers, so there is nothing left for a
    /// binding to say. The holdback is payload for the reason a buffer's capacity is — it is a number that
    /// changes what the graph does — and the duration for the same reason.
    /// </remarks>
    internal static LocalStageDescriptor Delay(TimeSpan delay, BufferOptions holdback) =>
        new(
            LocalStageKind.Delay,
            behavior: null,
            seed: null,
            LocalDelayParameters.Write(delay, holdback));

    /// <summary>Creates a stage configured by one duration.</summary>
    /// <param name="kind">
    /// Which of the four the occurrence is: an initial delay, a timeout, or one of the two windows.
    /// </param>
    /// <param name="duration">The validated duration.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// One factory for the four shapes that carry a duration and nothing else, because the descriptor they
    /// build differs in exactly one field — the kind — and a second factory per shape would be four copies
    /// of one line. The stage reference the kind derives is what says which of them a node is.
    /// </remarks>
    internal static LocalStageDescriptor Timed(LocalStageKind kind, TimeSpan duration) =>
        new(kind, behavior: null, seed: null, LocalDurationParameters.Write(duration));

    /// <summary>Creates a stage that holds a stream to a declared rate.</summary>
    /// <param name="options">The validated rate, burst, and mode.</param>
    /// <param name="cost">
    /// The function answering what one element costs, as the authoring value received it, or
    /// <see langword="null"/> when every element costs one.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The cost function is behavior for the reason a partition's router is: what an element costs is a
    /// statement about an element type, and an element type never appears in a local document. Its absence
    /// is behavior too — an occurrence with no cost function charges one per element — and that is why the
    /// binding may legitimately be <see langword="null"/> here.
    /// </remarks>
    internal static LocalStageDescriptor Throttle(ThrottleOptions options, object? cost) =>
        new(LocalStageKind.Throttle, cost, seed: null, LocalThrottleParameters.Write(options));

    /// <summary>Creates a stage that holds elements while its control is closed.</summary>
    /// <param name="mode">The validated state the valve starts a run in.</param>
    /// <param name="controlSlot">The validated name the control is declared under.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The one control-bearing shape with no behavior at all. A queue and a probe carry a factory that
    /// wraps the runtime's object into the author's element type; a valve has no element type to wrap, so
    /// the runtime object is the control an author receives and the binding has nothing to say.
    /// </remarks>
    internal static LocalStageDescriptor Valve(ValveMode mode, ResultSlotId controlSlot) =>
        new(
            LocalStageKind.Valve,
            behavior: null,
            seed: null,
            LocalValveParameters.Write(mode),
            controlSlot,
            typeof(IValve));

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

    /// <summary>Creates a junction that delivers each element to the one output its function names.</summary>
    /// <param name="router">The routing function, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The router is behavior for the reason an unzip's projections are: which leg an element belongs on is
    /// a statement about an element type, and an element type never appears in a local document. What the
    /// document states is that this node routes one stream into several, which is the part that is
    /// topology; how many legs it has is stated by its edges, so there is no payload here either.
    /// </remarks>
    internal static LocalStageDescriptor Partition(object router) =>
        new(LocalStageKind.Partition, router, seed: null, LocalVocabulary.EmptyParameters);

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

    /// <summary>Creates a junction that emits whichever of its inputs has an element.</summary>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// No behavior and no payload at all, for the reason a broadcast has neither: a merge is decided
    /// entirely by the edges its inputs carry, and elements pass through it untouched, so there is nothing
    /// left for a binding to say and nothing a document could state twice.
    /// </remarks>
    internal static LocalStageDescriptor Merge() =>
        new(LocalStageKind.Merge, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a junction that emits each of its inputs to its end before reading the next.</summary>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Concat() =>
        new(LocalStageKind.Concat, behavior: null, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a junction that emits a declared number of elements from each input in turn.</summary>
    /// <param name="segmentSize">The validated number of elements taken from one input before the next.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The one junction with a payload of its own. How many inputs the rotation runs over is stated by the
    /// edges, as it is for every junction; how many elements it takes from each of them before moving on is
    /// a number that changes the sequence the graph produces, so it is written into the document and into
    /// the fingerprint taken over it.
    /// </remarks>
    internal static LocalStageDescriptor Interleave(int segmentSize) =>
        new(
            LocalStageKind.Interleave,
            behavior: null,
            seed: null,
            LocalInterleaveParameters.Write(segmentSize));

    /// <summary>Creates a junction that emits one row per element from each of its inputs.</summary>
    /// <param name="combiner">
    /// The combiner of a row from the inputs' elements in port order, as the authoring value received it.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The combiner is behavior for the reason an unzip's halves are: which member of a row each input
    /// contributes is a statement about element types, and an element type never appears in a local
    /// document. What the document states is that this node joins several streams into one, which is the
    /// part that is topology — and it states how many through its edges, so a zip carries no payload at all.
    /// </remarks>
    internal static LocalStageDescriptor Zip(object combiner) =>
        new(LocalStageKind.Zip, combiner, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a junction that emits a row of every input's latest element on every arrival.</summary>
    /// <param name="combiner">
    /// The combiner of a row from the inputs' latest elements in port order, as the authoring value received
    /// it.
    /// </param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The same split a zip makes, and for the same reason. What separates the two is not anything either
    /// writes down but what its pump does with the elements it reads, which is why they are two stage
    /// references rather than one with a mode: a mode would be a parameter, and the difference between
    /// pairing positionally and remembering the latest is not configuration.
    /// </remarks>
    internal static LocalStageDescriptor CombineLatest(object combiner) =>
        new(LocalStageKind.CombineLatest, combiner, seed: null, LocalVocabulary.EmptyParameters);

    /// <summary>Creates a folding sink whose function is asynchronous.</summary>
    /// <param name="seed">The initial state, which may be <see langword="null"/>.</param>
    /// <param name="folder">The folding delegate, as the authoring value received it.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The result-bearing asynchronous terminal, and it carries no payload for the reason the asynchronous
    /// scan carries none: one fold runs at a time by construction, so there is no bound to declare. That is
    /// what separates it from <see cref="ForEachAsync"/>, which declares one because its callbacks are
    /// independent of each other.
    /// </remarks>
    internal static LocalStageDescriptor FoldAsync(object? seed, object folder) =>
        new(LocalStageKind.FoldAsync, folder, seed, LocalVocabulary.EmptyParameters);

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

    /// <summary>Creates a stage that throws where its declared arming says to.</summary>
    /// <param name="mode">The validated mode.</param>
    /// <param name="firstFailure">The validated one-based position of the first failing arrival.</param>
    /// <param name="fault">The factory of what to throw, over the one-based position of the arrival.</param>
    /// <param name="controlSlot">
    /// The validated name the control is declared under, or <see langword="null"/> for an occurrence that
    /// exposes none.
    /// </param>
    /// <param name="controlType">The closed type of the control an author receives, when there is one.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// <para>
    /// The arming is payload and what is thrown is binding, which is this vocabulary's usual split read over
    /// an unusual stage: when a fault point throws changes the stream a graph produces and belongs in the
    /// fingerprint, and an exception is a value of a type no local document names.
    /// </para>
    /// <para>
    /// The one occurrence in this vocabulary whose control slot is optional. A fault point standing inside a
    /// supervision scope is not a node — the stages of an inner chain have no identity — so there is nothing
    /// for a slot to name; its declared arming is the whole of what such an occurrence needs, and the
    /// authoring surface refuses a control-bearing one there rather than declaring a slot nothing could
    /// resolve.
    /// </para>
    /// </remarks>
    internal static LocalStageDescriptor FaultPoint(
        LocalFaultMode mode,
        int firstFailure,
        object fault,
        ResultSlotId? controlSlot,
        Type? controlType) =>
        new(
            LocalStageKind.FaultPoint,
            fault,
            seed: null,
            LocalFaultPointParameters.Write(mode, firstFailure),
            controlSlot,
            controlType);

    /// <summary>Creates a stage that answers the failures of the chain it owns.</summary>
    /// <param name="options">The validated policy.</param>
    /// <param name="fallback">
    /// The element a recovering scope emits, boxed by the caller; <see langword="null"/> and meaningless for
    /// every other form, and legitimately <see langword="null"/> for a nullable element type.
    /// </param>
    /// <param name="scope">The validated stages of the scope's chain, in flow order.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The second descriptor whose payload carries other descriptors' payloads, and it makes the same split
    /// <see cref="GroupBy"/> makes, read over a policy instead of over a key: which form the scope takes, how
    /// many attempts one element gets, how long it waits, what exhaustion costs, and <em>which stages the
    /// chain is</em> are configuration a document states; what each of those stages does and what a
    /// recovering scope emits are behavior. The binding holds the descriptors themselves rather than only
    /// their delegates, because the runtime needs both halves of each of them and reading the payload against
    /// the binding is what makes the two planes agree.
    /// </remarks>
    internal static LocalStageDescriptor Supervised(
        SupervisionOptions options,
        object? fallback,
        IReadOnlyList<LocalStageDescriptor> scope) =>
        new(
            LocalStageKind.Supervised,
            new object?[] { fallback, scope },
            seed: null,
            LocalSupervisionParameters.Write(options, scope));

    /// <summary>Creates a stage whose chain's state survives a resume.</summary>
    /// <param name="scope">The validated stages of the scope's chain, in flow order.</param>
    /// <returns>The descriptor.</returns>
    /// <remarks>
    /// The third descriptor whose payload carries other descriptors' payloads, and the one with nothing else
    /// in it: a durable scope declares which stages it is made of and nothing more, because when and where a
    /// checkpoint is taken is the run's option rather than the graph's. The binding holds the descriptors
    /// themselves for the reason the supervision scope's does — the runtime needs both halves of each of
    /// them, and reading the payload against the binding is what makes the two planes agree.
    /// </remarks>
    internal static LocalStageDescriptor Durable(IReadOnlyList<LocalStageDescriptor> scope) =>
        new(LocalStageKind.Durable, scope, seed: null, LocalDurableParameters.Write(scope));

    /// <summary>Creates a sink whose commit mark advances after its callback.</summary>
    /// <param name="callback">The side-effect delegate, as the authoring value received it.</param>
    /// <param name="controlSlot">The validated name the control is declared under.</param>
    /// <param name="controlType">The closed type of the control an author receives.</param>
    /// <param name="facade">The factory of that control over the run's marking sink.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor MarkingSink(
        object callback,
        ResultSlotId controlSlot,
        Type controlType,
        object facade) =>
        new(
            LocalStageKind.MarkingSink,
            new object?[] { callback, facade },
            seed: null,
            LocalVocabulary.EmptyParameters,
            controlSlot,
            controlType);

    /// <summary>Returns a one-line diagnostic summary of this occurrence.</summary>
    /// <returns>The stage reference text, such as <c>local:select@1</c>.</returns>
    /// <remarks>The bound behavior is deliberately not rendered: a closure has no useful text form.</remarks>
    public override string ToString() => Stage.ToString();
}
