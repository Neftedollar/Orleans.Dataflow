using System.Collections;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A graph reduced to the shape one run executes: where elements come from, what happens to each of them,
/// and what terminates them.
/// </summary>
/// <remarks>
/// <para>
/// The plan is the runnable artifact <see cref="Compilation.GraphCompiler"/> deliberately does not
/// produce: validation is a statement about a document, and turning a validated document into something
/// that runs is the runtime's job. A plan is built once per materialization, so two runs of one graph
/// share no delegate wrapper, no seed, and no enumerator.
/// </para>
/// <para>
/// The plan is a description and holds no run state. The fold's running state lives in
/// <see cref="LocalRun"/>, which is why <see cref="Seed"/> is here and the state is not: a fresh run starts
/// from the same seed the author wrote, and never from where another run left off.
/// </para>
/// <para>
/// What two runs do share is what the author shared with them: the same sequence instance, the same
/// delegate instances, and therefore whatever those delegates captured. A run isolates the state it owns —
/// its enumerator, its fold state, its wrappers — and cannot isolate state an author put outside the
/// graph. A lambda closing over a counter counts across every run of every graph it was composed into,
/// which is the author's arrangement and not the runtime's to undo.
/// </para>
/// <para>
/// The shape is exactly the linear chain the local authoring vocabulary can express: one source, any
/// number of mappings and filters, one terminal. Fan-out, fan-in, and cycles are later milestones and have
/// no representation here rather than an unimplemented one.
/// </para>
/// </remarks>
internal sealed class LocalRunPlan
{
    /// <summary>Initializes a new instance of the <see cref="LocalRunPlan"/> class.</summary>
    /// <param name="elements">The sequence the source enumerates.</param>
    /// <param name="stages">The element stages between the source and the terminal, in flow order.</param>
    /// <param name="folder">The terminal fold, or <see langword="null"/> when the terminal discards.</param>
    /// <param name="seed">The fold's initial state, meaningful only when <paramref name="folder"/> is not null.</param>
    /// <param name="slot">The result slot the fold's final state resolves, or <see langword="null"/>.</param>
    internal LocalRunPlan(
        IEnumerable elements,
        IReadOnlyList<LocalElementStage> stages,
        Func<object?, object?, object?>? folder,
        object? seed,
        ResultSlotId? slot)
    {
        Elements = elements;
        Stages = stages;
        Folder = folder;
        Seed = seed;
        Slot = slot;
    }

    /// <summary>Gets the sequence the source enumerates.</summary>
    /// <value>The very sequence the author handed to <see cref="Source.From{T}"/>.</value>
    /// <remarks>
    /// The sequence is not enumerated here. A run obtains its own enumerator when it starts, which is what
    /// makes two materializations of one graph two independent enumerations.
    /// </remarks>
    internal IEnumerable Elements { get; }

    /// <summary>Gets the element stages between the source and the terminal, in flow order.</summary>
    /// <value>The mappings and filters, which is empty for a graph that only sources and terminates.</value>
    internal IReadOnlyList<LocalElementStage> Stages { get; }

    /// <summary>Gets the terminal fold.</summary>
    /// <value>The folder over boxed state and boxed elements, or <see langword="null"/> when the terminal discards.</value>
    internal Func<object?, object?, object?>? Folder { get; }

    /// <summary>Gets the fold's initial state.</summary>
    /// <value>
    /// The seed the author wrote, which may legitimately be <see langword="null"/>; <see cref="Folder"/>
    /// and not this value decides whether a fold exists.
    /// </value>
    internal object? Seed { get; }

    /// <summary>Gets the result slot the fold's final state resolves.</summary>
    /// <value>
    /// The slot name the document declares, or <see langword="null"/> when the graph exposes no result.
    /// </value>
    /// <remarks>
    /// A fold with no slot is a real case rather than a defect: converting a result-bearing sink through
    /// <see cref="SinkWithResult{TIn, TResult}.ToSink"/> keeps the fold and drops the declaration, so the
    /// run still folds every element and simply exposes nothing to ask for.
    /// </remarks>
    internal ResultSlotId? Slot { get; }
}
