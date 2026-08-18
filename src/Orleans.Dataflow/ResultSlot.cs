using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A typed declaration of one result a graph exposes, bound to the graph that declared it.
/// </summary>
/// <typeparam name="TResult">The type of the value the slot resolves to.</typeparam>
/// <remarks>
/// <para>
/// A slot is a declaration, never a value and never a promise: it names a result of a graph, and a run of
/// that graph resolves it. Materializing the same graph twice yields two runs and one slot resolves against
/// either, which is why the slot carries no run identity of its own.
/// </para>
/// <para>
/// <see cref="Graph"/> is the <see cref="GraphFingerprint"/> of the document that declared the slot, per
/// ADR 0004 section 4, and for a nondeployable graph the slot additionally binds to the built instance's
/// authoring nonce. The fingerprint identifies shape, not behavior: a document built from lambda stages
/// never records what its delegates compute, so two graphs of one shape share a fingerprint whatever
/// their lambdas do. The nonce closes exactly that gap — a slot resolves only against a run of the very
/// graph instance that declared it, so misuse fails loudly instead of silently reading a graph that
/// merely looks the same.
/// </para>
/// <para>
/// A slot of a <see cref="PipelineDefinition"/> carries no nonce, and says so by carrying the reserved
/// value <see cref="Guid.Empty"/>. Registered stages carry their identity and their parameters in the
/// document, so a pipeline's content identity means something on its own and a per-instance nonce would
/// distinguish nothing (ADR 0004 section 4). The reserved value is not an absence: it is what makes the
/// two worlds tellable apart, so a run of a pipeline refuses a built graph's slot and a run of a built
/// graph refuses a pipeline's, each naming which world the slot came from rather than reporting two
/// fingerprints that happen to differ.
/// </para>
/// <para>
/// A slot a <see cref="Branch{TIn}"/> declared is the one slot that exists before its graph does, and it is
/// worth stating plainly rather than leaving to the mechanics. A branch is built as an argument of the
/// junction call that consumes it, so its sink and its name are fixed one expression before there is a
/// document to fingerprint; the slot therefore names its graph from that junction call onwards, and reading
/// <see cref="Graph"/> before then throws rather than answering with a fingerprint of nothing. The window is
/// the width of one argument list, and a branch that declares a result closes exactly one graph — consuming
/// it twice is refused, because the second graph would otherwise quietly take the first one's slot.
/// </para>
/// <para>
/// Equality is over three components, and each one is load-bearing: the slot <see cref="Id"/>, because a
/// graph may declare several results; the declaring document's <see cref="Graph"/> fingerprint, because a
/// name means nothing apart from the document that declared it; and the declaring instance's authoring
/// nonce, because a fingerprint covers shape and not behavior, so without it two lambda graphs that merely
/// look alike would resolve each other's results. The nonce is internal — nothing outside materialization
/// has a use for it — but it is part of what makes two slots equal, which is why two slots of the same
/// name on two runs of look-alike graphs are not.
/// </para>
/// <para>
/// The type is a readonly record struct for three reasons: equality over those components is the whole
/// contract and the synthesized equality is exactly it; the components are themselves readonly record
/// structs whose value equality it composes; and it matches how every other small identity in this
/// codebase is modeled. The default instance is meaningless and says so, the way
/// <see cref="PortAddress"/> does.
/// </para>
/// </remarks>
public readonly record struct ResultSlot<TResult>
{
    /// <summary>The number of hexadecimal digits of the authoring nonce <see cref="ToString"/> renders.</summary>
    private const int NonceDigits = 8;

    private readonly ResultSlotId _id;
    private readonly GraphFingerprint _graph;
    private readonly Guid _authoringNonce;
    private readonly BranchSlotBinding? _branch;

    /// <summary>Initializes a new instance of the <see cref="ResultSlot{TResult}"/> struct.</summary>
    /// <param name="id">The slot name, already validated.</param>
    /// <param name="graph">The fingerprint of the declaring document.</param>
    /// <param name="authoringNonce">The per-instance identity of the declaring graph.</param>
    private ResultSlot(ResultSlotId id, GraphFingerprint graph, Guid authoringNonce)
    {
        _id = id;
        _graph = graph;
        _authoringNonce = authoringNonce;
        _branch = null;
    }

    /// <summary>Initializes a new instance of the <see cref="ResultSlot{TResult}"/> struct on a branch.</summary>
    /// <param name="id">The slot name, already validated.</param>
    /// <param name="branch">The binding the junction call fills when it closes the graph.</param>
    private ResultSlot(ResultSlotId id, BranchSlotBinding branch)
    {
        _id = id;
        _graph = default;
        _authoringNonce = Guid.Empty;
        _branch = branch;
    }

    /// <summary>Gets the author-chosen name of this slot.</summary>
    /// <value>The slot identifier, which is unique within its graph.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public ResultSlotId Id => IsDefault ? throw DefaultAccess() : _id;

    /// <summary>Gets the fingerprint of the graph document that declared this slot.</summary>
    /// <value>The declaring document's identity.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, or it was declared by a branch whose junction call has not
    /// closed a graph yet.
    /// </exception>
    public GraphFingerprint Graph => IsDefault ? throw DefaultAccess() : _branch?.Graph ?? _graph;

    /// <summary>Gets the per-instance identity of the graph that declared this slot.</summary>
    /// <value>The declaring <see cref="RunnableGraph"/>'s authoring nonce.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, or it was declared by a branch whose junction call has not
    /// closed a graph yet.
    /// </exception>
    /// <remarks>
    /// Internal: the nonce is how a run rejects a slot of a different graph instance, and nothing outside
    /// materialization has a use for it.
    /// </remarks>
    internal Guid AuthoringNonce => IsDefault ? throw DefaultAccess() : _branch?.AuthoringNonce ?? _authoringNonce;

    /// <summary>Gets a value indicating whether this slot was declared by a pipeline rather than a built graph.</summary>
    /// <value>
    /// <see langword="true"/> when the declaring value was a <see cref="PipelineDefinition"/>;
    /// <see langword="false"/> when it was a <see cref="RunnableGraph"/> instance.
    /// </value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    /// <remarks>
    /// The two worlds are told apart by the reserved nonce <see cref="Guid.Empty"/>, which
    /// <see cref="PipelineDefinition.ResultSlot{TResult}"/> stamps and <see cref="RunnableGraph"/> never
    /// produces because it allocates its nonce with <see cref="Guid.NewGuid"/>. Which world a slot belongs
    /// to is a categorical fact about it, checked before which graph declared it, so a caller who crossed
    /// the two planes is told that rather than being told two fingerprints differ.
    /// </remarks>
    internal bool IsPipelineSlot => AuthoringNonce == Guid.Empty;

    /// <summary>Gets a value indicating whether this instance is the uninitialized default.</summary>
    /// <value><see langword="true"/> when the instance names no slot.</value>
    /// <remarks>The default instance arises only from <c>default(ResultSlot&lt;T&gt;)</c>; no API returns one.</remarks>
    public bool IsDefault => _id.IsDefault;

    /// <summary>Gets the declaring document's fingerprint without throwing for an unbound branch slot.</summary>
    /// <value>The fingerprint, or the default value when no graph has been closed over the branch yet.</value>
    /// <remarks>
    /// Equality and text rendering read the components through this and <see cref="Instance"/>, because
    /// neither operation may fail: a slot has to be comparable and loggable in every state it can be in,
    /// and one of those states is a branch slot whose junction call has not run.
    /// </remarks>
    private GraphFingerprint Declaring => _branch?.GraphOrDefault ?? _graph;

    /// <summary>Gets the declaring instance's nonce without throwing for an unbound branch slot.</summary>
    /// <value>The nonce, or <see cref="Guid.Empty"/> when no graph has been closed over the branch yet.</value>
    private Guid Instance => _branch?.AuthoringNonceOrDefault ?? _authoringNonce;

    /// <summary>Determines whether this slot and <paramref name="other"/> name the same result.</summary>
    /// <param name="other">The slot to compare with.</param>
    /// <returns>
    /// <see langword="true"/> when both slots have the same name, the same declaring document, and the same
    /// declaring instance.
    /// </returns>
    /// <remarks>
    /// Written out rather than synthesized, so that equality stays over exactly the three components the
    /// type documents even though a branch slot reaches two of them through a binding it shares with its
    /// branch. A branch slot whose graph has been closed is therefore equal to a slot of that same graph
    /// under that same name, which is what "the same result" has to mean; two unbound branch slots of one
    /// name are equal to each other, because neither names a graph yet and there is nothing else to tell
    /// them apart. An unbound slot is consequently not a stable dictionary key — its components change once
    /// the junction call closes the graph — and it is not meant to be one: the window in which a branch slot
    /// is unbound is the width of the argument list it was written in.
    /// </remarks>
    public bool Equals(ResultSlot<TResult> other) =>
        _id == other._id && Declaring == other.Declaring && Instance == other.Instance;

    /// <summary>Returns a hash code over the three components equality is defined by.</summary>
    /// <returns>A hash code consistent with <see cref="Equals(ResultSlot{TResult})"/>.</returns>
    public override int GetHashCode() => HashCode.Combine(_id, Declaring, Instance);

    /// <summary>Returns the text form, or a diagnostic literal for the default value.</summary>
    /// <returns>
    /// Text of the form <c>processed@sha256:9f86d081...#4f1c9a2b</c>, or <c>"(default ResultSlot)"</c>
    /// when <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// All three components a slot is equal by are rendered, because a text form that showed only two of
    /// them would print one line for two slots that are not equal — exactly the confusion the nonce
    /// exists to prevent, reintroduced in the logs. The nonce is abbreviated to its first eight
    /// hexadecimal digits: the text is a diagnostic label rather than a durable identity, and eight
    /// digits are enough to tell apart the handful of graph instances one program builds.
    /// </para>
    /// <para>
    /// The separators are <c>@</c> and <c>#</c>, neither of which is a character of the identifier
    /// grammar, and the method never throws, so logging stays safe for every instance including the
    /// default one.
    /// </para>
    /// </remarks>
    public override string ToString() =>
        IsDefault
            ? "(default ResultSlot)"
            : _branch is { IsBound: false }
                ? $"{_id}@(unclosed branch)"
                : $"{_id}@{Declaring}#{Instance.ToString("N", CultureInfo.InvariantCulture)[..NonceDigits]}";

    /// <summary>Creates a slot bound to the graph instance that declared it.</summary>
    /// <param name="id">The validated slot name.</param>
    /// <param name="graph">The declaring document's fingerprint.</param>
    /// <param name="authoringNonce">The declaring graph instance's nonce.</param>
    /// <returns>The slot.</returns>
    /// <remarks>
    /// Internal because a slot is only ever produced by closing a graph. There is no supported way to
    /// assert that some name is a slot of some graph without having built the graph.
    /// </remarks>
    internal static ResultSlot<TResult> Create(ResultSlotId id, GraphFingerprint graph, Guid authoringNonce) =>
        new(id, graph, authoringNonce);

    /// <summary>Creates a slot a branch declared, bound to the graph the branch will close.</summary>
    /// <param name="id">The validated slot name.</param>
    /// <param name="branch">The binding the junction call fills.</param>
    /// <returns>The slot.</returns>
    /// <remarks>
    /// The one slot that exists before its graph does. A branch is written as an argument of the junction
    /// call that consumes it, so its sink — and therefore its result name — is fixed one expression before
    /// there is a document to fingerprint; ADR 0006 chose that spelling because everything else about a
    /// branch infers from it. The slot is complete in every other respect and becomes complete in this one
    /// the moment the junction call closes the graph.
    /// </remarks>
    internal static ResultSlot<TResult> OnBranch(ResultSlotId id, BranchSlotBinding branch) => new(id, branch);

    /// <summary>Builds the exception for reading a component of the default instance.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(ResultSlot<TResult>)} carries no value. Obtain a slot by closing a graph with a result-bearing sink instead of using the uninitialized struct.");
}
