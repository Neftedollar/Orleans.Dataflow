using System.Globalization;
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

    /// <summary>Initializes a new instance of the <see cref="ResultSlot{TResult}"/> struct.</summary>
    /// <param name="id">The slot name, already validated.</param>
    /// <param name="graph">The fingerprint of the declaring document.</param>
    /// <param name="authoringNonce">The per-instance identity of the declaring graph.</param>
    private ResultSlot(ResultSlotId id, GraphFingerprint graph, Guid authoringNonce)
    {
        _id = id;
        _graph = graph;
        _authoringNonce = authoringNonce;
    }

    /// <summary>Gets the author-chosen name of this slot.</summary>
    /// <value>The slot identifier, which is unique within its graph.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public ResultSlotId Id => IsDefault ? throw DefaultAccess() : _id;

    /// <summary>Gets the fingerprint of the graph document that declared this slot.</summary>
    /// <value>The declaring document's identity.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    public GraphFingerprint Graph => IsDefault ? throw DefaultAccess() : _graph;

    /// <summary>Gets the per-instance identity of the graph that declared this slot.</summary>
    /// <value>The declaring <see cref="RunnableGraph"/>'s authoring nonce.</value>
    /// <exception cref="InvalidOperationException">This instance is the default value.</exception>
    /// <remarks>
    /// Internal: the nonce is how a run rejects a slot of a different graph instance, and nothing outside
    /// materialization has a use for it.
    /// </remarks>
    internal Guid AuthoringNonce => IsDefault ? throw DefaultAccess() : _authoringNonce;

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
            : $"{_id}@{_graph}#{_authoringNonce.ToString("N", CultureInfo.InvariantCulture)[..NonceDigits]}";

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

    /// <summary>Builds the exception for reading a component of the default instance.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(ResultSlot<TResult>)} carries no value. Obtain a slot by closing a graph with a result-bearing sink instead of using the uninitialized struct.");
}
