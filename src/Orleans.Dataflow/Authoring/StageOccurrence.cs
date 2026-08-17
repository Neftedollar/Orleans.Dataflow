using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One occurrence of one stage as an authoring value holds it, whatever kind of stage it is.
/// </summary>
/// <remarks>
/// <para>
/// An occurrence is not yet a node. It has no position and, for the lambda kind, no identity either,
/// because ADR 0004 allocates automatic identifiers at graph closure rather than at value creation: a
/// reusable <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/> occupies a different position in every graph it
/// is composed into. An authoring value therefore holds an ordered list of occurrences and composes by
/// concatenation; <see cref="LocalGraphBuilder"/> turns that list into identifiers, fragments, and a
/// document exactly once, at <c>To</c>.
/// </para>
/// <para>
/// The two kinds are <see cref="LocalStageDescriptor"/>, whose behavior is a delegate that never enters a
/// document, and <see cref="RegisteredStageOccurrence"/>, whose behavior is named by a
/// <see cref="StageRef"/> that resolves through a catalog. This class is exactly what the graph builder
/// needs of either: what the node says, which ports it leaves open, what it requires of its host, and
/// whether the author named it.
/// </para>
/// <para>
/// The members are deliberately document-shaped rather than kind-shaped. The builder never asks which
/// kind an occurrence is, which is what makes a mixed chain one code path instead of two, and what makes
/// the capability tokens of a closed document a fact derived from its occurrences rather than a constant
/// the builder decides.
/// </para>
/// </remarks>
internal abstract class StageOccurrence
{
    /// <summary>Gets the identifier the author gave this occurrence.</summary>
    /// <value>
    /// The explicit name, or <see langword="null"/> when the occurrence is numbered automatically at
    /// closure. A document containing an automatically numbered occurrence declares
    /// <see cref="CapabilityToken.EphemeralIdentity"/>, because positional identifiers are not edit-stable.
    /// </value>
    internal abstract NodeId? Name { get; }

    /// <summary>Gets the stage reference this occurrence declares in a document.</summary>
    internal abstract StageRef Stage { get; }

    /// <summary>Gets the parameter contract this occurrence declares in a document.</summary>
    internal abstract ContractReference ParameterContract { get; }

    /// <summary>Gets the parameter payload this occurrence writes into its node.</summary>
    internal abstract CanonicalJsonValue Parameters { get; }

    /// <summary>Gets the input port this occurrence leaves open for the stage before it.</summary>
    /// <value>The port name, or <see langword="null"/> when the occurrence consumes nothing.</value>
    internal abstract PortId? InputPort { get; }

    /// <summary>Gets the output port this occurrence leaves open for the stage after it.</summary>
    /// <value>The port name, or <see langword="null"/> when the occurrence produces nothing.</value>
    internal abstract PortId? OutputPort { get; }

    /// <summary>Gets the result port a slot declared over this occurrence is produced by.</summary>
    /// <value>
    /// The port and the contract of the value it yields, or <see langword="null"/> when the occurrence
    /// declares no result. A result port is never an open port: a result is exposed by declaring a slot
    /// against the closed graph, not by wiring an edge to it.
    /// </value>
    internal abstract ResultPortSpecification? ResultPort { get; }

    /// <summary>Gets the name this occurrence declares its runtime control under.</summary>
    /// <value>
    /// The slot name the author supplied when they wrote the stage, or <see langword="null"/> for every
    /// occurrence that produces no control.
    /// </value>
    /// <remarks>
    /// A control is named where it is written rather than where the graph is closed, because it belongs to
    /// a stage in the middle of a chain and there is no <c>To</c> to hand it back from. The builder reads
    /// this while it is already walking the chain and declares one more result slot for it, which is why a
    /// control needs no new closing overload and no second slot mechanism.
    /// </remarks>
    internal virtual ResultSlotId? ControlSlot => null;

    /// <summary>Gets the type of the runtime control this occurrence produces.</summary>
    /// <value>
    /// The closed generic interface an author receives, or <see langword="null"/> when there is no control.
    /// </value>
    /// <remarks>
    /// Recorded so that a closed graph can hand back a typed slot for a name without the author asserting
    /// the type: asking for the wrong one is then a diagnostic naming both types rather than a cast that
    /// fails inside a run.
    /// </remarks>
    internal virtual Type? ControlType => null;

    /// <summary>Gets the capability tokens a document containing this occurrence has to declare.</summary>
    /// <value>
    /// The tokens the occurrence's stage specification requires; every local stage requires
    /// <see cref="CapabilityToken.Nondeployable"/>, and a registered stage requires whatever its
    /// specification says. The graph compiler's <c>undeclared-capability</c> rule is what makes this the
    /// builder's business rather than a courtesy.
    /// </value>
    internal abstract IReadOnlyList<CapabilityToken> RequiredCapabilities { get; }
}
