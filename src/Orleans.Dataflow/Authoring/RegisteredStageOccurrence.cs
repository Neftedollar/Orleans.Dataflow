using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One occurrence of a registered stage: the specification it resolved to, the name the author gave it,
/// and the parameter payload it carries.
/// </summary>
/// <remarks>
/// <para>
/// This is the whole of what a deployable occurrence is. There is no delegate and no captured state,
/// because a registered stage's behavior is resolved from a catalog by the identity in the document
/// (ADR 0001); everything this type holds either goes into the node or was already checked against the
/// catalog when the typed handle was created.
/// </para>
/// <para>
/// The name is required rather than optional, which is the deployable half of ADR 0004 section 6: a
/// registered occurrence exists to be addressed across an edit, a checkpoint, and an upgrade, and a
/// positional identifier anchors none of those. That is also why the only source of
/// <see cref="CapabilityToken.EphemeralIdentity"/> in a mixed graph is a lambda stage.
/// </para>
/// <para>
/// The specification travels with the occurrence rather than the catalog it came from. Everything the
/// builder needs — the stage reference, the parameter contract, the port names, the result contract, the
/// required capabilities — is on the specification, and holding the catalog would suggest that this value
/// can answer whether some graph is valid, which is the compiler's question against the host's catalog and
/// not the authoring value's.
/// </para>
/// </remarks>
internal sealed class RegisteredStageOccurrence : StageOccurrence
{
    private readonly StageSpecification _specification;
    private readonly NodeId _name;
    private readonly CanonicalJsonValue _parameters;

    /// <summary>Initializes a new instance of the <see cref="RegisteredStageOccurrence"/> class.</summary>
    /// <param name="specification">
    /// The resolved specification, already checked to have the linear shape of the handle that produced
    /// this occurrence.
    /// </param>
    /// <param name="name">The validated occurrence name.</param>
    /// <param name="parameters">The parameter payload, as the author supplied it.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The payload rules are <see cref="StageNode"/>'s and are applied by building a node here and
    /// discarding it. Stating them again would let the two drift apart, and deferring them to closure
    /// would report the mistake at the <c>To</c> that happens to follow rather than at the attachment the
    /// author wrote. The node is rebuilt at closure because that is where identifiers are allocated, and
    /// building it twice costs one small record.
    /// </remarks>
    internal RegisteredStageOccurrence(
        StageSpecification specification,
        NodeId name,
        CanonicalJsonValue parameters)
    {
        _ = StageNode.Create(name, specification.Stage, specification.ParameterContract, parameters);

        _specification = specification;
        _name = name;
        _parameters = parameters;
    }

    /// <summary>Gets the specification this occurrence resolved to when its handle was created.</summary>
    internal StageSpecification Specification => _specification;

    /// <inheritdoc/>
    internal override NodeId? Name => _name;

    /// <inheritdoc/>
    internal override StageRef Stage => _specification.Stage;

    /// <inheritdoc/>
    internal override ContractReference ParameterContract => _specification.ParameterContract;

    /// <inheritdoc/>
    internal override CanonicalJsonValue Parameters => _parameters;

    /// <inheritdoc/>
    /// <remarks>
    /// Read from the specification rather than from a constant, because a registered stage names its ports
    /// whatever it likes. The lookup is total: every handle kind fixes the port multiplicity at creation,
    /// so a stage that reaches this type has either exactly one input port or none.
    /// </remarks>
    internal override PortId? InputPort =>
        _specification.InputPorts.Count == 1 ? _specification.InputPorts[0].Id : null;

    /// <inheritdoc/>
    internal override PortId? OutputPort =>
        _specification.OutputPorts.Count == 1 ? _specification.OutputPorts[0].Id : null;

    /// <inheritdoc/>
    internal override ResultPortSpecification? ResultPort =>
        _specification.ResultPorts.Count == 1 ? _specification.ResultPorts[0] : null;

    /// <inheritdoc/>
    internal override IReadOnlyList<CapabilityToken> RequiredCapabilities =>
        _specification.RequiredCapabilities;

    /// <summary>Returns a one-line diagnostic summary of this occurrence.</summary>
    /// <returns>Text of the form <c>orders-in [orleans-test/order-source@v1]</c>.</returns>
    /// <remarks>
    /// The payload is deliberately not rendered: it can be up to
    /// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/> of JSON, and a log line has no use for that.
    /// </remarks>
    public override string ToString() => $"{_name} [{_specification.Stage}]";
}
