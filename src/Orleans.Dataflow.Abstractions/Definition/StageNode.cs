using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// One occurrence of a registered stage inside a graph document.
/// </summary>
/// <remarks>
/// <para>
/// A node is data about behavior, never behavior itself. It names the stage family through a
/// <see cref="StageRef"/> that resolves against a catalog registered by deployment code, and it carries
/// the stage's configuration as a canonical JSON payload plus the contract that payload claims to satisfy
/// (ADR 0001, ADR 0003). Nothing here can cause code loading.
/// </para>
/// <para>
/// The declared <see cref="ParameterContract"/> is stored next to the payload rather than being inferred
/// from the catalog, so a document that was written against a different contract version is detected as a
/// mismatch instead of being reinterpreted under today's contract.
/// </para>
/// <para>
/// The execution policy is optional and its two members move together: a node either declares both a
/// policy contract and a policy payload, or neither, in which case the provider default applies. The
/// factory overloads make the invalid halfway state unrepresentable through the public surface.
/// </para>
/// <para>
/// Equality is structural over every member. Each member is a value type with value equality, including
/// <see cref="CanonicalJsonValue"/>, which compares its canonical bytes, so two nodes built independently
/// from the same inputs are equal even though nothing is shared between them.
/// </para>
/// </remarks>
public sealed record class StageNode
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StageNode"/> class.
    /// </summary>
    /// <param name="id">The validated node identity.</param>
    /// <param name="stage">The validated stage reference.</param>
    /// <param name="parameterContract">The validated parameter contract reference.</param>
    /// <param name="parameters">The validated parameter payload.</param>
    /// <param name="executionPolicyContract">The validated policy contract, or <see langword="null"/>.</param>
    /// <param name="executionPolicy">The validated policy payload, or <see langword="null"/>.</param>
    /// <remarks>
    /// The constructor is private and every member is get-only, so a node cannot be built or amended
    /// around <see cref="Create(NodeId, StageRef, ContractReference, CanonicalJsonValue)"/>: a
    /// <c>with</c> expression has no member it is allowed to change.
    /// </remarks>
    private StageNode(
        NodeId id,
        StageRef stage,
        ContractReference parameterContract,
        CanonicalJsonValue parameters,
        ContractReference? executionPolicyContract,
        CanonicalJsonValue? executionPolicy)
    {
        Id = id;
        Stage = stage;
        ParameterContract = parameterContract;
        Parameters = parameters;
        ExecutionPolicyContract = executionPolicyContract;
        ExecutionPolicy = executionPolicy;
    }

    /// <summary>
    /// Gets the identity of this node within its graph lineage.
    /// </summary>
    /// <value>A created <see cref="NodeId"/>.</value>
    public NodeId Id { get; }

    /// <summary>
    /// Gets the reference to the registered stage family this node is an occurrence of.
    /// </summary>
    /// <value>A created <see cref="StageRef"/>.</value>
    public StageRef Stage { get; }

    /// <summary>
    /// Gets the contract that <see cref="Parameters"/> claims to satisfy.
    /// </summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    public ContractReference ParameterContract { get; }

    /// <summary>
    /// Gets the stage configuration payload.
    /// </summary>
    /// <value>A created <see cref="CanonicalJsonValue"/>.</value>
    /// <remarks>
    /// The payload is validated against <see cref="ParameterContract"/> by the graph compiler, not here:
    /// contract validation needs the catalog, and the document model deliberately stays catalog-free.
    /// </remarks>
    public CanonicalJsonValue Parameters { get; }

    /// <summary>
    /// Gets the contract that <see cref="ExecutionPolicy"/> claims to satisfy.
    /// </summary>
    /// <value>
    /// A created <see cref="ContractReference"/>, or <see langword="null"/> when this node takes the
    /// provider default policy.
    /// </value>
    public ContractReference? ExecutionPolicyContract { get; }

    /// <summary>
    /// Gets the execution policy payload.
    /// </summary>
    /// <value>
    /// A created <see cref="CanonicalJsonValue"/>, or <see langword="null"/> when this node takes the
    /// provider default policy.
    /// </value>
    /// <remarks>
    /// This member is <see langword="null"/> exactly when <see cref="ExecutionPolicyContract"/> is
    /// <see langword="null"/>.
    /// </remarks>
    public CanonicalJsonValue? ExecutionPolicy { get; }

    /// <summary>
    /// Creates a node that takes the provider default execution policy.
    /// </summary>
    /// <param name="id">The node identity; must not be the default value.</param>
    /// <param name="stage">The stage reference; must not be the default value.</param>
    /// <param name="parameterContract">The parameter contract; must not be the default value.</param>
    /// <param name="parameters">The parameter payload; must not be the default value.</param>
    /// <returns>The validated node.</returns>
    /// <exception cref="ArgumentException">Any argument is the default value.</exception>
    public static StageNode Create(
        NodeId id,
        StageRef stage,
        ContractReference parameterContract,
        CanonicalJsonValue parameters)
    {
        EnsureCore(id, stage, parameterContract, parameters);

        return new StageNode(id, stage, parameterContract, parameters, executionPolicyContract: null, executionPolicy: null);
    }

    /// <summary>
    /// Creates a node that declares an explicit execution policy.
    /// </summary>
    /// <param name="id">The node identity; must not be the default value.</param>
    /// <param name="stage">The stage reference; must not be the default value.</param>
    /// <param name="parameterContract">The parameter contract; must not be the default value.</param>
    /// <param name="parameters">The parameter payload; must not be the default value.</param>
    /// <param name="executionPolicyContract">
    /// The execution policy contract; must not be the default value, because the contract and the payload
    /// are declared together or not at all.
    /// </param>
    /// <param name="executionPolicy">
    /// The execution policy payload; must not be the default value, because the contract and the payload
    /// are declared together or not at all.
    /// </param>
    /// <returns>The validated node.</returns>
    /// <exception cref="ArgumentException">
    /// Any argument is the default value. For the two execution-policy arguments the message names the
    /// pairing rule and points at the overload without them.
    /// </exception>
    public static StageNode Create(
        NodeId id,
        StageRef stage,
        ContractReference parameterContract,
        CanonicalJsonValue parameters,
        ContractReference executionPolicyContract,
        CanonicalJsonValue executionPolicy)
    {
        EnsureCore(id, stage, parameterContract, parameters);

        if (executionPolicyContract.IsDefault)
        {
            throw new ArgumentException(
                DescribeUnpairedExecutionPolicy(nameof(ContractReference), "contract"),
                nameof(executionPolicyContract));
        }

        if (executionPolicy.IsDefault)
        {
            throw new ArgumentException(
                DescribeUnpairedExecutionPolicy(nameof(CanonicalJsonValue), "payload"),
                nameof(executionPolicy));
        }

        if (IsJsonNull(executionPolicy))
        {
            throw new ArgumentException(
                DescribeNullPayload("execution policy payload"),
                nameof(executionPolicy));
        }

        return new StageNode(id, stage, parameterContract, parameters, executionPolicyContract, executionPolicy);
    }

    /// <summary>
    /// Validates the members every node carries, whatever its execution policy.
    /// </summary>
    /// <param name="id">The candidate node identity.</param>
    /// <param name="stage">The candidate stage reference.</param>
    /// <param name="parameterContract">The candidate parameter contract.</param>
    /// <param name="parameters">The candidate parameter payload.</param>
    /// <exception cref="ArgumentException">Any argument is the default value.</exception>
    private static void EnsureCore(
        NodeId id,
        StageRef stage,
        ContractReference parameterContract,
        CanonicalJsonValue parameters)
    {
        if (id.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(NodeId), "node"), nameof(id));
        }

        if (stage.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(StageRef), "stage"), nameof(stage));
        }

        if (parameterContract.IsDefault)
        {
            throw new ArgumentException(
                DescribeDefaultMember(nameof(ContractReference), "parameter contract"),
                nameof(parameterContract));
        }

        if (parameters.IsDefault)
        {
            throw new ArgumentException(
                DescribeDefaultMember(nameof(CanonicalJsonValue), "parameter payload"),
                nameof(parameters));
        }

        if (IsJsonNull(parameters))
        {
            throw new ArgumentException(DescribeNullPayload("parameter payload"), nameof(parameters));
        }
    }

    /// <summary>
    /// Determines whether a payload is the JSON null value.
    /// </summary>
    /// <param name="payload">A created payload.</param>
    /// <returns><see langword="true"/> when the canonical form is the literal <c>null</c>.</returns>
    private static bool IsJsonNull(CanonicalJsonValue payload) =>
        payload.CanonicalUtf8Bytes.Span.SequenceEqual("null"u8);

    /// <summary>Builds the message for a payload that is the JSON null value.</summary>
    /// <param name="role">The payload's role in the node, in prose.</param>
    /// <returns>A message naming the rule and the modeling alternative.</returns>
    /// <remarks>
    /// The rule lives in the model rather than only in the serializer because format version 1 encodes an
    /// absent execution policy as the literal <c>null</c> at a payload position: a node whose payload is
    /// itself the JSON null value would be a document with no byte form of its own, and a document either
    /// has exactly one byte form or it is not a document.
    /// </remarks>
    private static string DescribeNullPayload(string role) =>
        $"A {nameof(StageNode)} {role} must not be the JSON null value: the format has no byte form for it, because the literal null at a payload position encodes an absent execution policy. Model the empty case inside the payload schema, as an empty object or an explicit member.";

    /// <summary>
    /// Returns a one-line diagnostic summary of this node.
    /// </summary>
    /// <returns>Text of the form <c>normalize [orleans-dataflow/map@v1]</c>.</returns>
    /// <remarks>
    /// The record-synthesized <c>ToString</c> would print the entire parameter payload, which can be up to
    /// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/> of JSON; a log line has no use for that. The
    /// summary names the occurrence and the stage it references, and it never throws.
    /// </remarks>
    public override string ToString() => $"{Id} [{Stage}]";

    /// <summary>Builds the message for a member supplied as its default value.</summary>
    /// <param name="typeName">The member's type name.</param>
    /// <param name="role">The member's role in the node, in prose.</param>
    /// <returns>A message naming the type and the role.</returns>
    private static string DescribeDefaultMember(string typeName, string role) =>
        $"A {nameof(StageNode)} requires a created {typeName}; the default {typeName} names no {role}.";

    /// <summary>Builds the message for an execution policy whose two members do not agree.</summary>
    /// <param name="typeName">The missing member's type name.</param>
    /// <param name="role">The missing member's role, either the contract or the payload.</param>
    /// <returns>A message naming the pairing rule and the overload that declares no policy.</returns>
    private static string DescribeUnpairedExecutionPolicy(string typeName, string role) =>
        $"A {nameof(StageNode)} execution policy {role} requires a created {typeName}: the execution policy contract and payload are present together or absent together. Use the {nameof(Create)} overload without execution policy arguments for a node that takes the provider default policy.";
}
