using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// One declared result port of a stage specification: a name and the contract of the value it yields
/// when a run completes.
/// </summary>
/// <remarks>
/// <para>
/// A result port is where a materialized run hands a value back (ADR 0002). It is not an element stream,
/// which is why it carries no optionality or ignorability flag: a result port needs no edge at all, and a
/// graph reads it by declaring a result slot whose producer is this port. Nothing in the definition plane
/// forces a result to be consumed.
/// </para>
/// <para>
/// The result contract is a <see cref="ContractReference"/> rather than a CLR type, so a run handle can
/// resolve a slot across a process boundary where object identity cannot survive (ADR 0001, ADR 0002).
/// </para>
/// <para>
/// The default value declares no port: <see cref="IsDefault"/> reports it, the component properties throw
/// for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct ResultPortSpecification
{
    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default ResultPortSpecification)";

    private readonly PortId _id;
    private readonly ContractReference _resultContract;

    /// <summary>
    /// Initializes a new instance of the <see cref="ResultPortSpecification"/> struct.
    /// </summary>
    /// <param name="id">The validated port name.</param>
    /// <param name="resultContract">The validated result contract.</param>
    private ResultPortSpecification(PortId id, ContractReference resultContract)
    {
        _id = id;
        _resultContract = resultContract;
    }

    /// <summary>
    /// Gets the name of this port within its stage specification.
    /// </summary>
    /// <value>A created <see cref="PortId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    /// <remarks>
    /// Port names are unique across the whole stage, inputs, outputs, and result ports together, so this
    /// name identifies the port without also naming its direction.
    /// </remarks>
    public PortId Id => IsDefault ? throw DefaultAccess() : _id;

    /// <summary>
    /// Gets the contract of the value this port yields.
    /// </summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    public ContractReference ResultContract => IsDefault ? throw DefaultAccess() : _resultContract;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// <see cref="Create"/> rejects a default component and is the only way to build a specification, so a
    /// specification either carries both components or neither. Testing the name alone therefore
    /// identifies the default instance exactly.
    /// </remarks>
    public bool IsDefault => _id.IsDefault;

    /// <summary>
    /// Creates a result port specification.
    /// </summary>
    /// <param name="id">The port name; must not be the default value.</param>
    /// <param name="resultContract">The result contract; must not be the default value.</param>
    /// <returns>The validated specification.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> or <paramref name="resultContract"/> is the default value.
    /// </exception>
    public static ResultPortSpecification Create(PortId id, ContractReference resultContract)
    {
        if (id.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(PortId), "port"), nameof(id));
        }

        if (resultContract.IsDefault)
        {
            throw new ArgumentException(
                DescribeDefaultMember(nameof(ContractReference), "result contract"),
                nameof(resultContract));
        }

        return new ResultPortSpecification(id, resultContract);
    }

    /// <summary>
    /// Returns a diagnostic summary of this specification, or a literal when this instance is the default
    /// value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>count: counter-result@v1</c>, or <c>"(default ResultPortSpecification)"</c>
    /// when <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The text is for logs and debugger display, not for serialization: the byte form of a port
    /// specification is the catalog envelope, which spells every member out. This method never throws.
    /// </remarks>
    public override string ToString() => IsDefault ? DefaultText : $"{_id}: {_resultContract}";

    /// <summary>Builds the message for a member supplied as its default value.</summary>
    /// <param name="typeName">The member's type name.</param>
    /// <param name="role">The member's role in the specification, in prose.</param>
    /// <returns>A message naming the type and the role.</returns>
    private static string DescribeDefaultMember(string typeName, string role) =>
        $"A {nameof(ResultPortSpecification)} requires a created {typeName}; the default {typeName} names no {role}.";

    /// <summary>Builds the error for a component read from the default value.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(ResultPortSpecification)));
}
