using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// One declared input port of a stage specification: a name, the element contract it accepts, and
/// whether an edge has to terminate at it.
/// </summary>
/// <remarks>
/// <para>
/// A port specification is catalog data, not graph data. It says what a stage family declares; whether a
/// particular node in a particular document actually connects the port is a rule of the graph compiler.
/// The two planes stay separate so that a document can be structurally valid without a catalog and
/// semantically checked against one later.
/// </para>
/// <para>
/// The element contract is a <see cref="ContractReference"/> rather than a CLR type, so it is
/// language-neutral and stable across refactoring. Two ports carry compatible elements only when their
/// contract identifiers and major versions are equal; edge type checking is that comparison and nothing
/// more.
/// </para>
/// <para>
/// <see cref="IsOptional"/> is the port's own declaration, made once by whoever registers the stage,
/// rather than a per-node override. A stage that can run without an input says so in its specification,
/// and every occurrence of that stage inherits the answer.
/// </para>
/// <para>
/// The default value declares no port: <see cref="IsDefault"/> reports it, the component properties throw
/// for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct InputPortSpecification
{
    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default InputPortSpecification)";

    private readonly PortId _id;
    private readonly ContractReference _elementContract;
    private readonly bool _isOptional;

    /// <summary>
    /// Initializes a new instance of the <see cref="InputPortSpecification"/> struct.
    /// </summary>
    /// <param name="id">The validated port name.</param>
    /// <param name="elementContract">The validated element contract.</param>
    /// <param name="isOptional">Whether the port may be left unconnected.</param>
    private InputPortSpecification(PortId id, ContractReference elementContract, bool isOptional)
    {
        _id = id;
        _elementContract = elementContract;
        _isOptional = isOptional;
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
    /// Gets the contract of the elements this port accepts.
    /// </summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    public ContractReference ElementContract => IsDefault ? throw DefaultAccess() : _elementContract;

    /// <summary>
    /// Gets a value indicating whether a graph may leave this port unconnected.
    /// </summary>
    /// <value>
    /// <see langword="true"/> when the stage runs without an edge terminating at this port; otherwise
    /// <see langword="false"/>.
    /// </value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    public bool IsOptional => IsDefault ? throw DefaultAccess() : _isOptional;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// <see cref="Create(PortId, ContractReference)"/> rejects a default component and is the only way to
    /// build a specification, so a specification either carries every component or none. Testing the name
    /// alone therefore identifies the default instance exactly.
    /// </remarks>
    public bool IsDefault => _id.IsDefault;

    /// <summary>
    /// Creates a required input port specification.
    /// </summary>
    /// <param name="id">The port name; must not be the default value.</param>
    /// <param name="elementContract">The element contract; must not be the default value.</param>
    /// <returns>The validated specification, with <see cref="IsOptional"/> set to <see langword="false"/>.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> or <paramref name="elementContract"/> is the default value.
    /// </exception>
    /// <remarks>
    /// Required is the default because it is the safer answer: a port nobody remembered to classify is
    /// then reported as unconnected rather than silently accepted as a stage that starves. The flag is an
    /// overload rather than an optional parameter so that every call site reads the same in source and in
    /// a decompiled signature.
    /// </remarks>
    public static InputPortSpecification Create(PortId id, ContractReference elementContract) =>
        Create(id, elementContract, isOptional: false);

    /// <summary>
    /// Creates an input port specification with an explicit optionality.
    /// </summary>
    /// <param name="id">The port name; must not be the default value.</param>
    /// <param name="elementContract">The element contract; must not be the default value.</param>
    /// <param name="isOptional">
    /// <see langword="true"/> when a graph may leave the port unconnected; otherwise
    /// <see langword="false"/>.
    /// </param>
    /// <returns>The validated specification.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> or <paramref name="elementContract"/> is the default value.
    /// </exception>
    public static InputPortSpecification Create(PortId id, ContractReference elementContract, bool isOptional)
    {
        if (id.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(PortId), "port"), nameof(id));
        }

        if (elementContract.IsDefault)
        {
            throw new ArgumentException(
                DescribeDefaultMember(nameof(ContractReference), "element contract"),
                nameof(elementContract));
        }

        return new InputPortSpecification(id, elementContract, isOptional);
    }

    /// <summary>
    /// Returns a diagnostic summary of this specification, or a literal when this instance is the default
    /// value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>in: order@v1</c>, with <c> (optional)</c> appended when the port is optional,
    /// or <c>"(default InputPortSpecification)"</c> when <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The text is for logs and debugger display, not for serialization: the byte form of a port
    /// specification is the catalog envelope, which spells every member out. This method never throws.
    /// </remarks>
    public override string ToString() =>
        IsDefault
            ? DefaultText
            : $"{_id}: {_elementContract}{(_isOptional ? " (optional)" : string.Empty)}";

    /// <summary>Builds the message for a member supplied as its default value.</summary>
    /// <param name="typeName">The member's type name.</param>
    /// <param name="role">The member's role in the specification, in prose.</param>
    /// <returns>A message naming the type and the role.</returns>
    private static string DescribeDefaultMember(string typeName, string role) =>
        $"An {nameof(InputPortSpecification)} requires a created {typeName}; the default {typeName} names no {role}.";

    /// <summary>Builds the error for a component read from the default value.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(InputPortSpecification)));
}
