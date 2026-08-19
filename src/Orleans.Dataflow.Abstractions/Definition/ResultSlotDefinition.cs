using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// A named, versioned result or runtime control a graph exposes to whoever runs it.
/// </summary>
/// <remarks>
/// <para>
/// Result slots are how a materialized run hands values back. The definition plane declares
/// the slot: its name, the contract of the value it yields, and the result port that produces it. A run
/// handle resolves the declared slot to an actual value, and only for the graph identity, revision, and
/// import scope the run was materialized from.
/// </para>
/// <para>
/// The slot name is graph-level and stable, which is what lets a durable or cross-process run address a
/// result at all: object identity of a materialized value cannot survive a process boundary, and a name
/// can.
/// </para>
/// <para>
/// Two slots may share one <see cref="Producer"/>. That is two names for one produced value, which is
/// structurally sound and sometimes exactly what an author wants; only slot names have to be unique.
/// </para>
/// </remarks>
public sealed record class ResultSlotDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ResultSlotDefinition"/> class.
    /// </summary>
    /// <param name="id">The validated slot identity.</param>
    /// <param name="resultContract">The validated result contract reference.</param>
    /// <param name="producer">The validated producing port address.</param>
    /// <remarks>
    /// The constructor is private and every member is get-only, so a definition cannot be built or amended
    /// around <see cref="Create"/>: a <c>with</c> expression has no member it is allowed to change.
    /// </remarks>
    private ResultSlotDefinition(ResultSlotId id, ContractReference resultContract, PortAddress producer)
    {
        Id = id;
        ResultContract = resultContract;
        Producer = producer;
    }

    /// <summary>
    /// Gets the name under which a run handle resolves this slot.
    /// </summary>
    /// <value>A created <see cref="ResultSlotId"/>.</value>
    public ResultSlotId Id { get; }

    /// <summary>
    /// Gets the contract of the value this slot yields.
    /// </summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    public ContractReference ResultContract { get; }

    /// <summary>
    /// Gets the result port that produces this slot's value.
    /// </summary>
    /// <value>A created <see cref="PortAddress"/>.</value>
    /// <remarks>
    /// That the address names a result port which actually exists on the resolved stage specification, and
    /// whose result contract matches <see cref="ResultContract"/>, is a catalog rule checked by the graph
    /// compiler. The document model only requires the address to point at a declared node.
    /// </remarks>
    public PortAddress Producer { get; }

    /// <summary>
    /// Creates a <see cref="ResultSlotDefinition"/> from its components.
    /// </summary>
    /// <param name="id">The slot identity; must not be the default value.</param>
    /// <param name="resultContract">The result contract; must not be the default value.</param>
    /// <param name="producer">The producing port address; must not be the default value.</param>
    /// <returns>The validated slot definition.</returns>
    /// <exception cref="ArgumentException">Any argument is the default value.</exception>
    public static ResultSlotDefinition Create(
        ResultSlotId id,
        ContractReference resultContract,
        PortAddress producer)
    {
        if (id.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(ResultSlotId), "slot"), nameof(id));
        }

        if (resultContract.IsDefault)
        {
            throw new ArgumentException(
                DescribeDefaultMember(nameof(ContractReference), "result contract"),
                nameof(resultContract));
        }

        if (producer.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(PortAddress), "producer"), nameof(producer));
        }

        return new ResultSlotDefinition(id, resultContract, producer);
    }

    /// <summary>Builds the message for a member supplied as its default value.</summary>
    /// <param name="typeName">The member's type name.</param>
    /// <param name="role">The member's role in the definition, in prose.</param>
    /// <returns>A message naming the type and the role.</returns>
    private static string DescribeDefaultMember(string typeName, string role) =>
        $"A {nameof(ResultSlotDefinition)} requires a created {typeName}; the default {typeName} names no {role}.";
}
