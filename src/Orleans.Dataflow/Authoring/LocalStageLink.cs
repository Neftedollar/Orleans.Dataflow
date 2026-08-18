using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One internal connection of a <see cref="LocalGraphShape"/>: which port of which occurrence feeds which
/// port of which other occurrence, by position rather than by identity.
/// </summary>
/// <param name="From">The position of the producing occurrence in authoring order.</param>
/// <param name="FromPort">The output port of that occurrence the elements leave through.</param>
/// <param name="To">The position of the consuming occurrence in authoring order.</param>
/// <param name="ToPort">The input port of that occurrence the elements arrive at.</param>
/// <remarks>
/// <para>
/// A link names occurrences by position because an occurrence has no identity until a graph is closed: a
/// reusable <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/> stands at a different position in every graph it
/// is composed into, and the node identifiers that would name it are allocated once, over the whole shape,
/// by <see cref="LocalGraphBuilder"/>. Positions are what an authoring value can honestly hold.
/// </para>
/// <para>
/// Both ports are named explicitly, because the shapes that need links at all are the ones a single
/// <c>in</c> and <c>out</c> pair cannot describe: a junction's legs are <c>out-0</c> upwards, a fan-in's
/// inputs are <c>in-0</c> upwards, and an unzip's halves are <c>left</c> and <c>right</c>.
/// </para>
/// </remarks>
internal readonly record struct LocalStageLink(int From, PortId FromPort, int To, PortId ToPort);
