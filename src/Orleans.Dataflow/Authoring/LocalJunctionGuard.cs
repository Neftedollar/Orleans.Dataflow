using System.Globalization;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// What a junction call checks about its branches before anything is composed, and the port names it wires
/// them to.
/// </summary>
/// <remarks>
/// <para>
/// A junction's arity is a fact about a document's edges rather than about a payload, so nothing downstream
/// of here restates it: the ports past the required two are ignorable or optional, and the graph compiler
/// reads the edges to learn how many legs a given occurrence actually has. What that leaves is a bound, and
/// this is where a call that exceeds it is told so — with the number it asked for — instead of building a
/// document that names a port no specification declares.
/// </para>
/// <para>
/// The port lists are built once per arity and shared. They are the same names for every junction of one
/// direction, which is what makes a fan-out's leg order and a fan-in's input order one statement rather than
/// one per stage.
/// </para>
/// </remarks>
internal static class LocalJunctionGuard
{
    /// <summary>The leg ports of a fan-out, by arity.</summary>
    private static readonly PortId[][] FanOutPortsByArity = Ports(LocalVocabulary.FanOutPort);

    /// <summary>The input ports of a fan-in, by arity.</summary>
    private static readonly PortId[][] FanInPortsByArity = Ports(LocalVocabulary.FanInPort);

    /// <summary>Returns the leg ports a fan-out of one arity wires, in leg order.</summary>
    /// <param name="arity">The number of legs, already checked.</param>
    /// <returns>The ports <c>out-0</c> upwards.</returns>
    internal static IReadOnlyList<PortId> FanOutPorts(int arity) => FanOutPortsByArity[arity];

    /// <summary>Returns the input ports a fan-in of one arity wires, in input order.</summary>
    /// <param name="arity">The number of inputs, already checked.</param>
    /// <returns>The ports <c>in-0</c> upwards.</returns>
    internal static IReadOnlyList<PortId> FanInPorts(int arity) => FanInPortsByArity[arity];

    /// <summary>Checks the branches of a fan-out call.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="branches">The branches the author supplied.</param>
    /// <param name="parameterName">The name of the call's parameter they arrived in.</param>
    /// <returns>The same branches.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="branches"/>, or one of its elements, is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// There are fewer than <see cref="LocalVocabulary.MinFanOut"/> branches or more than
    /// <see cref="LocalVocabulary.MaxFanOut"/>.
    /// </exception>
    /// <remarks>
    /// One branch is a chain written the long way and none is a discarding sink, so both are rejected here
    /// rather than composed into a junction that is not one. The upper bound is the declared port list's,
    /// and a call that needs a ninth leg is told the number it asked for instead of losing one silently.
    /// </remarks>
    internal static Branch<TIn>[] Branches<TIn>(Branch<TIn>[] branches, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(branches, parameterName);

        if (branches.Length < LocalVocabulary.MinFanOut || branches.Length > LocalVocabulary.MaxFanOut)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A fan-out junction has between {LocalVocabulary.MinFanOut} and {LocalVocabulary.MaxFanOut} branches, and this call has {branches.Length}. One branch is a chain written the long way, none is a discarding sink, and more than {LocalVocabulary.MaxFanOut} is past the legs a local junction declares."),
                parameterName);
        }

        for (int index = 0; index < branches.Length; index++)
        {
            ArgumentNullException.ThrowIfNull(branches[index], parameterName);
        }

        return branches;
    }

    /// <summary>Collects the result requests of the branches one fan-out call consumed.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="junction">The position of the junction occurrence in the shape.</param>
    /// <param name="branches">The branches, in argument order.</param>
    /// <returns>One request per result-bearing branch, in argument order.</returns>
    /// <remarks>
    /// The producing occurrence of a branch's result is the branch's last occurrence, and the branches were
    /// appended after the junction in argument order, so the positions follow from the lengths. This is the
    /// one place that arithmetic is done, and it is done here rather than in every junction call.
    /// </remarks>
    internal static IReadOnlyList<LocalSlotRequest> Slots<TIn>(int junction, IReadOnlyList<Branch<TIn>> branches)
    {
        List<LocalSlotRequest> slots = [];
        int position = junction;

        for (int index = 0; index < branches.Count; index++)
        {
            Branch<TIn> branch = branches[index];

            position += branch.Stages.Count;

            if (branch.SlotName is { } name)
            {
                slots.Add(new LocalSlotRequest(name, position, branch.Binding));
            }
        }

        return Array.AsReadOnly<LocalSlotRequest>([.. slots]);
    }

    /// <summary>Reads the occurrences of every branch, in argument order.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="branches">The branches.</param>
    /// <returns>One occurrence list per branch.</returns>
    internal static IReadOnlyList<IReadOnlyList<StageOccurrence>> Chains<TIn>(IReadOnlyList<Branch<TIn>> branches)
    {
        IReadOnlyList<StageOccurrence>[] chains = new IReadOnlyList<StageOccurrence>[branches.Count];

        for (int index = 0; index < branches.Count; index++)
        {
            chains[index] = branches[index].Stages;
        }

        return chains;
    }

    /// <summary>Builds the port list of every arity a junction can have.</summary>
    /// <param name="port">The naming rule of one numbered port.</param>
    /// <returns>An array indexed by arity, each holding that many ports in order.</returns>
    /// <remarks>
    /// The arities below the minimum are built too and are simply never asked for: indexing by arity is
    /// what makes the lookup a read rather than a search, and a table with holes in it would be a second
    /// statement of the bound the guards already make.
    /// </remarks>
    private static PortId[][] Ports(Func<int, PortId> port)
    {
        int widest = Math.Max(LocalVocabulary.MaxFanOut, LocalVocabulary.MaxFanIn);
        PortId[][] ports = new PortId[widest + 1][];

        for (int arity = 0; arity <= widest; arity++)
        {
            ports[arity] = new PortId[arity];

            for (int index = 0; index < arity; index++)
            {
                ports[arity][index] = port(index);
            }
        }

        return ports;
    }
}
