using System.Globalization;
using Orleans.Dataflow.Definition;
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

    /// <summary>Checks the branches of a registered fan-out call against the junction's own arity.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="branches">The branches the author supplied.</param>
    /// <param name="legs">The number of output ports the registered junction declares.</param>
    /// <param name="stage">The junction's stage reference, for the diagnostic.</param>
    /// <param name="parameterName">The name of the call's parameter they arrived in.</param>
    /// <returns>The same branches.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="branches"/>, or one of its elements, is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// There is not exactly one branch per declared leg.
    /// </exception>
    /// <remarks>
    /// The bound a local junction is checked against is a range, because the local specifications declare
    /// eight ports of which the first two are required and the rest ignorable. A registered junction's arity
    /// is not a range at all: the stage declares exactly the ports it has, every one of them is wired, and a
    /// call with the wrong number of branches would build a document naming a port no stage declares or
    /// leaving one unconnected. So it is an equality, and the diagnostic names the stage that fixed it.
    /// </remarks>
    internal static Branch<TIn>[] Legs<TIn>(
        Branch<TIn>[] branches,
        int legs,
        StageRef stage,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(branches, parameterName);

        if (branches.Length != legs)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The registered fan-out '{stage}' declares {legs} output ports, and this call has {branches.Length} branches. A junction's legs are the ports its stage declares, so a branch is written for each one; the order is the specification's own port order."),
                parameterName);
        }

        for (int index = 0; index < branches.Length; index++)
        {
            ArgumentNullException.ThrowIfNull(branches[index], parameterName);
        }

        return branches;
    }

    /// <summary>Checks the sources of a registered fan-in call against the junction's own arity.</summary>
    /// <typeparam name="T">The element type every source produces.</typeparam>
    /// <param name="others">The sources the author supplied beside the receiver of the call.</param>
    /// <param name="inputs">The number of input ports the registered junction declares.</param>
    /// <param name="stage">The junction's stage reference, for the diagnostic.</param>
    /// <param name="parameterName">The name of the call's parameter they arrived in.</param>
    /// <returns>The same sources.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="others"/>, or one of its elements, is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The receiver plus the arguments are not exactly the declared inputs.
    /// </exception>
    /// <remarks>
    /// The receiver counts, which is why the arithmetic is here rather than at the call site: <c>a.FanIn(j,
    /// …, b)</c> joins two streams and a junction declaring two inputs is the one it fits.
    /// </remarks>
    internal static Source<T>[] Joined<T>(
        Source<T>[] others,
        int inputs,
        StageRef stage,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(others, parameterName);

        if (others.Length + 1 != inputs)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The registered fan-in '{stage}' declares {inputs} input ports, and this call joins {others.Length + 1} streams counting the source it was written on. A junction's inputs are the ports its stage declares, so a source is written for each one; the order is the specification's own port order, with the receiver first."),
                parameterName);
        }

        for (int index = 0; index < others.Length; index++)
        {
            ArgumentNullException.ThrowIfNull(others[index], parameterName);
        }

        return others;
    }

    /// <summary>Reads the port names of a registered junction's ports, in the specification's own order.</summary>
    /// <param name="ports">The specification's input ports.</param>
    /// <returns>The identifiers, in canonical order.</returns>
    internal static IReadOnlyList<PortId> PortsOf(IReadOnlyList<InputPortSpecification> ports)
    {
        PortId[] ids = new PortId[ports.Count];

        for (int index = 0; index < ports.Count; index++)
        {
            ids[index] = ports[index].Id;
        }

        return ids;
    }

    /// <summary>Reads the port names of a registered junction's legs, in the specification's own order.</summary>
    /// <param name="ports">The specification's output ports.</param>
    /// <returns>The identifiers, in canonical order.</returns>
    internal static IReadOnlyList<PortId> PortsOf(IReadOnlyList<OutputPortSpecification> ports)
    {
        PortId[] ids = new PortId[ports.Count];

        for (int index = 0; index < ports.Count; index++)
        {
            ids[index] = ports[index].Id;
        }

        return ids;
    }

    /// <summary>Reads the branches of a fan-out call as legs, in argument order.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="branches">The branches.</param>
    /// <returns>One leg per branch.</returns>
    /// <remarks>
    /// The bridge from the typed branch values to the untyped legs the composition works in. It exists
    /// because an unzip-shaped junction's legs carry unlike element types, so a list of them cannot be
    /// typed by one of them; everything past this point is about occurrences and slot names, neither of
    /// which is typed at all.
    /// </remarks>
    internal static IReadOnlyList<BranchLeg> Legs<TIn>(IReadOnlyList<Branch<TIn>> branches)
    {
        BranchLeg[] legs = new BranchLeg[branches.Count];

        for (int index = 0; index < branches.Count; index++)
        {
            legs[index] = Leg(branches[index]);
        }

        return legs;
    }

    /// <summary>Reads one branch as a leg.</summary>
    /// <typeparam name="TIn">The element type the branch consumes.</typeparam>
    /// <param name="branch">The branch.</param>
    /// <returns>The leg.</returns>
    internal static BranchLeg Leg<TIn>(Branch<TIn> branch) =>
        new(branch.Stages, branch.SlotName, branch.Binding);

    /// <summary>Collects the result requests of the branches one fan-out call consumed.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="junction">The position of the junction occurrence in the shape.</param>
    /// <param name="branches">The branches, in argument order.</param>
    /// <returns>One request per result-bearing branch, in argument order.</returns>
    internal static IReadOnlyList<LocalSlotRequest> Slots<TIn>(int junction, IReadOnlyList<Branch<TIn>> branches) =>
        Slots(junction, Legs(branches));

    /// <summary>Collects the result requests of the legs one fan-out call consumed.</summary>
    /// <param name="junction">The position of the junction occurrence in the shape.</param>
    /// <param name="legs">The legs, in argument order.</param>
    /// <returns>One request per result-bearing leg, in argument order.</returns>
    /// <remarks>
    /// The producing occurrence of a leg's result is the leg's last occurrence, and the legs were appended
    /// after the junction in argument order, so the positions follow from the lengths. This is the one
    /// place that arithmetic is done, and it is done here rather than in every junction call.
    /// </remarks>
    internal static IReadOnlyList<LocalSlotRequest> Slots(int junction, IReadOnlyList<BranchLeg> legs)
    {
        List<LocalSlotRequest> slots = [];
        int position = junction;

        for (int index = 0; index < legs.Count; index++)
        {
            BranchLeg leg = legs[index];

            position += leg.Stages.Count;

            if (leg.SlotName is { } name)
            {
                slots.Add(new LocalSlotRequest(name, position, leg.Binding));
            }
        }

        return Array.AsReadOnly<LocalSlotRequest>([.. slots]);
    }

    /// <summary>Reads the occurrences of every branch, in argument order.</summary>
    /// <typeparam name="TIn">The element type every branch consumes.</typeparam>
    /// <param name="branches">The branches.</param>
    /// <returns>One occurrence list per branch.</returns>
    internal static IReadOnlyList<IReadOnlyList<StageOccurrence>> Chains<TIn>(IReadOnlyList<Branch<TIn>> branches) =>
        Chains(Legs(branches));

    /// <summary>Reads the occurrences of every leg, in argument order.</summary>
    /// <param name="legs">The legs.</param>
    /// <returns>One occurrence list per leg.</returns>
    internal static IReadOnlyList<IReadOnlyList<StageOccurrence>> Chains(IReadOnlyList<BranchLeg> legs)
    {
        IReadOnlyList<StageOccurrence>[] chains = new IReadOnlyList<StageOccurrence>[legs.Count];

        for (int index = 0; index < legs.Count; index++)
        {
            chains[index] = legs[index].Stages;
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

/// <summary>One leg of a fan-out call, with its element type forgotten.</summary>
/// <param name="Stages">The occurrences the leg contributes, in authoring order.</param>
/// <param name="SlotName">The name of the result its terminal declares, when it declares one.</param>
/// <param name="Binding">The binding of that result's slot, waiting for the graph that closes it.</param>
/// <remarks>
/// A <see cref="Branch{TIn}"/> is typed because an author writes one against a stream of a known type. A
/// junction's legs, taken together, are not: an unzip splits a row into unlike halves, so the list a
/// composition works over cannot be typed by any one of them. This is that list's element, and it carries
/// exactly the three things a composition needs.
/// </remarks>
internal readonly record struct BranchLeg(
    IReadOnlyList<StageOccurrence> Stages,
    ResultSlotId? SlotName,
    BranchSlotBinding? Binding);
