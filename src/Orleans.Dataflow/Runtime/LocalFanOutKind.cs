namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The fan-out junctions this engine executes, which are the rows of ADR 0005's fan-out table.
/// </summary>
/// <remarks>
/// A discriminator rather than four pump implementations, because three of the four differ only in two
/// synchronous answers inside one loop; see <see cref="LocalFanOut"/> for the answers and
/// <see cref="LocalRun"/> for the loops. <see cref="Partition"/> is the one that needs a loop of its own,
/// because its target is a function of the element and therefore cannot be known before the read: it reads
/// first and waits second, which is the one place in this engine where that order is right.
/// </remarks>
internal enum LocalFanOutKind
{
    /// <summary>Delivers every element to every live output, pulling when all of them have room.</summary>
    Broadcast,

    /// <summary>Delivers each element to one output with room, in rotation among the willing.</summary>
    Balance,

    /// <summary>Delivers each element to the output its routing function names, waiting for that one.</summary>
    Partition,

    /// <summary>Delivers each half of a row to its own output, pulling when both of them have room.</summary>
    Unzip,
}
