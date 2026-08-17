namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The fan-out junctions this engine executes, which are the rows of ADR 0005's fan-out table.
/// </summary>
/// <remarks>
/// A discriminator rather than three pump implementations, because the three differ in two synchronous
/// answers inside one loop; see <see cref="LocalFanOut"/> for the answers and <see cref="LocalRun"/> for
/// the loop. The routed junction of the table, <c>partition</c>, is a later checkpoint and is deliberately
/// absent rather than declared and unimplemented.
/// </remarks>
internal enum LocalFanOutKind
{
    /// <summary>Delivers every element to every live output, pulling when all of them have room.</summary>
    Broadcast,

    /// <summary>Delivers each element to one output with room, in rotation among the willing.</summary>
    Balance,

    /// <summary>Delivers each half of a row to its own output, pulling when both of them have room.</summary>
    Unzip,
}
