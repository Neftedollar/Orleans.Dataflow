namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The fan-in junctions this engine executes, which are the first three rows of ADR 0005's fan-in table.
/// </summary>
/// <remarks>
/// A discriminator rather than three pump implementations, for the reason <see cref="LocalFanOutKind"/> is
/// one: the three differ in two synchronous answers inside one loop — which input to read next, and what
/// the end of an input does to that choice — and in nothing else. See <see cref="LocalFanIn"/> for the
/// answers and <see cref="LocalRun"/> for the loop. The row-building junctions of the table, <c>zip</c> and
/// <c>combine-latest</c>, are a later checkpoint and are deliberately absent rather than declared and
/// unimplemented: they are the two that hold a partial row, and a pump that never holds one cannot pretend
/// to be them.
/// </remarks>
internal enum LocalFanInKind
{
    /// <summary>Emits whichever input has an element, in rotation among the ready ones.</summary>
    Merge,

    /// <summary>Emits one input to its end before reading the next one at all.</summary>
    Concat,

    /// <summary>Emits a declared number of elements from each input in fixed rotation.</summary>
    Interleave,
}
