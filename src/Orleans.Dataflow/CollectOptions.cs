using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// How many elements a collecting sink may hold before the run is a failure.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag (ADR 0004 section 7). Collecting is the one sink whose
/// state grows with the stream, so the bound on that growth is its own decision and its own type.
/// </para>
/// <para>
/// <see cref="MaxElements"/> is <see langword="required"/> and has no unbounded spelling, for the reason
/// <see cref="BufferOptions.Capacity"/> has none: a list that grows with an unbounded stream is a memory
/// leak nobody wrote down. Exceeding the bound faults the run with a
/// <see cref="CollectOverflowException"/> rather than truncating, because a truncated list is a wrong
/// answer that looks like a right one.
/// </para>
/// <para>
/// The value is checked where the sink is created rather than here, so <c>with</c> expressions and object
/// initializers compose freely and the diagnostic names the factory's own parameter.
/// </para>
/// </remarks>
public sealed record class CollectOptions
{
    /// <summary>Gets the greatest number of elements the sink collects.</summary>
    /// <value>A positive number; there is no spelling for an unbounded collection.</value>
    /// <remarks>
    /// A run that delivers exactly this many elements succeeds with all of them; the element after them is
    /// what fails the run, so the bound is a size the result may reach rather than one it may not.
    /// </remarks>
    public required int MaxElements { get; init; }

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>Text of the form <c>collect (at most 1000 elements)</c>.</returns>
    /// <remarks>
    /// The count is formatted with the invariant culture, and the method never throws, including for a
    /// bound that creating the sink would reject.
    /// </remarks>
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"collect (at most {MaxElements} elements)");
}
