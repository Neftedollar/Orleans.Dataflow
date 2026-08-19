using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// How many callbacks of an asynchronous stage may be in flight at one time.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag. Parallelism and buffering are
/// separate decisions: an asynchronous stage bounds how much work is outstanding, and a
/// <see cref="BufferOptions"/> in front of it bounds how much work is waiting to start.
/// </para>
/// <para>
/// <see cref="MaxConcurrency"/> is <see langword="required"/> and has no unbounded spelling, for the same
/// reason a buffer has no unbounded capacity: unbounded concurrency is unbounded memory and unbounded load
/// on whatever the callback talks to, and neither is a thing to arrive at by leaving a value unwritten.
/// </para>
/// <para>
/// The value is checked where the stage is placed rather than here, so <c>with</c> expressions and object
/// initializers compose freely and the diagnostic names the operator's own parameter.
/// </para>
/// </remarks>
public sealed record class ParallelismOptions
{
    /// <summary>Gets the greatest number of callbacks that may be in flight at one time.</summary>
    /// <value>A positive number; there is no spelling for unbounded concurrency.</value>
    /// <remarks>
    /// A maximum of one is the sequential asynchronous map: one callback runs, its result is emitted, and
    /// the next element starts. It is a real setting rather than a degenerate one, and it is what an
    /// author writes for a callback that talks to something which tolerates no concurrency at all.
    /// </remarks>
    public required int MaxConcurrency { get; init; }

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>Text of the form <c>parallelism (max concurrency 4)</c>.</returns>
    /// <remarks>
    /// The count is formatted with the invariant culture and the method never throws, including for a
    /// concurrency that placing a stage would reject.
    /// </remarks>
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"parallelism (max concurrency {MaxConcurrency})");
}
