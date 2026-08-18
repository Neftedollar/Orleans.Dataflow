using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// How many distinct keys a deduplicating stage may remember before the run fails.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag (ADR 0004 section 7). Deduplication is the first operator
/// whose memory grows with the data rather than with the graph, so it gets a bound of its own rather than a
/// share of somebody else's.
/// </para>
/// <para>
/// <see cref="MaxTrackedKeys"/> is <see langword="required"/> and has no unbounded spelling. Remembering
/// every key an unbounded stream ever carried is an unbounded amount of memory, and a default would be a
/// leak nobody wrote down.
/// </para>
/// <para>
/// <see cref="OverflowPolicy"/> is what the bound costs when it is reached, and it defaults to failing
/// because that is the value that keeps the operator's own promise: evicting a key changes what the stage
/// means, since an element whose key was evicted is emitted a second time and the stream is then distinct
/// over a window rather than over its history. <see cref="KeyOverflowPolicy.EvictOldest"/> is that trade
/// spelled out and chosen on purpose. Policies that trade exactness for a smaller footprint in other ways —
/// decay, approximate membership — are their own vocabulary and are not here.
/// </para>
/// <para>
/// The value is checked where the stage is placed rather than here, so <c>with</c> expressions and object
/// initializers compose freely and the diagnostic names the operator's own parameter.
/// </para>
/// </remarks>
public sealed record class DistinctOptions
{
    /// <summary>Gets the greatest number of distinct keys the stage may remember at one time.</summary>
    /// <value>A positive number; there is no spelling for unbounded key tracking.</value>
    /// <remarks>
    /// The bound counts distinct keys and not elements: a repeated element is recognized and dropped
    /// without occupying anything new, so a stream of one key forever runs inside a bound of one.
    /// </remarks>
    public required int MaxTrackedKeys { get; init; }

    /// <summary>Gets what the stage does with the key that would be one past the bound.</summary>
    /// <value>
    /// <see cref="KeyOverflowPolicy.Fail"/> by default, which is exact deduplication over the whole run.
    /// </value>
    /// <remarks>
    /// Defaulted rather than required, unlike the bound beside it, and the two are different questions. How
    /// much a stage may remember has no answer this library could guess; what to do when it has remembered
    /// that much has one honest answer, which is to report that the bound was wrong instead of silently
    /// becoming a weaker operator.
    /// </remarks>
    public KeyOverflowPolicy OverflowPolicy { get; init; }

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>Text of the form <c>distinct (up to 1000 tracked keys, Fail)</c>.</returns>
    /// <remarks>
    /// The count is formatted with the invariant culture and the method never throws, including for a bound
    /// or a policy that placing a stage would reject.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"distinct (up to {MaxTrackedKeys} tracked keys, {OverflowPolicy})");
}
