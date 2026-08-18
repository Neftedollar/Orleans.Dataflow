using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// How many keys a keyed stage may hold a substream for at one time, and what the key past that bound
/// costs.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag (ADR 0004 section 7). Grouping by key is the operator whose
/// memory grows with the <em>data</em> in the sharpest way this vocabulary has — one running substream per
/// distinct key rather than one entry per distinct key — so it gets a bound of its own rather than a share
/// of somebody else's.
/// </para>
/// <para>
/// <see cref="MaxActiveKeys"/> is <see langword="required"/> and has no unbounded spelling. A substream per
/// key an unbounded stream ever carried is an unbounded amount of memory, and a default would be a leak
/// nobody wrote down.
/// </para>
/// <para>
/// <see cref="OverflowPolicy"/> is what the bound costs when it is reached, and it defaults to failing
/// because that is the value that keeps the operator's own promise: an evicted key's substream ends where it
/// stood and the same key can start a second one later, so the stream downstream is grouped over a window of
/// activity rather than over the whole run. <see cref="ActiveKeyOverflowPolicy.EvictIdle"/> is that trade
/// spelled out and chosen on purpose.
/// </para>
/// <para>
/// The value is checked where the stage is placed rather than here, so <c>with</c> expressions and object
/// initializers compose freely and the diagnostic names the operator's own parameter.
/// </para>
/// </remarks>
public sealed record class GroupByOptions
{
    /// <summary>Gets the greatest number of keys the stage may hold a substream for at one time.</summary>
    /// <value>A positive number; there is no spelling for an unbounded number of active keys.</value>
    /// <remarks>
    /// The bound counts keys with a substream open and not elements: a key that has already been seen costs
    /// nothing new however many elements it carries, so a stream of one key forever runs inside a bound of
    /// one. A key whose substream ended of its own accord — a <c>Take</c> inside the group flow reaching its
    /// bound — still occupies its place, because remembering that a key has ended is what keeps it ended.
    /// </remarks>
    public required int MaxActiveKeys { get; init; }

    /// <summary>Gets what the stage does with the key that would be one past the bound.</summary>
    /// <value>
    /// <see cref="ActiveKeyOverflowPolicy.Fail"/> by default, which is one substream per key over the whole
    /// run.
    /// </value>
    /// <remarks>
    /// Defaulted rather than required, unlike the bound beside it, and the two are different questions. How
    /// many keys a stage may hold at once has no answer this library could guess; what to do when it is
    /// holding that many has one honest answer, which is to report that the bound was wrong instead of
    /// silently emitting one key's elements as two substreams.
    /// </remarks>
    public ActiveKeyOverflowPolicy OverflowPolicy { get; init; }

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>Text of the form <c>group-by (up to 1000 active keys, Fail)</c>.</returns>
    /// <remarks>
    /// The count is formatted with the invariant culture and the method never throws, including for a bound
    /// or a policy that placing a stage would reject.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"group-by (up to {MaxActiveKeys} active keys, {OverflowPolicy})");
}
