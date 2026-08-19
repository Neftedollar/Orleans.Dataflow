using System.Globalization;
using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow;

/// <summary>
/// The rate a throttle holds a stream to, the burst it tolerates, and what it does when the stream exceeds
/// them.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag: a rate is not a buffer and not a
/// concurrency bound, and a type that carried all three would let an author set one while meaning another.
/// </para>
/// <para>
/// <see cref="Elements"/> and <see cref="Per"/> are <see langword="required"/> and are one number together:
/// a rate has no meaning without the period it is measured over, and a default period would be a rate the
/// author did not write. <see cref="MaximumBurst"/> defaults to <see cref="Elements"/>, which is the token
/// bucket that starts full and holds one period's worth — the smallest burst that lets a stream arriving
/// exactly at the declared rate pass without being paced at all.
/// </para>
/// <para>
/// The model is a token bucket and is stated rather than implied. The bucket holds <see cref="MaximumBurst"/>
/// cost units, starts full, and refills at <see cref="Elements"/> units per <see cref="Per"/> — continuously
/// rather than in steps, so a throttle of ten per second admits one element every hundred milliseconds
/// instead of ten at the top of each second. An element costs one unit unless the operator was given a cost
/// function, in which case it costs what that function answers.
/// </para>
/// <para>
/// The values are checked where the throttle is placed rather than here, so <c>with</c> expressions and
/// object initializers compose freely and the diagnostic names the operator's own parameter. What is checked
/// is that <see cref="Elements"/> is at least one, that <see cref="Per"/> is a positive finite duration,
/// that <see cref="MaximumBurst"/> — when it is written — is at least one, and that <see cref="Mode"/> is a
/// declared member of its enumeration.
/// </para>
/// </remarks>
public sealed record class ThrottleOptions
{
    /// <summary>Gets the number of cost units the throttle admits per <see cref="Per"/>.</summary>
    /// <value>A positive number; there is no spelling for an unlimited rate.</value>
    /// <remarks>
    /// Counted in cost units rather than in elements, because the two are the same number only for the
    /// overload with no cost function. A throttle of a hundred per second with a cost function that answers
    /// the size of a batch admits a hundred units of batch per second, however many batches that is.
    /// </remarks>
    public required int Elements { get; init; }

    /// <summary>Gets the period <see cref="Elements"/> is measured over.</summary>
    /// <value>A positive, finite duration.</value>
    public required TimeSpan Per { get; init; }

    /// <summary>Gets the greatest budget the throttle ever holds.</summary>
    /// <value>
    /// A positive number, or <see langword="null"/> for the default, which is <see cref="Elements"/>.
    /// </value>
    /// <remarks>
    /// The bucket's size, and therefore the longest quiet period a stream can bank: a throttle that has
    /// been idle admits a burst of this many units at once and is then paced. An element whose cost exceeds
    /// it can never be admitted by waiting, and both modes fail the run for one rather than waiting forever.
    /// </remarks>
    public int? MaximumBurst { get; init; }

    /// <summary>Gets what the throttle does with an element it has no budget for.</summary>
    /// <value>
    /// One of the declared <see cref="ThrottleMode"/> values; the default is
    /// <see cref="ThrottleMode.Shaping"/>, which waits.
    /// </value>
    /// <remarks>
    /// The waiting value is the default deliberately. The other one ends the run, and ending a run is
    /// something an author says out loud.
    /// </remarks>
    public ThrottleMode Mode { get; init; } = ThrottleMode.Shaping;

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>Text of the form <c>throttle (10 per 00:00:01, burst 10, shaping)</c>.</returns>
    /// <remarks>
    /// The record-synthesized text would print the CLR enumeration member name; the mode is rendered in the
    /// same kebab-case spelling the document payload carries, read from the one place that spelling is
    /// defined, so one vocabulary reads across the authoring surface, the document, and a log line. The
    /// numbers are formatted with the invariant culture and the method never throws, including for values
    /// that placing a throttle would reject.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"throttle ({Elements} per {Per}, burst {MaximumBurst?.ToString(CultureInfo.InvariantCulture) ?? "default"}, {LocalThrottleParameters.Spell(Mode) ?? ((int)Mode).ToString(CultureInfo.InvariantCulture)})");
}
