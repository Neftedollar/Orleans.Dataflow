using System.Globalization;
using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow;

/// <summary>
/// How much a buffer holds and what it does when it is full.
/// </summary>
/// <remarks>
/// <para>
/// One record per concern, never one options bag: buffering and parallelism are
/// different decisions with different defaults, and a type that carried both would let an author set one
/// while meaning the other.
/// </para>
/// <para>
/// <see cref="Capacity"/> is <see langword="required"/> and has no unbounded spelling. A buffer is the one
/// place in a local graph where elements accumulate, so its size is the author's decision to make and not
/// the runtime's to guess; an unbounded default would be a memory leak that compiles.
/// </para>
/// <para>
/// The values are checked where the buffer is placed rather than here, so <c>with</c> expressions and
/// object initializers compose freely and the diagnostic names the operator's own parameter. What is
/// checked is that <see cref="Capacity"/> is at least one and that <see cref="OverflowPolicy"/> is a
/// declared member of its enumeration.
/// </para>
/// </remarks>
public sealed record class BufferOptions
{
    /// <summary>Gets the greatest number of elements the buffer holds at one time.</summary>
    /// <value>A positive number; there is no spelling for an unbounded buffer.</value>
    /// <remarks>
    /// A capacity of one is the smallest buffer and is still a real boundary: it decouples the segments on
    /// either side of it into two loops, which a fused chain is not.
    /// </remarks>
    public required int Capacity { get; init; }

    /// <summary>Gets what the buffer does when an element is offered to it and it is full.</summary>
    /// <value>
    /// One of the declared <see cref="Orleans.Dataflow.OverflowPolicy"/> values; the default is
    /// <see cref="OverflowPolicy.Backpressure"/>, which loses nothing.
    /// </value>
    /// <remarks>
    /// The lossless value is the default deliberately. Every other value discards elements, and discarding
    /// elements is something an author says out loud.
    /// </remarks>
    public OverflowPolicy OverflowPolicy { get; init; } = OverflowPolicy.Backpressure;

    /// <summary>Returns a one-line diagnostic summary of these options.</summary>
    /// <returns>Text of the form <c>buffer (capacity 8, drop-oldest)</c>.</returns>
    /// <remarks>
    /// The record-synthesized text would print the CLR enumeration member name; the policy is rendered in
    /// the same kebab-case spelling the document payload carries, read from the one place that spelling
    /// is defined, so one vocabulary reads across the authoring surface, the document, and a log line. The
    /// capacity is formatted with the invariant culture and the method never throws, including for a
    /// capacity or a policy that placing a buffer would reject.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"buffer (capacity {Capacity}, {LocalBufferParameters.Spell(OverflowPolicy) ?? ((int)OverflowPolicy).ToString(CultureInfo.InvariantCulture)})");
}
