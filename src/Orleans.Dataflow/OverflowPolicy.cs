namespace Orleans.Dataflow;

/// <summary>
/// What a bounded buffer does when an element is offered to it and it is already full.
/// </summary>
/// <remarks>
/// <para>
/// The policy is applied at one moment and one moment only: when the upstream segment offers an element
/// to a buffer that has no room. It says nothing about what happens while there is room, and it is never
/// consulted a second time for an element that was accepted.
/// </para>
/// <para>
/// Four of the five values lose elements, and losing elements is never silent: every dropped element is
/// counted by the run that dropped it, so a buffer that quietly ate half a stream is a thing a monitor can
/// report rather than a thing an author has to infer. The fifth, <see cref="Fail"/>, converts the same
/// situation into a failure of the run.
/// </para>
/// </remarks>
public enum OverflowPolicy
{
    /// <summary>
    /// The upstream segment waits until the buffer has room, and no element is lost.
    /// </summary>
    /// <remarks>
    /// This is the default and the only lossless value. It is worth being exact about what it buys:
    /// backpressure is prefetch, not loss. A buffer under this policy lets the upstream run ahead of the
    /// downstream by at most the declared capacity and then stops it, so the effect on a slow consumer is
    /// that the producer is slowed to its rate — never that the producer's elements are thrown away.
    /// </remarks>
    Backpressure,

    /// <summary>
    /// The oldest buffered element is dropped to make room, and the arriving element is buffered.
    /// </summary>
    /// <remarks>
    /// The buffer keeps the newest elements: under sustained overflow, what survives is the tail of the
    /// stream. This is the policy for data whose latest value supersedes its earlier ones.
    /// </remarks>
    DropOldest,

    /// <summary>
    /// The arriving element is dropped, and the buffer keeps exactly what it already holds.
    /// </summary>
    /// <remarks>
    /// The newest element is the one arriving, and this policy is the one that drops it. The buffer keeps
    /// the oldest elements: under sustained overflow, what survives is the head of the stream. This is the
    /// policy for data whose first observations matter more than its later ones.
    /// </remarks>
    DropNewest,

    /// <summary>
    /// Every buffered element is dropped, and the arriving element is buffered alone.
    /// </summary>
    /// <remarks>
    /// The whole buffer is discarded rather than one element of it, so the downstream resumes from the
    /// arriving element with no backlog at all. This is the policy for data that is only meaningful as a
    /// fresh batch, where delivering a stale backlog first would be worse than delivering nothing.
    /// </remarks>
    DropBuffer,

    /// <summary>
    /// The run fails with a <see cref="BufferOverflowException"/>.
    /// </summary>
    /// <remarks>
    /// Overflow is treated as a defect of the pipeline rather than a condition to absorb: a buffer that
    /// was sized on an assumption reports that the assumption was wrong instead of hiding it behind lost
    /// elements or a stalled producer.
    /// </remarks>
    Fail,
}
