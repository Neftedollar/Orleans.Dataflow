namespace Orleans.Dataflow;

/// <summary>
/// What a throttle does with an element the declared rate has no budget for.
/// </summary>
/// <remarks>
/// <para>
/// The two values are two different operators wearing one name, and the capability matrix asks for both:
/// one paces a stream that is going too fast, the other reports that it went too fast. Neither is a
/// default that could be guessed from the numbers, which is why the mode is written down beside them and
/// travels into the document.
/// </para>
/// <para>
/// Nothing here loses an element. A shaping throttle delays it and an enforcing throttle fails the run
/// with it in hand; an author who wants a rate that discards is asking for an overflow policy on a buffer
/// and not for a throttle.
/// </para>
/// </remarks>
public enum ThrottleMode
{
    /// <summary>
    /// The element waits until the declared rate has budget for it, and the stream is paced to that rate.
    /// </summary>
    /// <remarks>
    /// The default, because pacing is what a throttle is usually written for and because it is the value
    /// that loses nothing and reports nothing. The wait happens on the segment's own thread and is one of
    /// this runtime's own waits, so it says so to the pause gate and is released by a stop.
    /// </remarks>
    Shaping,

    /// <summary>
    /// The run fails with a <see cref="RateLimitExceededException"/> as soon as an element exceeds the
    /// declared rate.
    /// </summary>
    /// <remarks>
    /// The rate is treated as a contract the upstream is expected to keep rather than as a speed to be
    /// held to: a stream that breaks it has already violated an assumption, and pacing it would hide the
    /// violation behind latency. Nothing is delayed and nothing is dropped — the first element with no
    /// budget ends the run.
    /// </remarks>
    Enforcing,
}
