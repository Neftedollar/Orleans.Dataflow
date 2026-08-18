namespace Orleans.Dataflow;

/// <summary>
/// What a keyed stage does when an element arrives whose key would be one past its declared bound on
/// active keys.
/// </summary>
/// <remarks>
/// <para>
/// A third overflow enumeration, and the reason it is not <see cref="KeyOverflowPolicy"/> is what the two
/// evictions cost. A deduplicating stage remembers a <em>set</em>, so forgetting a member costs one element
/// emitted twice and age is naturally "when the key was first remembered". A keyed stage holds a running
/// <em>substream</em> per key — a scan's state, a batch's open group — so forgetting one costs the whole of
/// what that substream was holding, which is why the key that goes is the one that has waited longest for an
/// element rather than the one that arrived first, and why it is flushed on the way out rather than dropped.
/// Two different evictions with two different prices are two enumerations.
/// </para>
/// <para>
/// Neither value makes the stage unbounded. The bound is declared either way; what the policy chooses is
/// what the bound costs when it is reached.
/// </para>
/// </remarks>
public enum ActiveKeyOverflowPolicy
{
    /// <summary>
    /// The run fails with a <see cref="TrackedKeyOverflowException"/> naming the bound and the key.
    /// </summary>
    /// <remarks>
    /// The default, and the value that keeps the operator's own promise exactly: every key that arrived got
    /// its own substream from its own first element to the end of the stream. A bound that was sized on an
    /// assumption reports that the assumption was wrong instead of quietly becoming something weaker.
    /// </remarks>
    Fail,

    /// <summary>
    /// The key that has gone longest without an element is flushed and forgotten, and the arriving key takes
    /// its place.
    /// </summary>
    /// <remarks>
    /// The deliberate weakening, and what it costs is worth being exact about: eviction is a
    /// flush-and-forget, so the evicted key's substream ends where it stood — whatever its stages were
    /// holding walks downstream at that moment — and an element of that key arriving later starts a
    /// <em>fresh</em> substream from its own seed. One key can therefore appear more than once downstream,
    /// with a scan restarting from its seed and a batch from an empty group. That is what bounded means
    /// here, and it is the only reading of this policy to rely on.
    /// </remarks>
    EvictIdle,
}
