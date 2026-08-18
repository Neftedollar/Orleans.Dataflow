namespace Orleans.Dataflow;

/// <summary>
/// What a deduplicating stage does when an element arrives whose key would be one past its declared bound.
/// </summary>
/// <remarks>
/// <para>
/// A separate enumeration from <see cref="OverflowPolicy"/> and deliberately so. A buffer's five policies
/// are about which <em>elements</em> survive a queue that has no room; these two are about which
/// <em>keys</em> a stage still remembers, and the choice changes what the operator means rather than which
/// elements it delivers. Three of the buffer's five have no reading at all here — there is no such thing as
/// dropping the arriving key, or discarding the whole set to make room for it, without saying what the
/// stream then is — so offering them would be offering a policy with no contract.
/// </para>
/// <para>
/// Neither value makes deduplication unbounded. The bound is declared either way; what the policy chooses is
/// what the bound costs when it is reached.
/// </para>
/// </remarks>
public enum KeyOverflowPolicy
{
    /// <summary>
    /// The run fails with a <see cref="TrackedKeyOverflowException"/>.
    /// </summary>
    /// <remarks>
    /// The default, and the value that keeps the operator's own promise exactly: everything this stage
    /// emitted was the first of its key, and it says so by refusing to go on rather than by quietly becoming
    /// something weaker. A bound that was sized on an assumption reports that the assumption was wrong.
    /// </remarks>
    Fail,

    /// <summary>
    /// The key that has been remembered longest is forgotten, and the arriving key takes its place.
    /// </summary>
    /// <remarks>
    /// The deliberate weakening, and what it costs is worth being exact about: an element whose key was
    /// evicted is emitted a second time if it ever arrives again, so the stream is no longer distinct over
    /// its whole history — it is distinct over a window of the last <c>MaxTrackedKeys</c> keys to arrive.
    /// That is the honest reading of this policy and the only one to rely on. Age is measured by when a key
    /// was first remembered and not by when it was last seen, so a repeat does not refresh a key.
    /// </remarks>
    EvictOldest,
}
