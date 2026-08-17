namespace Orleans.Dataflow.Adapters;

/// <summary>
/// The credit a keyed grain-call stage keeps for one run: one call in flight per key, and the run's declared
/// bound across keys.
/// </summary>
/// <remarks>
/// <para>
/// <b>The whole credit protocol lives here, and there is nothing of it on the wire.</b> Two bounds hold at
/// once. The bound across keys is the engine's own — an ordered asynchronous stage admits at most its
/// declared number of callbacks and frees a slot on emission — so a call in flight is credit spent and an
/// element cannot enter the stage until a slot is free. The bound within a key is this type: a key's next
/// call is chained behind its previous one, so the reply to element N <em>is</em> the grant that lets N+1 be
/// sent. Grants ride on replies in the strongest form available, because a reply is the only message there
/// is.
/// </para>
/// <para>
/// <b>Why one per key rather than a window per key.</b> Because the per-key ordering this stage promises has
/// to come from somewhere, and it cannot come from Orleans: no pairwise message ordering between activations
/// is documented, and the probe in this repository's suite watched a caller's pipelined calls arrive at a
/// non-reentrant callee badly out of order inside a single silo — the first arrival of two hundred was the
/// fourteenth sent. A window of two per key would therefore mean two messages whose relative order nothing
/// undertakes to keep. With one in flight there is never a second message to reorder, so the ordering is a
/// property of this accounting rather than of the transport, and it holds on one silo and on fifty alike.
/// </para>
/// <para>
/// <b>What the table costs.</b> One entry per key with work in flight and nothing per key the run has ever
/// seen: an entry is made when a key's first call is chained and removed when its last one settles. The
/// number of keys with work in flight is bounded by the stage's declared bound, so the table is bounded by
/// a number the document names — not by the cardinality of the key space, which is what would make a keyed
/// stage leak on a long run over many keys.
/// </para>
/// <para>
/// <b>What a predecessor's failure does.</b> Nothing, here. A chained call does not inherit the outcome of
/// the call before it: the run's engine observes every callback and faults the run on the first failure, and
/// this reporting it a second time would attribute one grain's exception to a later element that never
/// reached it. What the following calls actually meet is the run's cancelled token, which is the correct
/// answer — the run is ending, and the elements behind the failure are abandoned rather than blamed.
/// </para>
/// </remarks>
internal sealed class KeyedCallWindow
{
    private readonly Dictionary<string, Task> _tails = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();

    /// <summary>Submits one element behind whatever its key already has in flight.</summary>
    /// <param name="key">The key the element belongs to.</param>
    /// <param name="call">The call to make once the key is free.</param>
    /// <returns>The reply.</returns>
    /// <remarks>
    /// Returns at once and waits nowhere. The engine invokes an asynchronous stage's callback on the
    /// segment's own loop thread, so a submission that blocked until the key was free would stall every
    /// other key behind this one — the exact opposite of what a keyed stage is for. What happens under the
    /// lock is a dictionary lookup and an assignment; the waiting, and the call itself, are inside the
    /// returned task and never under the lock.
    /// </remarks>
    internal Task<object?> SubmitAsync(string key, Func<Task<object?>> call)
    {
        lock (_gate)
        {
            Task previous = _tails.TryGetValue(key, out Task? tail) ? tail : Task.CompletedTask;
            Task<object?> next = ChainAsync(previous, call);

            _tails[key] = next;

            // Scheduled rather than run inline, so that releasing a key never happens on the thread that
            // completed the author's call and never inside the lock this method holds.
            _ = next.ContinueWith(
                (settled, held) => Release((string)held!, settled),
                key,
                CancellationToken.None,
                TaskContinuationOptions.DenyChildAttach,
                TaskScheduler.Default);

            return next;
        }
    }

    /// <summary>Reports how many keys have work in flight right now.</summary>
    /// <returns>The number of entries in the table.</returns>
    /// <remarks>
    /// A reading of a moment, kept for the tests that assert the table is bounded by the declared bound
    /// rather than by the number of keys a run has seen. Nothing in the adapter reads it.
    /// </remarks>
    internal int Tracked
    {
        get
        {
            lock (_gate)
            {
                return _tails.Count;
            }
        }
    }

    /// <summary>Waits for a key's previous call and then makes this one.</summary>
    /// <param name="previous">The key's previous call, or a completed task for its first.</param>
    /// <param name="call">The call to make.</param>
    /// <returns>The reply.</returns>
    /// <remarks>
    /// <para>
    /// The yield is not a formality. This method is started while <see cref="SubmitAsync"/> holds the
    /// window's lock, and a key's first call has nothing to wait for — so without it the author's call would
    /// begin, synchronously, underneath that lock. An author who wrote a call that blocks before returning
    /// its task would then be holding the accounting for every other key as well, which is precisely the
    /// failure a per-key window exists to prevent. Yielding first costs one thread-pool dispatch per element
    /// on a stage that makes a grain call for every element, and buys the guarantee that no author code and
    /// no grain call ever runs under this lock.
    /// </para>
    /// <para>
    /// The predecessor's own outcome is swallowed rather than inherited: the run's engine observes every
    /// callback and faults the run on the first failure, and reporting it again here would attribute one
    /// grain's exception to a later element that never reached it.
    /// </para>
    /// </remarks>
    private static async Task<object?> ChainAsync(Task previous, Func<Task<object?>> call)
    {
        await Task.Yield();

        try
        {
            await previous.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The predecessor's outcome is the run's business and not this element's; see the remarks.
        }

        return await call().ConfigureAwait(false);
    }

    /// <summary>Forgets a key whose last call has settled.</summary>
    /// <param name="key">The key.</param>
    /// <param name="settled">The call that settled.</param>
    /// <remarks>
    /// The identity check is what keeps the table correct under a key that is still busy: a call chained
    /// behind this one has already replaced the tail, so removing by key alone would forget a key with work
    /// in flight and let the next element of it overtake the one already running.
    /// </remarks>
    private void Release(string key, Task settled)
    {
        lock (_gate)
        {
            if (_tails.TryGetValue(key, out Task? tail) && ReferenceEquals(tail, settled))
            {
                _ = _tails.Remove(key);
            }
        }
    }
}
