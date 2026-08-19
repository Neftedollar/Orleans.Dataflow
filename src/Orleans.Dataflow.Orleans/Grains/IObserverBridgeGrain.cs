namespace Orleans.Dataflow.Grains;

/// <summary>
/// The addressable end of one run's observer bridge: what grain code anywhere in the cluster pushes at,
/// while that run is listening.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a grain at all.</b> A run's ingress is an object in the memory of the silo executing the run, and
/// nothing outside that process can hold a reference to it. A grain can be addressed by key from anywhere,
/// so the bridge is a grain that holds the run's receiver and forwards to it. The key is composed —
/// <c>{graph}/{run}/{binding}</c> — so that a caller holding the run's ticket can derive it without being
/// told, which is what makes the bridge usable without a directory.
/// </para>
/// <para>
/// <b>Lifetime.</b> One bridge belongs to one run. The run attaches when its source opens and detaches on
/// every terminal path, so a bridge whose run has ended answers <see cref="DataflowPushOutcome.Closed"/>
/// to every push — for as long as the activation lives, and again after it is recycled, because a fresh
/// activation has no receiver either. It never becomes a queue: an unattached bridge stores nothing,
/// remembers nothing, and grows by nothing.
/// </para>
/// <para>
/// <b>Best effort, stated.</b> There is no history, no replay, and no delivery to a run that has not
/// attached yet. A push made a moment before a run opens is refused and lost, exactly as a broadcast to a
/// subscriber that has not subscribed is. What the bridge adds over silence is that every push says what
/// became of it.
/// </para>
/// <para>
/// <b>Ordering and concurrency.</b> The grain is not reentrant, so pushes are serialized in arrival order
/// and one pusher's elements reach the run in the order it sent them. That also means a push waiting for
/// room under the backpressure policy holds the bridge, and every other pusher waits behind it — which is
/// what backpressure is, applied to everyone sharing one bridge, and the reason a bridge whose consumers
/// cannot pay that cost declares a dropping policy instead.
/// </para>
/// </remarks>
public interface IObserverBridgeGrain : IGrainWithStringKey
{
    /// <summary>Attaches one run's receiver to this bridge.</summary>
    /// <param name="receiver">The run's receiver, created by the run itself.</param>
    /// <returns>A task that completes when the bridge is listening.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="receiver"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Refused while another receiver is attached. Two live runs never collide here — their keys differ by
    /// run identity — so the only way to reach the refusal is one document declaring two occurrences of one
    /// binding, which would be two stages competing for one address, and saying so at the start is better
    /// than silently letting one of them win.
    /// </remarks>
    Task AttachAsync(IDataflowPushReceiver receiver);

    /// <summary>Detaches whatever run is attached, so every later push is refused.</summary>
    /// <returns>A task that completes when the bridge has stopped listening.</returns>
    /// <remarks>Idempotent: detaching a bridge nothing is attached to is a no-op.</remarks>
    Task DetachAsync();

    /// <summary>Reports whether a run is listening on this bridge right now.</summary>
    /// <returns><see langword="true"/> when a receiver is attached.</returns>
    /// <remarks>
    /// A reading of a moment and never a reservation. A caller that checks this and then pushes may still
    /// be refused, because the run may have ended in between; the outcome of the push is the answer that
    /// means something.
    /// </remarks>
    Task<bool> IsListeningAsync();

    /// <summary>Pushes one element at the run listening on this bridge.</summary>
    /// <param name="element">The element, which must satisfy Orleans serialization.</param>
    /// <returns>What became of the element.</returns>
    /// <exception cref="ArgumentException">
    /// The element is not of the type the bridge's binding declares in the silo executing the run.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A refusal is an outcome and never an exception: <see cref="DataflowPushOutcome.Closed"/> for a
    /// bridge nothing is listening on, <see cref="DataflowPushOutcome.Dropped"/> for a full ingress under a
    /// dropping policy, and <see cref="DataflowPushOutcome.Failed"/> for a run whose ingress has been
    /// failed. The one exception is a type mismatch, which is a programming error rather than a delivery
    /// outcome.
    /// </para>
    /// <para>
    /// <b>The parameter is <see cref="object"/> and the type check happens after deserialization, so what
    /// bounds the types a caller can put on this wire is Orleans' own allow-list rather than anything this
    /// library declares</b>: Orleans 7 and later deserialize only types it has been told about —
    /// <c>[GenerateSerializer]</c> types and registered serializers — and a deployment that widens that
    /// allow-list widens this member with it.
    /// </para>
    /// </remarks>
    Task<DataflowPushOutcome> PushAsync(object? element);
}

/// <summary>
/// The bridge grain: one activation holding one run's receiver, or nothing.
/// </summary>
/// <remarks>
/// Deliberately stateless in the durable sense. Nothing here is written to storage, because a bridge is
/// only meaningful while a run is listening and a persisted receiver would be an address for a run that no
/// longer exists. Losing the activation therefore loses the attachment, which is the same thing losing the
/// run would do, and a fresh activation refuses every push until a run attaches to it.
/// </remarks>
internal sealed class ObserverBridgeGrain : Grain, IObserverBridgeGrain
{
    private IDataflowPushReceiver? _receiver;

    /// <inheritdoc/>
    public Task AttachAsync(IDataflowPushReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);

        if (_receiver is not null)
        {
            throw new InvalidOperationException(
                $"The observer bridge '{this.GetPrimaryKeyString()}' already has a run listening on it. A bridge address belongs to one occurrence of one run, so a second attachment is two stages competing for one address rather than a second listener.");
        }

        _receiver = receiver;

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DetachAsync()
    {
        _receiver = null;

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<bool> IsListeningAsync() => Task.FromResult(_receiver is not null);

    /// <inheritdoc/>
    public async Task<DataflowPushOutcome> PushAsync(object? element)
    {
        if (_receiver is not { } receiver)
        {
            return DataflowPushOutcome.Closed;
        }

        DataflowPushOutcome outcome;

        try
        {
            outcome = await receiver.PushAsync(element);
        }
        catch (ArgumentException)
        {
            // The one exception that is the caller's business rather than the run's: pushing the wrong type
            // is a programming error and has to reach whoever made it, not be flattened into an outcome.
            throw;
        }
        catch (Exception)
        {
            // Everything else is a reference into another process's memory failing, and every way it can
            // fail — a dead silo, a recycled run, a lost connection — means the same thing to a pusher:
            // nobody is listening any more. Saying that as an outcome is what keeps a best-effort bridge
            // from making its callers write catch blocks for ordinary events.
            outcome = DataflowPushOutcome.Closed;
        }

        // A run that has stopped accepting is forgotten here as well, so the next push is refused without
        // a hop to a receiver whose run is gone. It is an optimisation of the same answer rather than a
        // different one: an ended run refuses every push either way.
        if (outcome is DataflowPushOutcome.Closed or DataflowPushOutcome.Failed)
        {
            _receiver = null;
        }

        return outcome;
    }
}
