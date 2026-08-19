using Orleans.BroadcastChannel;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The channel namespace this package owns, and the reason it owns one.
/// </summary>
/// <remarks>
/// <para>
/// A Broadcast Channel subscription is <b>implicit only</b>: a grain <em>type</em> carries
/// <see cref="ImplicitChannelSubscriptionAttribute"/> naming a namespace, and the runtime activates one
/// grain of that type per channel key when something publishes. There is no explicit subscription and no API
/// by which a running thing subscribes to a channel it chooses at run time. That is a property of the
/// platform and not a choice made here, and its consequence is exact: a dataflow run can only ever receive
/// from a namespace whose subscriber grain type was compiled into this package. So the package owns one
/// namespace, a document names a channel <em>key</em> within it, and consuming an arbitrary namespace of a
/// deployment's choosing is not something any design could offer.
/// </para>
/// <para>
/// The sink is unaffected and stays namespace-free: publishing needs no subscription, so a broadcast sink
/// addresses any namespace a deployment likes. The asymmetry is the implicit-subscription rule showing
/// through, and it is stated rather than hidden.
/// </para>
/// </remarks>
internal static class OrleansBroadcastChannels
{
    /// <summary>The one namespace whose channels a dataflow run can consume.</summary>
    /// <remarks>
    /// A compile-time constant because <see cref="ImplicitChannelSubscriptionAttribute"/> takes one, which
    /// is the same fact seen from the language: the namespace a subscriber receives is fixed when the
    /// package is built.
    /// </remarks>
    internal const string SourceNamespace = "orleans-dataflow-broadcast";
}

/// <summary>
/// The delivery registry of one broadcast channel: the implicit subscriber the runtime activates, holding
/// the receivers of every run that is listening to that channel right now.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a relay at all.</b> The observer bridge's problem was addressing — a run's ingress lives in one
/// silo's memory and needs a name a caller can hold. This one's problem is the opposite: the subscriber's
/// address is the <em>runtime's</em> to choose, because implicit subscription activates a grain of a
/// compiled type per channel key. A run cannot be that grain, so a grain of ours is, and what it holds is a
/// registry mapping the channel to the runs that want it. That registry is what the phase-3 note meant by
/// "the delivery-registry design that belongs with distribution".
/// </para>
/// <para>
/// <b>One activation per channel key, probed.</b> The runtime activates this type under a grain key equal to
/// the channel's key, one activation per key, and calls
/// <see cref="IOnBroadcastChannelSubscribed.OnSubscribed"/> once per activation per
/// publishing provider. All three were measured on this repository's cluster rather than read off the
/// documentation, and the probe stays in the suite. The first of them is what makes the relay reachable at
/// all: a run composes the same key from its document and attaches to the very activation the runtime feeds.
/// </para>
/// <para>
/// <b>Non-durable on purpose.</b> Nothing here is persisted, because a channel is best-effort with no
/// history and a persisted receiver would be an address for a run that no longer exists. Losing the
/// activation loses the attachments, and the stated cost is that the runs attached to it go quiet rather
/// than fail — nothing links back from a relay to a run, exactly as nothing links back from a reminder
/// trigger to one.
/// </para>
/// <para>
/// <b>What it costs a deployment that never uses it.</b> The attribute makes this type an implicit
/// subscriber of one namespace in every silo that references this package, whether or not any document
/// names a broadcast source. That costs an entry in the runtime's subscriber table and nothing else: an
/// implicit subscriber is activated only when something publishes to its namespace, and nothing publishes
/// into a namespace this package owns unless it means to reach a run.
/// </para>
/// <para>
/// <b>The turn is never parked.</b> Forwarding happens on this grain's own turn and the pushes are awaited,
/// so a receiver that waited for room would hold the activation and every other run's delivery behind it —
/// and under a fire-and-forget provider it would hold it for nobody, since the publisher is not waiting.
/// That is why the source's payload refuses the backpressuring overflow policy: every other policy answers a
/// full ingress at once.
/// </para>
/// <para>
/// <b>Nothing thrown ever leaves.</b> Under a provider configured for checked delivery a subscriber's
/// exception is the publisher's exception — measured, not assumed — so one run's ingress problem would fail
/// a publication that has nothing to do with it. Every failure is therefore an outcome here, and a receiver
/// that refuses or fails is forgotten after one refusal, for the same reason the observer bridge forgets
/// one: an unreachable receiver costs the full response timeout per push.
/// </para>
/// </remarks>
internal interface IBroadcastRelayGrain : IGrainWithStringKey
{
    /// <summary>Attaches one run's receiver to this channel.</summary>
    /// <param name="subscriber">
    /// What this attachment is called — <c>{graph}/{run}/{node}</c>, so that one run's two occurrences of
    /// the source are two attachments and two runs are never one.
    /// </param>
    /// <param name="provider">
    /// The broadcast provider whose publications this run declared. A channel identity is a namespace and a
    /// key with no provider in it — probed — so two providers publishing one key reach this one activation,
    /// and this is what keeps a run from being handed elements from a channel it did not declare.
    /// </param>
    /// <param name="receiver">The run's receiver, created by the run itself.</param>
    /// <returns>A task that completes when the relay is forwarding to that run.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// That subscriber is already attached, which is one occurrence of one run competing with itself.
    /// </exception>
    Task AttachAsync(string subscriber, string provider, IDataflowPushReceiver receiver);

    /// <summary>Detaches one run's receiver, so later publications no longer reach it.</summary>
    /// <param name="subscriber">The name the attachment was made under.</param>
    /// <returns>A task that completes when the relay has stopped forwarding to that run.</returns>
    /// <remarks>Idempotent: detaching what is not attached is a no-op.</remarks>
    Task DetachAsync(string subscriber);

    /// <summary>Reports how many runs are listening to this channel right now.</summary>
    /// <returns>The number of attachments.</returns>
    /// <remarks>
    /// A reading of a moment and never a reservation, exactly as the observer bridge's is: a publication a
    /// moment later may still find nobody, because a run may have ended in between.
    /// </remarks>
    Task<int> ListenerCountAsync();
}

/// <summary>
/// The relay grain: one activation per channel key, holding the attachments of the runs listening to it.
/// </summary>
/// <remarks>
/// The subscription is attached as <see cref="object"/> and that is forced rather than lazy. Which CLR type
/// a channel carries is stated by the document of whichever run attaches, and
/// <see cref="OnSubscribed"/> may fire before any run has attached — or, as the probe showed, on an
/// activation a run created by attaching, with the subscription wired onto it afterwards. So the relay
/// cannot name the type, and Orleans allows it not to: an untyped attachment receives the author's own type
/// unchanged, which was probed. The type check happens where the type is known, on the run's own receiver.
/// </remarks>
[ImplicitChannelSubscription(OrleansBroadcastChannels.SourceNamespace)]
internal sealed class BroadcastRelayGrain : Grain, IBroadcastRelayGrain, IOnBroadcastChannelSubscribed
{
    private readonly Dictionary<string, Attachment> _attached = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public Task AttachAsync(string subscriber, string provider, IDataflowPushReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(subscriber);
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(receiver);

        if (!_attached.TryAdd(subscriber, new Attachment(provider, receiver)))
        {
            throw new InvalidOperationException(
                $"The broadcast relay '{this.GetPrimaryKeyString()}' already has '{subscriber}' listening on it. An attachment is named by a run and one of its nodes, so a second attachment under that name is one occurrence of one run competing with itself rather than a second listener.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DetachAsync(string subscriber)
    {
        ArgumentNullException.ThrowIfNull(subscriber);

        _ = _attached.Remove(subscriber);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<int> ListenerCountAsync() => Task.FromResult(_attached.Count);

    /// <inheritdoc/>
    public Task OnSubscribed(IBroadcastChannelSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        // Captured once per subscription rather than read per delivery, because the delivery handler is the
        // only place that knows which provider an element arrived through: the callback fires once per
        // provider on one activation, so the closure is the whole of the provider bookkeeping.
        string provider = subscription.ProviderName;

        return subscription.Attach<object>(
            element => DeliverAsync(provider, element),
            // A channel-level error is the provider telling one subscriber that its subscription is over. It
            // is not any particular run's failure and there is nothing here to fail: the runs stay attached
            // and a later publication reaches them, exactly as best-effort implies.
            static _ => Task.CompletedTask);
    }

    /// <summary>Forwards one published element to every run that declared this provider's channel.</summary>
    /// <param name="provider">The provider the element arrived through.</param>
    /// <param name="element">The element, as the publisher's own type.</param>
    /// <returns>A task that completes when every attached run has answered.</returns>
    /// <remarks>
    /// <para>
    /// The fan-out is concurrent and the reason is the cost of a run whose process has gone: a receiver
    /// nobody answers for costs the full response timeout, and paid one after another that cost would
    /// multiply by the number of listeners. Concurrency changes no ordering that exists — two runs are
    /// independent — and ordering within one run is kept by this grain being non-reentrant, so a publication
    /// is fully forwarded before the next one starts.
    /// </para>
    /// <para>
    /// A publication with nobody attached is dropped and says nothing, which is what best-effort means here:
    /// there is no history, no queue, and no subscriber list a publisher could have consulted first.
    /// </para>
    /// </remarks>
    private async Task DeliverAsync(string provider, object? element)
    {
        List<KeyValuePair<string, Attachment>> listening =
            [.. _attached.Where(attached => string.Equals(attached.Value.Provider, provider, StringComparison.Ordinal))];

        if (listening.Count == 0)
        {
            return;
        }

        (string Subscriber, DataflowPushOutcome Outcome)[] answered = await Task.WhenAll(
            listening.Select(attached => PushAsync(attached.Key, attached.Value.Receiver, element)));

        foreach ((string subscriber, DataflowPushOutcome outcome) in answered)
        {
            // Forgotten on the first refusal rather than asked again. A run that has ended refuses every
            // later push anyway, and a run whose silo is gone costs the whole response timeout each time it
            // is asked, so remembering it would make every publication on this channel pay for it.
            if (outcome is DataflowPushOutcome.Closed or DataflowPushOutcome.Failed)
            {
                _ = _attached.Remove(subscriber);
            }
        }
    }

    /// <summary>Offers one element to one run and reports what became of it, whatever happened.</summary>
    /// <param name="subscriber">The attachment's name.</param>
    /// <param name="receiver">The run's receiver.</param>
    /// <param name="element">The element.</param>
    /// <returns>The attachment's name and the outcome.</returns>
    /// <remarks>
    /// Every way a call into another process's memory can fail — a dead silo, a recycled run, a lost
    /// connection, a receiver that raised — means one thing to a channel: nobody is listening there any
    /// more. Saying that as an outcome is what keeps a subscriber's trouble from becoming a publisher's,
    /// which under a provider configured for checked delivery it otherwise would be.
    /// </remarks>
    private static async Task<(string Subscriber, DataflowPushOutcome Outcome)> PushAsync(
        string subscriber,
        IDataflowPushReceiver receiver,
        object? element)
    {
        try
        {
            return (subscriber, await receiver.PushAsync(element));
        }
        catch (Exception)
        {
            return (subscriber, DataflowPushOutcome.Closed);
        }
    }

    /// <summary>One run's place in the registry.</summary>
    /// <param name="Provider">The broadcast provider that run declared.</param>
    /// <param name="Receiver">The run's receiver.</param>
    private sealed record Attachment(string Provider, IDataflowPushReceiver Receiver);
}
