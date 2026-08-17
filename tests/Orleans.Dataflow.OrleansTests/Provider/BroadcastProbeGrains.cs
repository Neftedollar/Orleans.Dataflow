using Microsoft.Extensions.DependencyInjection;
using Orleans.BroadcastChannel;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// The grain that records everything the runtime does to an implicit channel subscriber.
/// </summary>
/// <remarks>
/// The whole of the broadcast <em>source</em> design rests on facts about implicit subscription that
/// Microsoft's documentation states loosely: which grain key the runtime activates a subscriber under, how
/// often the subscription callback fires, and whether two channel keys of one namespace are one activation
/// or two. None of that could be read off an API surface, so this grain records it and the probe tests
/// assert it. It is the same shape the reminder and stream probes take: the answer stays true because a run
/// of the suite re-asks the question.
/// </remarks>
public interface IBroadcastProbeGrain : IGrainWithStringKey
{
    /// <summary>Reports what this activation has seen.</summary>
    /// <returns>The report.</returns>
    Task<BroadcastProbeReport> ReportAsync();

    /// <summary>Makes the next delivery throw, so a publisher's view of a failing subscriber is observable.</summary>
    /// <returns>A task that completes when the refusal is armed.</returns>
    Task RefuseNextAsync();
}

/// <summary>What one implicit subscriber activation has seen.</summary>
/// <param name="Activation">
/// A value made once per activation, so two reports carrying two values are two activations.
/// </param>
/// <param name="PrimaryKey">The grain key the runtime activated this subscriber under.</param>
/// <param name="Subscriptions">
/// One entry per <c>OnSubscribed</c> call, spelled <c>{provider}|{namespace}|{key}</c>.
/// </param>
/// <param name="Received">The identity of every element delivered to this activation, in arrival order.</param>
[GenerateSerializer]
public sealed record BroadcastProbeReport(
    [property: Id(0)] string Activation,
    [property: Id(1)] string PrimaryKey,
    [property: Id(2)] List<string> Subscriptions,
    [property: Id(3)] List<string> Received);

/// <summary>The implicit subscriber that records rather than consumes.</summary>
[ImplicitChannelSubscription(BroadcastObservations.ProbeNamespace)]
internal sealed class BroadcastProbeGrain : Grain, IBroadcastProbeGrain, IOnBroadcastChannelSubscribed
{
    private readonly string _activation = Guid.NewGuid().ToString("N");
    private readonly List<string> _subscriptions = [];
    private readonly List<string> _received = [];
    private bool _refuseNext;

    /// <inheritdoc/>
    public Task<BroadcastProbeReport> ReportAsync() =>
        Task.FromResult(new BroadcastProbeReport(
            _activation,
            this.GetPrimaryKeyString(),
            [.. _subscriptions],
            [.. _received]));

    /// <inheritdoc/>
    public Task RefuseNextAsync()
    {
        _refuseNext = true;

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task OnSubscribed(IBroadcastChannelSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        _subscriptions.Add(
            $"{subscription.ProviderName}|{subscription.ChannelId.GetNamespace()}|{subscription.ChannelId.GetKeyAsString()}");

        return subscription.Attach<AdapterOrder>(
            order =>
            {
                if (_refuseNext)
                {
                    _refuseNext = false;

                    throw new InvalidTimeZoneException("the probe refused this delivery");
                }

                _received.Add(order.Id);

                return Task.CompletedTask;
            },
            _ => Task.CompletedTask);
    }
}

/// <summary>
/// The grain that subscribes to a channel without knowing what the channel carries.
/// </summary>
/// <remarks>
/// The relay grain cannot know a channel's CLR element type when the runtime subscribes it: the type comes
/// from the document of whichever run attaches, and the subscription callback may fire before any run has.
/// So the relay has to attach as <see cref="object"/>, and whether Orleans allows that is a fact rather than
/// a preference. This grain is that question, asked twice — once with the activation created by the
/// publication and once with an activation that already existed before anything was published.
/// </remarks>
public interface IBroadcastObjectProbeGrain : IGrainWithStringKey
{
    /// <summary>Reports what this activation has seen.</summary>
    /// <returns>The report, whose received entries are spelled <c>{type}:{identity}</c>.</returns>
    Task<BroadcastProbeReport> ReportAsync();

    /// <summary>Activates this grain without publishing anything at it.</summary>
    /// <returns>A task that completes once the activation exists.</returns>
    Task ActivateAsync();
}

/// <summary>The untyped subscriber.</summary>
[ImplicitChannelSubscription(BroadcastObservations.ObjectProbeNamespace)]
internal sealed class BroadcastObjectProbeGrain : Grain, IBroadcastObjectProbeGrain, IOnBroadcastChannelSubscribed
{
    private readonly string _activation = Guid.NewGuid().ToString("N");
    private readonly List<string> _subscriptions = [];
    private readonly List<string> _received = [];

    /// <inheritdoc/>
    public Task<BroadcastProbeReport> ReportAsync() =>
        Task.FromResult(new BroadcastProbeReport(
            _activation,
            this.GetPrimaryKeyString(),
            [.. _subscriptions],
            [.. _received]));

    /// <inheritdoc/>
    public Task ActivateAsync() => Task.CompletedTask;

    /// <inheritdoc/>
    public Task OnSubscribed(IBroadcastChannelSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        _subscriptions.Add(
            $"{subscription.ProviderName}|{subscription.ChannelId.GetNamespace()}|{subscription.ChannelId.GetKeyAsString()}");

        return subscription.Attach<object>(
            item =>
            {
                _received.Add($"{item?.GetType().Name ?? "null"}:{(item as AdapterOrder)?.Id ?? "-"}");

                return Task.CompletedTask;
            },
            _ => Task.CompletedTask);
    }
}

/// <summary>
/// The grain that records whether two deliveries to one subscriber can overlap.
/// </summary>
/// <remarks>
/// The relay grain keeps its attach table in an ordinary dictionary and mutates it while forwarding, which
/// is safe exactly as long as two deliveries to one activation cannot run at once. Grains are non-reentrant
/// by default and the channel's consumer extension carries no interleaving attribute, but "by default" and
/// "carries no attribute" are readings of an API rather than a measurement — and a dictionary corrupted by a
/// second turn would be the kind of defect that shows up as a lost run months later. This grain takes its
/// time over each delivery and writes down when it entered and left, so the question is settled by what
/// happened.
/// </remarks>
public interface IBroadcastSerialProbeGrain : IGrainWithStringKey
{
    /// <summary>Reports what this activation has seen.</summary>
    /// <returns>The report, whose received entries are the interleaving of enters and exits.</returns>
    Task<BroadcastProbeReport> ReportAsync();

    /// <summary>Activates this grain so that the deliveries reach one that already exists.</summary>
    /// <returns>A task that completes once the activation exists.</returns>
    Task ActivateAsync();
}

/// <summary>The deliberately slow subscriber.</summary>
[ImplicitChannelSubscription(BroadcastObservations.SerialProbeNamespace)]
internal sealed class BroadcastSerialProbeGrain : Grain, IBroadcastSerialProbeGrain, IOnBroadcastChannelSubscribed
{
    /// <summary>How long one delivery is held, so that an overlapping second one would be visible.</summary>
    /// <remarks>
    /// Long enough that two publications dispatched back to back would certainly overlap if the runtime
    /// allowed them to, and short enough to cost the suite nothing worth counting.
    /// </remarks>
    private static readonly TimeSpan Dwell = TimeSpan.FromMilliseconds(200);

    private readonly string _activation = Guid.NewGuid().ToString("N");
    private readonly List<string> _subscriptions = [];
    private readonly List<string> _received = [];

    /// <inheritdoc/>
    public Task<BroadcastProbeReport> ReportAsync() =>
        Task.FromResult(new BroadcastProbeReport(
            _activation,
            this.GetPrimaryKeyString(),
            [.. _subscriptions],
            [.. _received]));

    /// <inheritdoc/>
    public Task ActivateAsync() => Task.CompletedTask;

    /// <inheritdoc/>
    public Task OnSubscribed(IBroadcastChannelSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        _subscriptions.Add(subscription.ProviderName);

        return subscription.Attach<object>(
            async item =>
            {
                string id = (item as AdapterOrder)?.Id ?? "-";

                _received.Add($"enter:{id}");

                await Task.Delay(Dwell);

                _received.Add($"exit:{id}");
            },
            static _ => Task.CompletedTask);
    }
}

/// <summary>The grain that publishes to a Broadcast Channel from inside a silo.</summary>
/// <remarks>
/// Publication has to happen somewhere that can resolve a named broadcast provider, and the test client does
/// not register one. A grain is that place, and making the publication's outcome the reply is what lets a
/// probe see whether a subscriber's failure reaches the publisher at all.
/// </remarks>
public interface IBroadcastPublisherGrain : IGrainWithStringKey
{
    /// <summary>Publishes one order and reports what the publication did.</summary>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="channelNamespace">The channel's namespace.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="order">The order.</param>
    /// <returns><c>published</c>, or <c>threw:{type}:{message}</c>.</returns>
    Task<string> PublishAsync(string provider, string channelNamespace, string key, AdapterOrder order);

    /// <summary>Publishes one price, which is the wrong type for every channel these tests declare.</summary>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="channelNamespace">The channel's namespace.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="price">The price.</param>
    /// <returns><c>published</c>, or <c>threw:{type}:{message}</c>.</returns>
    /// <remarks>
    /// A channel is untyped, so nothing stops a publisher from putting a second type on one — which is
    /// exactly the situation a consuming run has to survive, and the reason this method exists.
    /// </remarks>
    Task<string> PublishPriceAsync(string provider, string channelNamespace, string key, AdapterPrice price);
}

/// <summary>The publisher.</summary>
internal sealed class BroadcastPublisherGrain(IServiceProvider services) : Grain, IBroadcastPublisherGrain
{
    /// <inheritdoc/>
    public Task<string> PublishAsync(
        string provider,
        string channelNamespace,
        string key,
        AdapterOrder order) =>
        PublishElementAsync(provider, channelNamespace, key, order);

    /// <inheritdoc/>
    public Task<string> PublishPriceAsync(
        string provider,
        string channelNamespace,
        string key,
        AdapterPrice price) =>
        PublishElementAsync(provider, channelNamespace, key, price);

    /// <summary>Publishes one element of any type and reports what the publication did.</summary>
    /// <typeparam name="T">The element type the channel writer is opened under.</typeparam>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="channelNamespace">The channel's namespace.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="element">The element.</param>
    /// <returns><c>published</c>, or <c>threw:{type}:{message}</c>.</returns>
    private async Task<string> PublishElementAsync<T>(
        string provider,
        string channelNamespace,
        string key,
        T element)
    {
        try
        {
            await services.GetRequiredKeyedService<IBroadcastChannelProvider>(provider)
                .GetChannelWriter<T>(ChannelId.Create(channelNamespace, key))
                .Publish(element);

            return "published";
        }
        catch (Exception thrown)
        {
            return $"threw:{thrown.GetType().Name}:{thrown.Message}";
        }
    }
}
