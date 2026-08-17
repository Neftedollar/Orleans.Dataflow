using Microsoft.Extensions.Options;
using Orleans.BroadcastChannel;
using Orleans.Runtime;
using ReminderOptions = Orleans.Hosting.ReminderOptions;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// The grain that answers what a silo's reminder configuration actually is and what it does with a period
/// below it.
/// </summary>
/// <remarks>
/// Reminders can only be registered or read from inside a grain — Orleans refuses the registry from any
/// other context, which was probed rather than assumed — so a test that wants to know what a cluster
/// enforces has to ask a grain. That is the whole of this type.
/// </remarks>
public interface IReminderProbeGrain : IGrainWithStringKey
{
    /// <summary>Reports this silo's configured minimum reminder period.</summary>
    /// <returns>The period, as its round-trip text.</returns>
    Task<string> MinimumPeriodAsync();

    /// <summary>Attempts to register a reminder of a given period and reports what happened.</summary>
    /// <param name="milliseconds">The period to ask for.</param>
    /// <returns>
    /// <c>registered</c> when the registration succeeded, or <c>threw:{type}:{message}</c> when it did not.
    /// </returns>
    Task<string> RegisterAsync(long milliseconds);

    /// <summary>Reports how many reminders this grain has.</summary>
    /// <returns>The count.</returns>
    Task<int> ReminderCountAsync();

    /// <summary>Removes every reminder this grain has.</summary>
    /// <returns>A task that completes when none remain.</returns>
    Task UnregisterAllAsync();
}

/// <summary>The probe grain.</summary>
internal sealed class ReminderProbeGrain(IOptions<ReminderOptions> options)
    : Grain, IReminderProbeGrain, IRemindable
{
    /// <inheritdoc/>
    public Task<string> MinimumPeriodAsync() =>
        Task.FromResult(options.Value.MinimumReminderPeriod.ToString());

    /// <inheritdoc/>
    public async Task<string> RegisterAsync(long milliseconds)
    {
        TimeSpan period = TimeSpan.FromMilliseconds(milliseconds);

        try
        {
            _ = await this.RegisterOrUpdateReminder("probe", period, period);

            return "registered";
        }
        catch (Exception thrown)
        {
            return $"threw:{thrown.GetType().FullName}:{thrown.Message}";
        }
    }

    /// <inheritdoc/>
    public async Task<int> ReminderCountAsync() => (await this.GetReminders()).Count;

    /// <inheritdoc/>
    public async Task UnregisterAllAsync()
    {
        foreach (IGrainReminder reminder in await this.GetReminders())
        {
            await this.UnregisterReminder(reminder);
        }
    }

    /// <inheritdoc/>
    public Task ReceiveReminder(string reminderName, TickStatus status) => Task.CompletedTask;
}

/// <summary>
/// The grain that records what a Broadcast Channel delivered to it.
/// </summary>
/// <remarks>
/// An implicit subscriber, because a Broadcast Channel has no other kind: a grain type declares the
/// namespace it receives and the runtime activates one grain per channel key. That is exactly why there is
/// no channel <em>source</em> adapter in this phase — a run cannot be one of these.
/// </remarks>
public interface IBroadcastReceiverGrain : IGrainWithStringKey
{
    /// <summary>Reports the orders this receiver has been delivered, in arrival order.</summary>
    /// <returns>The orders.</returns>
    Task<List<AdapterOrder>> ReceivedAsync();
}

/// <summary>The implicit subscriber.</summary>
[ImplicitChannelSubscription(BroadcastObservations.ChannelNamespace)]
internal sealed class BroadcastReceiverGrain : Grain, IBroadcastReceiverGrain, IOnBroadcastChannelSubscribed
{
    private readonly List<AdapterOrder> _received = [];

    /// <inheritdoc/>
    public Task<List<AdapterOrder>> ReceivedAsync() => Task.FromResult(new List<AdapterOrder>(_received));

    /// <inheritdoc/>
    public Task OnSubscribed(IBroadcastChannelSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        return subscription.Attach<AdapterOrder>(
            order =>
            {
                _received.Add(order);

                return Task.CompletedTask;
            },
            _ => Task.CompletedTask);
    }
}

/// <summary>
/// What the broadcast tests agree on with the runtime.
/// </summary>
/// <remarks>
/// One constant, and it has to be one: a channel's namespace appears in the attribute that makes a grain
/// type an implicit subscriber and again in every address a test publishes to, and two spellings of it
/// would be a test that publishes into silence.
/// </remarks>
internal static class BroadcastObservations
{
    /// <summary>The namespace every broadcast test publishes into.</summary>
    internal const string ChannelNamespace = "adapter-broadcast";
}
