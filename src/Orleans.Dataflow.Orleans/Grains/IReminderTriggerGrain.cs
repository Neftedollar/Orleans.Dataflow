using Orleans.Runtime;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The clock of one reminder-triggered run: the grain that owns the reminder definition and forwards its
/// ticks into the run's ingress.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a grain of its own.</b> A reminder belongs to a grain, and only a grain can register or receive
/// one. A run is not a grain — it executes on threads beside the grains of its silo — so the trigger is a
/// grain the run starts, keyed by <c>{graph}/{run}/{node}</c>, whose whole job is to hold one reminder and
/// hand each tick to the run's receiver.
/// </para>
/// <para>
/// <b>What survives and what does not, verbatim.</b> The reminder <em>definition</em> survives restarts;
/// the run does not. Missed ticks are never replayed: a reminder that should have fired while nothing was
/// running fires once when a silo picks it up again, and the ticks in between are gone. So a tick that
/// arrives to a trigger with no live receiver — because this activation was recycled, because the run
/// grain was, or because the run ended without a chance to clean up — is the end of that reminder: the
/// trigger unregisters it and returns. Nothing resumes, and the attempt that owned the reminder stays
/// exactly as it ended, faulted or lost. Continuing a run from where it stopped is what a durable run's
/// checkpoint is for, and nothing here quietly approximates it.
/// </para>
/// <para>
/// <b>Ticks do not queue.</b> The stage's ingress is bounded and its overflow policy may not be
/// backpressure, because a clock cannot be slowed: a tick that finds no room is dropped or fails by the
/// declared policy, and the reminder keeps its own schedule. That is also what keeps this grain's turn
/// free — a tick forwarded into a full queue answers at once instead of parking the activation that owns
/// the cluster's reminder for this run.
/// </para>
/// </remarks>
public interface IReminderTriggerGrain : IGrainWithStringKey
{
    /// <summary>Registers the reminder and starts forwarding its ticks to one run.</summary>
    /// <param name="receiver">The run's receiver, created by the run itself.</param>
    /// <param name="periodMilliseconds">The period between ticks, in milliseconds.</param>
    /// <returns>A task that completes when the reminder is registered.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="receiver"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="periodMilliseconds"/> is not positive.</exception>
    /// <remarks>
    /// The first tick arrives one period after this call, because a reminder's due time is its period here:
    /// a trigger that fired immediately would make "every minute" mean "now and then every minute", which
    /// is a different contract and one the author did not write.
    /// </remarks>
    Task StartAsync(IDataflowPushReceiver receiver, long periodMilliseconds);

    /// <summary>Unregisters the reminder and stops forwarding.</summary>
    /// <returns>A task that completes when the reminder is gone.</returns>
    /// <remarks>
    /// Idempotent, and called by the run on every terminal path it can still reach: completion, a graceful
    /// shutdown, a cancellation, and the disposal a deactivating run grain performs. What it cannot cover
    /// is a silo that stopped without running anything, which is exactly the case the tick-side cleanup
    /// exists for.
    /// </remarks>
    Task StopAsync();

    /// <summary>Reports whether a reminder for this trigger is registered right now.</summary>
    /// <returns><see langword="true"/> when the cluster holds a reminder definition for this trigger.</returns>
    /// <remarks>
    /// The one honest way to ask, because a reminder registry may only be read from inside a grain. It
    /// reads the cluster's own record rather than this activation's memory, so it answers correctly for a
    /// trigger whose activation has been recycled — which is precisely the state worth asking about.
    /// </remarks>
    Task<bool> IsScheduledAsync();
}

/// <summary>
/// The trigger grain: one reminder, one receiver, and a tick counter.
/// </summary>
/// <remarks>
/// Nothing here is persisted. The reminder is the only durable thing in the design and it is Orleans' own;
/// the receiver and the tick index live in this activation, so a recycled activation has no receiver and
/// its next tick is what removes the reminder. That is the cleanup path stated as code.
/// </remarks>
internal sealed class ReminderTriggerGrain : Grain, IReminderTriggerGrain, IRemindable
{
    /// <summary>The name every dataflow trigger registers its reminder under.</summary>
    /// <remarks>
    /// One constant rather than a composed name, because the grain key already separates one trigger from
    /// every other: reminder names are scoped to their grain, so a name that repeated the key would say the
    /// same thing twice.
    /// </remarks>
    internal const string ReminderName = "orleans-dataflow-trigger";

    private IDataflowPushReceiver? _receiver;
    private long _ticks;

    /// <inheritdoc/>
    public async Task StartAsync(IDataflowPushReceiver receiver, long periodMilliseconds)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        ArgumentOutOfRangeException.ThrowIfLessThan(periodMilliseconds, 1);

        TimeSpan period = TimeSpan.FromMilliseconds(periodMilliseconds);

        _receiver = receiver;
        _ticks = 0;

        _ = await this.RegisterOrUpdateReminder(ReminderName, period, period);
    }

    /// <inheritdoc/>
    public async Task StopAsync()
    {
        _receiver = null;

        await UnregisterAsync();
    }

    /// <inheritdoc/>
    public async Task<bool> IsScheduledAsync() => await this.GetReminder(ReminderName) is not null;

    /// <inheritdoc/>
    /// <remarks>
    /// Three outcomes and one rule. A tick with no receiver removes the reminder, because the run that
    /// asked for it is gone and nothing here can find it again. A tick the run accepted or dropped leaves
    /// everything as it is, because a dropped tick is the declared overflow policy doing its job and not a
    /// reason to stop the clock. A tick the run refused — closed or failed — is the run saying it has ended
    /// without having been able to say so, so the reminder is removed there too.
    /// </remarks>
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (_receiver is not { } receiver)
        {
            await UnregisterAsync();

            return;
        }

        DataflowPushOutcome outcome;

        try
        {
            outcome = await receiver.PushAsync(_ticks);
        }
        catch (Exception)
        {
            // The receiver is a reference into another process's memory, so every way that process can
            // stop — a dead silo, a recycled run, a lost connection — arrives here as a failed call rather
            // than as an outcome. All of them mean the same thing: nobody is listening any more.
            outcome = DataflowPushOutcome.Closed;
        }

        if (outcome is DataflowPushOutcome.Closed or DataflowPushOutcome.Failed)
        {
            _receiver = null;

            await UnregisterAsync();

            return;
        }

        _ticks++;
    }

    /// <summary>Removes this trigger's reminder if the cluster still holds one.</summary>
    /// <returns>A task that completes when no reminder remains.</returns>
    /// <remarks>
    /// The lookup comes first because unregistering takes the reminder itself, and asking for one that is
    /// not there is the ordinary case rather than an error: a trigger is stopped by whichever of the run
    /// and the tick gets there first, and both call this.
    /// </remarks>
    private async Task UnregisterAsync()
    {
        if (await this.GetReminder(ReminderName) is { } reminder)
        {
            await this.UnregisterReminder(reminder);
        }
    }
}
