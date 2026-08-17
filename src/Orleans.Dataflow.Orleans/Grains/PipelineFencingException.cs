using System.Globalization;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// A control call carried an ownership epoch that is not the one the run it addressed was started with.
/// </summary>
/// <remarks>
/// <para>
/// Ownership of a run is claimed once, by the epoch the coordinator assigned when it started the run, and
/// every later control call restates that claim. A call carrying any other epoch is either older than the
/// run — a caller holding a ticket from before some other start — or newer, which is a caller from a
/// future this activation has not seen. Both are answered the same way, loudly, because a stale owner
/// silently succeeding is precisely the split-brain the epoch exists to prevent.
/// </para>
/// <para>
/// Ownership and existence are different questions and this type answers only the first. A call to a grain
/// where no run is active at all is answered by <see cref="PipelineRunLostException"/>, because "your claim
/// is out of date" and "there is nothing here to claim" send a caller to different places.
/// </para>
/// <para>
/// This is the run grain's half of the fencing. The coordinator's half is Orleans-native and needs no type
/// of its own: a stale coordinator activation writing its state hits the ETag conflict, is killed, and the
/// fresh activation re-reads the truth.
/// </para>
/// <para>
/// The exception crosses the grain boundary, so it carries its own serializer annotations and reports both
/// epochs. A caller that sees it knows not only that its claim was refused but which claim is current,
/// which is what makes the refusal actionable rather than merely loud.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class PipelineFencingException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="PipelineFencingException"/> class.</summary>
    public PipelineFencingException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineFencingException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    public PipelineFencingException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineFencingException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public PipelineFencingException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineFencingException"/> class.</summary>
    /// <param name="currentEpoch">The epoch the addressed run was started with.</param>
    /// <param name="callerEpoch">The epoch the refused call carried.</param>
    public PipelineFencingException(long currentEpoch, long callerEpoch)
        : base(Describe(currentEpoch, callerEpoch))
    {
        CurrentEpoch = currentEpoch;
        CallerEpoch = callerEpoch;
    }

    /// <summary>Gets the ownership epoch the addressed run was started with.</summary>
    /// <value>The claim currently held, which is what a caller has to hold to be heard.</value>
    [Id(0)]
    public long CurrentEpoch { get; init; }

    /// <summary>Gets the ownership epoch the refused call carried.</summary>
    [Id(1)]
    public long CallerEpoch { get; init; }

    /// <summary>Builds the message of a refusal.</summary>
    /// <param name="currentEpoch">The epoch the addressed run was started with.</param>
    /// <param name="callerEpoch">The epoch the refused call carried.</param>
    /// <returns>The message.</returns>
    private static string Describe(long currentEpoch, long callerEpoch) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"The call carries the ownership epoch {callerEpoch}, and this run was started with the epoch {currentEpoch}. A control call restates the claim its ticket was issued under, so a different epoch is a claim to a run this is not.");
}
