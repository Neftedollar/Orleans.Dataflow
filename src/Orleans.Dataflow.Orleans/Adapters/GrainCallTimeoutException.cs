using System.Globalization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// The failure of a grain call that did not reply within the timeout its stage declared.
/// </summary>
/// <remarks>
/// <para>
/// A type of its own rather than an <see cref="OperationCanceledException"/>, because the two mean opposite
/// things to a caller: a cancelled run resolves nothing and was asked to stop, and a timed-out call is a
/// run that failed and has a diagnosis. Folding the timeout into a cancellation would make every expired
/// call look like a shutdown somebody requested.
/// </para>
/// <para>
/// The timeout is this adapter's own and is enforced whether or not the registered call honors the token it
/// was given: the wait is bounded here, and the token is cancelled beside it so that a call which does
/// honor it stops rather than running on unobserved. Orleans 10 defaults
/// <c>MessagingOptions.CancelRequestOnTimeout</c> to false, so Orleans' own call timeout would not have
/// cancelled the grain side either — which is exactly why the stage carries a timeout of its own.
/// </para>
/// <para>
/// The exception reaches a remote caller as its type name and its message rather than as itself, because
/// a run's failure crosses a grain boundary as text.
/// </para>
/// </remarks>
public sealed class GrainCallTimeoutException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="GrainCallTimeoutException"/> class.</summary>
    public GrainCallTimeoutException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GrainCallTimeoutException"/> class.</summary>
    /// <param name="message">The message.</param>
    public GrainCallTimeoutException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GrainCallTimeoutException"/> class.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The cause.</param>
    public GrainCallTimeoutException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GrainCallTimeoutException"/> class.</summary>
    /// <param name="call">The registered call's name.</param>
    /// <param name="timeout">The timeout the stage declared.</param>
    /// <remarks>
    /// The message names the call and the duration, which are the two things a reader has to change: either
    /// the grain is slower than the stage assumed, or the stage's timeout is shorter than the work.
    /// </remarks>
    internal GrainCallTimeoutException(string call, TimeSpan timeout)
        : base(string.Create(
            CultureInfo.InvariantCulture,
            $"The grain call '{call}' did not reply within the {timeout.TotalMilliseconds} ms this stage declared. The call was asked to cancel and the element it was handed is not retried; a run's first failure is what the run reports."))
    {
    }
}
