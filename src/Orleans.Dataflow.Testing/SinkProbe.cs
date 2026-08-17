using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The typed consumer facade over one run's sink probe.
/// </summary>
/// <typeparam name="T">The element type the probe receives.</typeparam>
/// <remarks>
/// The rendezvous itself belongs to the runtime, because holding an element on a segment's own thread under
/// the run's stop and pause discipline is runtime semantics. What is here is the part that needs the
/// element type and the part that decides what a test should see: the receipt the rendezvous answers with
/// becomes an element or a <see cref="ProbeTerminatedException"/>, and the run's outcome becomes either the
/// exception a test asked for or the failure of the expectation it wrote.
/// </remarks>
/// <param name="probe">The run's own rendezvous.</param>
internal sealed class SinkProbe<T>(LocalSinkProbe probe) : ISinkProbe<T>
{
    /// <inheritdoc/>
    public async ValueTask<T> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        LocalReceipt receipt = await probe.ReceiveAsync(cancellationToken).ConfigureAwait(false);

        return receipt.Received
            ? (T)receipt.Element!
            : throw ProbeTerminatedException.Ended("deliver", receipt.Outcome);
    }

    /// <inheritdoc/>
    public async ValueTask ExpectCompletedAsync(CancellationToken cancellationToken = default)
    {
        if (await Ended(cancellationToken).ConfigureAwait(false) is { } outcome)
        {
            throw ProbeTerminatedException.Expected("completed", outcome);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<Exception> ExpectFailedAsync(CancellationToken cancellationToken = default)
    {
        Exception? outcome = await Ended(cancellationToken).ConfigureAwait(false);

        return outcome is not null and not OperationCanceledException
            ? outcome
            : throw ProbeTerminatedException.Expected("failed", outcome);
    }

    /// <summary>Returns a one-line diagnostic summary of this probe.</summary>
    /// <returns>The literal <c>sink probe</c>.</returns>
    /// <remarks>A probe's interesting state is its run's, and the method never throws.</remarks>
    public override string ToString() => "sink probe";

    /// <summary>Waits for the run to end and reports how it did.</summary>
    /// <param name="cancellationToken">The caller's own token, which stops this wait and nothing else.</param>
    /// <returns>The run's failure, or <see langword="null"/> when it completed.</returns>
    private async ValueTask<Exception?> Ended(CancellationToken cancellationToken) =>
        await (cancellationToken.CanBeCanceled
            ? probe.Ended.WaitAsync(cancellationToken)
            : probe.Ended).ConfigureAwait(false);
}
