namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The two things a source of one run needs to know about that run: when it must abandon its work, and
/// when it may stop producing.
/// </summary>
/// <param name="RunToken">
/// The run's own token, cancelled when the run is cancelled and when anything in the run fails. This is
/// the token an author's callback receives.
/// </param>
/// <param name="StopToken">
/// Cancelled for everything <paramref name="RunToken"/> is cancelled for, and additionally when a graceful
/// shutdown is asked for.
/// </param>
/// <remarks>
/// <para>
/// The two are different because shutdown and cancellation are different (LOCAL-RUNTIME.md): cancellation
/// abandons the run, and shutdown stops it producing while everything already admitted keeps flowing. A
/// source that waits — for an offer, for a channel, for nothing at all — has to be released by both, and
/// has to tell them apart afterwards: a release by cancellation raises
/// <see cref="OperationCanceledException"/> and a release by shutdown ends the sequence as if the source
/// had run out.
/// </para>
/// <para>
/// Only the runtime's own waits observe <paramref name="StopToken"/>. A wait inside an author's delegate —
/// a slow enumerable, a generator that ignores its token — receives <paramref name="RunToken"/> and
/// nothing else, so shutdown still waits for it, which is the documented slow-source rule and not an
/// oversight.
/// </para>
/// </remarks>
internal readonly record struct LocalRunContext(CancellationToken RunToken, CancellationToken StopToken)
{
    /// <summary>Reports whether a wait released by <see cref="StopToken"/> was a graceful stop.</summary>
    /// <value>
    /// <see langword="true"/> when the run was asked to shut down and was not cancelled, which is the one
    /// case where a waiting source ends its sequence instead of raising.
    /// </value>
    internal bool ShuttingDown => StopToken.IsCancellationRequested && !RunToken.IsCancellationRequested;
}
