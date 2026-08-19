namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// A stage that throws, inside a supervision scope, twice over.
/// </summary>
/// <remarks>
/// <para>
/// A supervision scope is a section of a graph that answers the failures raised inside it, and the answer is
/// written into the document. Two of the four answers are shown here because they are the two an operator
/// reaches for, and they are two graphs rather than one for a reason worth knowing: <b>this library refuses
/// a scope inside a scope</b>, on the grounds that which of two nested policies wins is a contract nobody has
/// written yet. So "retry, and if that runs out substitute a fallback" is not one scope with two answers; it
/// is a choice between them.
/// </para>
/// <para>
/// The retrying graph offers the failing order again with a declared ladder of waits, and the third offer
/// succeeds, so nothing is lost. The recovering graph meets an order that fails every time, emits a declared
/// fallback in its place, and ends the scope's stream successfully — so everything below the scope drains and
/// the run reports success with fewer orders than it started with.
/// </para>
/// <para>
/// Both runs print the same three counters afterwards, read from
/// <see cref="RunHandle.Snapshot"/>, because "the run succeeded" and "nothing went wrong" are two different
/// readings and the counters are where the difference lives. That snapshot is where a run's diagnostics are:
/// status, dropped elements, supervised failures, poison elements, checkpoints, and the time checkpoints
/// held the run.
/// </para>
/// </remarks>
internal static class Failure
{
    /// <summary>How many times the retrying scope offers one order before giving up.</summary>
    /// <remarks>Attempts and not retries, so three means one offer and two re-offers.</remarks>
    private const int Attempts = 3;

    /// <summary>The order the retrying graph's stage refuses, twice, before letting it through.</summary>
    private const int FlakyOrder = 1;

    /// <summary>The order the recovering graph's stage refuses every single time.</summary>
    private const int PoisonOrder = 2;

    /// <summary>How long the retrying scope waits before each re-offer.</summary>
    /// <remarks>
    /// A ladder rather than a base and a factor, because a ladder is what a document can state exactly: a
    /// reader of the payload sees the waits the run will take. The last rung repeats.
    /// </remarks>
    private static readonly TimeSpan[] Backoff = [TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(20)];

    /// <summary>Authors and runs both graphs.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Both fingerprints, what each run delivered, and the counters afterwards.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orders = SampleOrders.Take(sample.Scale.Pick(full: 6, smokeSize: 4));
        LocalDataflowHost host = new();
        List<GraphReading> graphs = [];
        List<Observation> observations =
        [
            Observation.Of("orders-in-the-feed", orders.Count),
            Observation.Of("declared-attempts", Attempts),
            Observation.Of("declared-backoff-rungs", Backoff.Length),
        ];

        // The retrying graph. Two failures inside a scope that allows three attempts, so the third offer of
        // the order the stage dislikes is the one that succeeds.
        FlakyStage flaky = new(FlakyOrder, 2);
        List<string> retried = [];

        RunnableGraph retrying = Source.From(orders)
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = Attempts,
                    Backoff = Backoff,
                    OnExhaustion = RetryExhaustion.Fail,
                },
                Flow.For<OrderEvent>().Select(order => flaky.Pass(order)))
            .Select(OrderDocument.FromEvent)
            .To(s => s.ForEach(document => retried.Add(document.OrderId)));

        RunSnapshot afterRetries;

        await using (RunHandle retryRun = await host.MaterializeAsync(retrying, cancellationToken))
        {
            await retryRun.Completion;

            afterRetries = retryRun.Snapshot();
        }

        graphs.Add(GraphReading.Of("retry", retrying));
        observations.Add(Observation.Of("retry/times-the-stage-threw", flaky.Raised));
        observations.Add(Observation.Of("retry/orders-delivered", string.Join(' ', retried)));
        observations.Add(Observation.Of("retry/run-status", afterRetries.Status.ToString()));
        observations.Add(Observation.Of("retry/supervised-failures", afterRetries.SupervisedFailures));
        observations.Add(Observation.Of("retry/poison-elements", afterRetries.PoisonElements));
        observations.Add(Observation.Of("retry/dropped-elements", afterRetries.DroppedElements));

        // The recovering graph. The stage refuses one order for ever, so the scope substitutes the declared
        // fallback and ends its stream there.
        FlakyStage poison = FlakyStage.AlwaysAt(PoisonOrder);
        List<string> recovered = [];
        OrderEvent fallback = new(-1, "order-fallback", "none", 0m, false);

        RunnableGraph recovering = Source.From(orders)
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Recover },
                Flow.For<OrderEvent>().Select(order => poison.Pass(order)),
                fallback)
            .Select(OrderDocument.FromEvent)
            .To(s => s.ForEach(document => recovered.Add(document.OrderId)));

        RunSnapshot afterRecovery;

        await using (RunHandle recoverRun = await host.MaterializeAsync(recovering, cancellationToken))
        {
            await recoverRun.Completion;

            afterRecovery = recoverRun.Snapshot();
        }

        graphs.Add(GraphReading.Of("recover", recovering));
        observations.Add(Observation.Of("recover/times-the-stage-threw", poison.Raised));
        observations.Add(Observation.Of("recover/orders-delivered", string.Join(' ', recovered)));
        observations.Add(Observation.Of("recover/run-status", afterRecovery.Status.ToString()));
        observations.Add(Observation.Of("recover/supervised-failures", afterRecovery.SupervisedFailures));
        observations.Add(Observation.Of("recover/poison-elements", afterRecovery.PoisonElements));
        observations.Add(Observation.Of("recover/dropped-elements", afterRecovery.DroppedElements));

        return ScenarioOutcome.Of(graphs, observations);
    }
}
