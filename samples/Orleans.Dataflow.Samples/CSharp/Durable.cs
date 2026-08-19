using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// A durable run that dies, and a second host that continues it from where it got to.
/// </summary>
/// <remarks>
/// <para>
/// A durable run writes its position into a checkpoint store on a cadence its options declare — here, every
/// few orders. When the first attempt dies, the store still holds the last position that was written down; a
/// second host handed the same document, the same run identity, and the same store continues from there
/// rather than from the beginning.
/// </para>
/// <para>
/// <b>The window between the last checkpoint and the crash is delivered twice, and that is the contract.</b>
/// This is at-least-once delivery, stated as a number the sample prints rather than as a footnote: the
/// orders between the stored position and the moment the attempt died are exactly the orders both attempts
/// saw. Narrowing the window is what the cadence is for, and it is never zero.
/// </para>
/// <para>
/// The store is this application's own <see cref="SampleCheckpointStore"/>, written against the published
/// interface in fifty lines. Nothing test-only is involved: implementing that interface is what a deployment
/// does, and its doc comment says which of the three duties it honors and which it fakes for a demonstration
/// that lives in one process.
/// </para>
/// </remarks>
internal static class Durable
{
    /// <summary>The name the two attempts of this run share.</summary>
    /// <remarks>
    /// What separates two durable runs of one graph. A local graph is anonymous, so without a run identity
    /// there would be nothing for a store to key a checkpoint by.
    /// </remarks>
    private static readonly RunId Run = RunId.Create("orders-of-the-day");

    /// <summary>Authors the graph, kills the first attempt, and continues it on a second host.</summary>
    /// <param name="sample">The run this scenario belongs to, which supplies the checkpoint store.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The fingerprint, what each attempt delivered, and the replay window between them.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orders = SampleOrders.Take(sample.Scale.Pick(full: 12, smokeSize: 6));
        int crashAt = sample.Scale.Pick(full: 8, smokeSize: 5);
        int everyElements = sample.Scale.Pick(full: 3, smokeSize: 2);

        ICheckpointStore store = sample.NewCheckpointStore();

        DurableRunOptions Durability() => new()
        {
            Store = store,
            RunId = Run,
            EveryElements = everyElements,
        };

        List<string> firstAttempt = [];
        List<string> secondAttempt = [];

        RunnableGraph Build(int failAt, List<string> seen) =>
            Source.From(orders)
                .Select(order => order.Sequence == failAt
                    ? throw new InvalidOperationException(
                        $"The host died while handling {order.OrderId}. This is the sample's deliberate crash.")
                    : order)
                .To(s => s.ForEach(order => seen.Add(order.OrderId)));

        RunnableGraph crashing = Build(crashAt, firstAttempt);
        RunnableGraph continuing = Build(-1, secondAttempt);

        string failure;
        RunSnapshot afterCrash;

        await using (RunHandle attempt =
            await new LocalDataflowHost().MaterializeDurableAsync(crashing, Durability(), cancellationToken))
        {
            try
            {
                await attempt.Completion;

                failure = "the attempt completed, which means the crash never happened";
            }
            catch (InvalidOperationException crash)
            {
                failure = crash.Message;
            }

            afterCrash = attempt.Snapshot();
        }

        // A second host, standing in for a second process: it is handed the same document, the same run
        // identity and the same store, and nothing else passes between them.
        RunSnapshot afterResume;

        await using (RunHandle continued =
            await new LocalDataflowHost().MaterializeFromCheckpointAsync(continuing, Durability(), cancellationToken))
        {
            await continued.Completion;

            afterResume = continued.Snapshot();
        }

        HashSet<string> replayed = new(firstAttempt, StringComparer.Ordinal);

        replayed.IntersectWith(secondAttempt);

        HashSet<string> delivered = new(firstAttempt, StringComparer.Ordinal);

        delivered.UnionWith(secondAttempt);

        return ScenarioOutcome.Of(
            [GraphReading.Of("main", crashing)],
            [
                Observation.Of("orders-in-the-feed", orders.Count),
                Observation.Of("checkpoint-every-orders", everyElements),
                Observation.Of("both-attempts-are-one-document", crashing.Fingerprint == continuing.Fingerprint),
                Observation.Of("first-attempt/delivered", string.Join(' ', firstAttempt)),
                Observation.Of("first-attempt/status", afterCrash.Status.ToString()),
                Observation.Of("first-attempt/checkpoints-written", afterCrash.Checkpoints),
                Observation.Of("first-attempt/failure", failure),
                Observation.Of("second-attempt/delivered", string.Join(' ', secondAttempt)),
                Observation.Of("second-attempt/status", afterResume.Status.ToString()),
                Observation.Of(
                    "delivered-twice-the-at-least-once-window",
                    string.Join(' ', replayed.OrderBy(order => order, StringComparer.Ordinal))),
                Observation.Of("every-order-delivered-at-least-once", delivered.Count == orders.Count),
            ]);
    }
}
