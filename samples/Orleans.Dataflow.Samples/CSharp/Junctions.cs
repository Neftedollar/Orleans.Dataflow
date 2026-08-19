namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// One stream broadcast into two branches, each with a result of its own.
/// </summary>
/// <remarks>
/// <para>
/// A branch is a flow that ends in a terminal, and a junction call is what turns a list of them into a
/// closed graph. Every element reaches every branch, so the two counts below are two readings of the same
/// orders rather than a partition of them — which is the difference between a broadcast and the balance and
/// partition junctions beside it.
/// </para>
/// <para>
/// A broadcast asks every leg for room before it pulls, so a branch that stops consuming holds all of them
/// up. That is the bounded memory this junction buys: nothing anywhere accumulates on behalf of a slow leg.
/// </para>
/// </remarks>
internal static class Junctions
{
    /// <summary>What an order has to be worth to count as large.</summary>
    private const decimal Large = 50m;

    /// <summary>Authors the broadcast, runs it, and reports both branches' results.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The graph's fingerprint and the two counts.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orders = SampleOrders.Take(sample.Scale.Pick(full: 12, smokeSize: 6));

        Branch<OrderDocument> largeBranch = Flow.For<OrderDocument>()
            .Where(document => document.Amount >= Large)
            .To(Sink.Count<OrderDocument>(), "large", out ResultSlot<long> largeSlot);

        Branch<OrderDocument> northBranch = Flow.For<OrderDocument>()
            .Where(document => document.Region == "north")
            .To(Sink.Count<OrderDocument>(), "north", out ResultSlot<long> northSlot);

        RunnableGraph graph = Source.From(orders)
            .Select(OrderDocument.FromEvent)
            .BroadcastTo(largeBranch, northBranch);

        await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);

        long largeOrders = await run.GetValueAsync(largeSlot, cancellationToken);
        long northOrders = await run.GetValueAsync(northSlot, cancellationToken);

        await run.Completion;

        return ScenarioOutcome.Of(
            [GraphReading.Of("main", graph)],
            [
                Observation.Of("orders-broadcast", orders.Count),
                Observation.Of("orders-worth-50-or-more", largeOrders),
                Observation.Of("orders-from-the-north", northOrders),
            ]);
    }
}
