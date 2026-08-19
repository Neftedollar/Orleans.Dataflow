namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// A source, a filter, a map, and a fold, run locally, with one typed result slot.
/// </summary>
/// <remarks>
/// <para>
/// This is the repository README's C# snippet, complete and running. Read it first: the four lines below are
/// the whole authoring vocabulary a reader needs before any of the seven scenarios after this one makes
/// sense, and its F# twin next door is the README's other snippet, unchanged.
/// </para>
/// <para>
/// Building a graph starts nothing. <c>Source.From</c> through <c>To</c> produces an immutable value — a
/// document, a fingerprint, and a typed slot — and only <c>MaterializeAsync</c> makes anything run. That
/// separation is why the same value can be fingerprinted, compared against another frontend's, and shipped
/// to a silo.
/// </para>
/// </remarks>
internal static class FirstPipeline
{
    /// <summary>Authors the pipeline in C#, runs it, and reports what it produced.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>The graph's fingerprint and the count the fold resolved.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orderEvents = SampleOrders.Take(sample.Scale.Pick(full: 12, smokeSize: 4));

        // The README's snippet. The slot comes back through an out parameter because the graph is what the
        // chain answers with; F# hands back a tuple instead, and the document is the same either way.
        RunnableGraph graph = Source.From(orderEvents)
            .Where(order => order.IsValid)
            .Select(OrderDocument.FromEvent)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

        await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);

        long processedOrders = await run.GetValueAsync(processed, cancellationToken);

        await run.Completion;

        return ScenarioOutcome.Of(
            [GraphReading.Of("main", graph)],
            [
                Observation.Of("orders-in-the-feed", orderEvents.Count),
                Observation.Of("orders-the-filter-kept", processedOrders),
            ]);
    }
}
