namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// An asynchronous mapping with a declared concurrency bound, ordered and unordered.
/// </summary>
/// <remarks>
/// <para>
/// Two things are on show and they are independent of each other. The first is that the concurrency a graph
/// declares is exactly the concurrency it gets: the mapping holds every invocation until the declared number
/// of them are inside it together, so a run whose bound was not honored would wait rather than print a
/// number that was not true. The second is what ordering means — the first order's work is arranged to
/// finish after the rest of its concurrent batch, so an ordered mapping still emits it first and an
/// unordered one emits it after them.
/// </para>
/// <para>
/// The bound is the backpressure. An awaited call in flight is credit spent, and elements reach the stage
/// through a bounded channel, so "four at a time" is a statement about memory as much as about throughput.
/// </para>
/// </remarks>
internal static class AsyncWork
{
    /// <summary>How many invocations of the mapping may be in flight at once.</summary>
    private const int Declared = 4;

    /// <summary>Authors and runs the mapping once ordered and once unordered.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Both fingerprints, the peak concurrency each run reached, and what ordering did.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orders = SampleOrders.Take(sample.Scale.Pick(full: 8, smokeSize: 8));
        ParallelismOptions options = new() { MaxConcurrency = Declared };
        LocalDataflowHost host = new();
        List<GraphReading> graphs = [];
        List<Observation> observations =
        [
            Observation.Of("declared-max-concurrency", Declared),
            Observation.Of("orders-mapped", orders.Count),
        ];

        await AttemptAsync("ordered", unordered: false);
        await AttemptAsync("unordered", unordered: true);

        return ScenarioOutcome.Of(graphs, observations);

        // One authoring, run twice: everything except the operator's name is shared, so what the two runs
        // differ by is the operator and not the arrangement around it.
        async Task AttemptAsync(string name, bool unordered)
        {
            Concurrency concurrency = new(Declared);

            // The rest of the first concurrent batch, and not the rest of the feed: an ordered mapping holds
            // a completed result until everything before it has been emitted, so waiting on an order outside
            // the declared window would wait for one that can never be admitted.
            Countdown others = new(Declared - 1);
            List<string> arrived = [];

            async Task<OrderDocument> AcceptAsync(OrderEvent order, CancellationToken token)
            {
                await concurrency.EnterAsync(token);

                if (order.Sequence == 0)
                {
                    await others.WaitAsync(token);
                }
                else
                {
                    others.Signal();
                }

                return OrderDocument.FromEvent(order);
            }

            Source<OrderEvent> feed = Source.From(orders);

            RunnableGraph graph = (unordered
                    ? feed.SelectAsyncUnordered(options, AcceptAsync)
                    : feed.SelectAsync(options, AcceptAsync))
                .To(s => s.ForEach(document => arrived.Add(document.OrderId)));

            await using (RunHandle run = await host.MaterializeAsync(graph, cancellationToken))
            {
                await run.Completion;
            }

            bool inFeedOrder = orders.Select(order => order.OrderId).SequenceEqual(arrived);

            graphs.Add(GraphReading.Of(name, graph));
            observations.Add(Observation.Of($"{name}/peak-invocations-in-flight", concurrency.Peak));
            observations.Add(Observation.Of($"{name}/orders-emitted", arrived.Count));
            observations.Add(Observation.Of($"{name}/emitted-in-feed-order", inFeedOrder));
            observations.Add(
                Observation.Of($"{name}/first-order-emitted-first", arrived[0] == orders[0].OrderId));
        }
    }
}
