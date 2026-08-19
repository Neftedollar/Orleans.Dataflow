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

        // One authoring, run twice: the operator is what the two runs differ by, together with the one part
        // of the arrangement the operator's own contract forces to differ with it — which of the two
        // announces the rest of the batch. The note above the graph below is where that is argued.
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
                else if (!unordered)
                {
                    // Ordered only. See the sink below for who announces these orders in the unordered run,
                    // and why it cannot be this line.
                    others.Signal();
                }

                return OrderDocument.FromEvent(order);
            }

            Source<OrderEvent> feed = Source.From(orders);

            // Where the two runs stop being one arrangement, and the reason the callback above has a branch
            // in it. What the first order has to outlast is the rest of its batch being *emitted*, and a
            // callback returning is not that: its result is still on its way to the sink, so an arrangement
            // that counted returns would be counting the wrong event, and would flip whenever the first
            // order's result overtook one still in flight.
            //
            // Unordered: the sink announces each order as it emits it, which is the event the observation is
            // about, so the first order cannot be emitted first however the machine schedules the batch.
            //
            // Ordered: the callbacks announce themselves instead, and they must. An ordered mapping holds a
            // finished result until everything before it has been emitted, so a first order waiting to see
            // the rest of its batch emitted would be waiting for emissions that cannot happen until it is
            // emitted itself — the same deadlock the note above warns about, one step further in. Nothing is
            // lost by it: an ordered mapping emits the first order first because that is what ordered means,
            // so this run's answer is the operator's guarantee rather than the arrangement's.
            RunnableGraph graph = (unordered
                    ? feed.SelectAsyncUnordered(options, AcceptAsync)
                    : feed.SelectAsync(options, AcceptAsync))
                .To(s => s.ForEach(document =>
                {
                    arrived.Add(document.OrderId);

                    if (unordered)
                    {
                        others.Signal();
                    }
                }));

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
