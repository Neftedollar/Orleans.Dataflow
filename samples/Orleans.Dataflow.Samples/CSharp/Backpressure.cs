namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// A fast source, a slow sink, and a bounded buffer between them, run under two policies.
/// </summary>
/// <remarks>
/// <para>
/// The same shape twice, so that the overflow policy is the only thing that differs and the two kept sets
/// are therefore a statement about the policy. The buffer is declared to hold three elements, the sink stops
/// dead on the first one it is given, and the source keeps going until it has offered everything it has.
/// What is left when the sink is let go is what the policy chose to keep.
/// </para>
/// <para>
/// The declared capacity is what bounds the memory: nothing anywhere in a run uses a mailbox as a buffer, so
/// three is three whatever the source does. The default policy is not on show here and is worth naming —
/// <see cref="OverflowPolicy.Backpressure"/> loses nothing and stalls the source instead, which is what a
/// pipeline that must not lose an order asks for.
/// </para>
/// </remarks>
internal static class Backpressure
{
    /// <summary>How many elements the declared buffer holds.</summary>
    private const int Capacity = 3;

    /// <summary>The two policies this scenario contrasts, in the order it runs them.</summary>
    private static readonly (string Name, OverflowPolicy Policy)[] Policies =
    [
        ("drop-oldest", OverflowPolicy.DropOldest),
        ("drop-newest", OverflowPolicy.DropNewest),
    ];

    /// <summary>Authors and runs the shape once per policy.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>One fingerprint and one kept set per policy.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orders = SampleOrders.Take(sample.Scale.Pick(full: 9, smokeSize: 6));
        LocalDataflowHost host = new();
        List<GraphReading> graphs = [];
        List<Observation> observations =
        [
            Observation.Of("declared-buffer-capacity", Capacity),
            Observation.Of("orders-offered", orders.Count),
        ];

        foreach ((string name, OverflowPolicy policy) in Policies)
        {
            // A gate the sink stops at, and a feed that waits for it to be stood at before running ahead.
            // Together they make "the source got ahead of the sink" a fact rather than a hope.
            Gate gate = new();
            PacedFeed<OrderEvent> feed = new(orders, gate);
            List<string> kept = [];

            RunnableGraph graph = Source.From(feed.Elements)
                .Buffer(new BufferOptions { Capacity = Capacity, OverflowPolicy = policy })
                .To(s => s.ForEach(order =>
                {
                    kept.Add(order.OrderId);
                    gate.Wait();
                }));

            await using RunHandle run = await host.MaterializeAsync(graph, cancellationToken);

            // Everything has now been offered to a buffer that could hold three of it, and the sink is
            // still standing on the first element it was given.
            await feed.Exhausted.WaitAsync(cancellationToken);

            gate.Open();

            await run.Completion;

            RunSnapshot snapshot = run.Snapshot();

            graphs.Add(GraphReading.Of(name, graph));
            observations.Add(Observation.Of($"{name}/orders-the-sink-saw", string.Join(' ', kept)));
            observations.Add(Observation.Of($"{name}/orders-dropped", snapshot.DroppedElements));
        }

        return ScenarioOutcome.Of(graphs, observations);
    }
}
