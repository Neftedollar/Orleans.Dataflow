using System.Globalization;

namespace Orleans.Dataflow.Samples.CSharp;

/// <summary>
/// Bounded grouping by count and time, and a group-by that refuses a key past its bound.
/// </summary>
/// <remarks>
/// <para>
/// Two graphs, and the pair is the lesson. The first closes a group when either four orders have arrived or
/// a window has elapsed, whichever comes first, so the memory it holds is bounded by the count even when the
/// feed goes quiet. The second keeps one running substream per region and declares how many regions it is
/// willing to keep at once; the feed has three and the bound is two, so the third region is refused.
/// </para>
/// <para>
/// <b>The refusal is a designed outcome and not a crash.</b> The run fails with a named exception whose
/// message quotes both the bound the author declared and the key that exceeded it, which is what makes the
/// alternative — a keyed operator that quietly grows until the process dies — the thing this library does
/// not do. Choosing the other policy, evicting the least recently used key instead, is one field on the same
/// options record.
/// </para>
/// </remarks>
internal static class Windowing
{
    /// <summary>How many orders close a group.</summary>
    private const int GroupSize = 4;

    /// <summary>How many regions the keyed graph is willing to keep substreams for.</summary>
    private const int MaxActiveRegions = 2;

    /// <summary>How long a group stays open once its first order has arrived.</summary>
    /// <remarks>
    /// Long enough that the count is always what closes a group in this sample, so the batch sizes below are
    /// arithmetic rather than timing. A feed that went quiet mid-group would see the window close it.
    /// </remarks>
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(30);

    /// <summary>Authors and runs both graphs.</summary>
    /// <param name="sample">The run this scenario belongs to.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Both fingerprints, the batch sizes, and the refusal.</returns>
    internal static async Task<ScenarioOutcome> RunAsync(SampleRun sample, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sample);

        IReadOnlyList<OrderEvent> orders = SampleOrders.Take(sample.Scale.Pick(full: 12, smokeSize: 6));
        LocalDataflowHost host = new();
        List<GraphReading> graphs = [];
        List<Observation> observations =
        [
            Observation.Of("orders-in-the-feed", orders.Count),
            Observation.Of("declared-group-size", GroupSize),
        ];

        RunnableGraph batched = Source.From(orders)
            .Select(OrderDocument.FromEvent)
            .GroupedWithin(GroupSize, Window)
            .To(
                s => s.Collect(new CollectOptions { MaxElements = 32 }),
                "batches",
                out ResultSlot<IReadOnlyList<IReadOnlyList<OrderDocument>>> batches);

        IReadOnlyList<IReadOnlyList<OrderDocument>> groups;

        await using (RunHandle batchRun = await host.MaterializeAsync(batched, cancellationToken))
        {
            groups = await batchRun.GetValueAsync(batches, cancellationToken);

            await batchRun.Completion;
        }

        graphs.Add(GraphReading.Of("grouped-within", batched));
        observations.Add(Observation.Of("groups-emitted", groups.Count));
        observations.Add(
            Observation.Of(
                "group-sizes",
                string.Join(' ', groups.Select(group => group.Count.ToString(CultureInfo.InvariantCulture)))));

        // The second graph. One substream per region, two regions allowed, three regions in the feed.
        RunnableGraph keyed = Source.From(orders)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = MaxActiveRegions },
                order => order.Region,
                Flow.For<OrderEvent>())
            .To(Sink.Ignore<OrderEvent>());

        graphs.Add(GraphReading.Of("bounded-keys", keyed));
        observations.Add(Observation.Of("declared-max-active-regions", MaxActiveRegions));

        string refusal;

        await using (RunHandle keyedRun = await host.MaterializeAsync(keyed, cancellationToken))
        {
            try
            {
                await keyedRun.Completion;

                refusal = "the run completed, which means the bound was never reached";
            }
            catch (TrackedKeyOverflowException overflow)
            {
                refusal = overflow.Message;
            }
        }

        observations.Add(
            Observation.Of("regions-in-the-feed", orders.Select(order => order.Region).Distinct().Count()));
        observations.Add(Observation.Of("bounded-keys-refusal", refusal));

        return ScenarioOutcome.Of(graphs, observations);
    }
}
