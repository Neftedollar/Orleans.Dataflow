namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The nine programs ADR 0006 was decided on, written against the surface it decided.
/// </summary>
/// <remarks>
/// <para>
/// These are the compile prototypes, moved here as the ADR promised. Their value is not that they run —
/// though they do, in <c>FluentJunctionTests</c> — but that they compile: every one of them was written to
/// need no explicit type argument anywhere an author would resent one, and that claim is a build break the
/// moment inference regresses. Nothing here may be "simplified" by adding a type argument or a lambda
/// annotation; that would delete the evidence.
/// </para>
/// <para>
/// Each program hands back the graph together with the slots it declared, so that one set of programs serves
/// both the authoring tests and the execution tests. What the elements are is deliberately small and
/// arithmetic, because the assertions are about junctions rather than about the payloads.
/// </para>
/// </remarks>
internal static class JunctionPrograms
{
    /// <summary>Gets the unit prices the zip program joins.</summary>
    /// <remarks>
    /// Hoisted out of the call the prototype wrote inline, and for a reason that has nothing to do with the
    /// surface: a constant array written as an argument is what CA1861 asks to be a field. The junction call
    /// itself is verbatim, which is the part the ADR proved.
    /// </remarks>
    private static decimal[] Prices { get; } = [10m, 20m];

    /// <summary>Gets the quantities the zip program joins against <see cref="Prices"/>.</summary>
    private static int[] Quantities { get; } = [3, 4];

    /// <summary>Broadcast to two sinks with two named slots — the simplest fan-out.</summary>
    /// <returns>The graph, the count of orders, and their total.</returns>
    internal static (RunnableGraph Graph, ResultSlot<long> Counted, ResultSlot<decimal> Totaled) BroadcastTwoSinks()
    {
        Source<Order> orders = Source.From(new[] { new Order("a", 10m), new Order("b", 20m) });

        RunnableGraph graph = orders.BroadcastTo(
            Flow.For<Order>().To(s => s.Count(), "counted", out ResultSlot<long> counted),
            Flow.For<Order>().To(s => s.Aggregate(0m, (sum, o) => sum + o.Amount), "totaled", out ResultSlot<decimal> totaled));

        return (graph, counted, totaled);
    }

    /// <summary>A tap: audit the main line without disturbing it.</summary>
    /// <returns>The graph and the count of orders the main line kept.</returns>
    internal static (RunnableGraph Graph, ResultSlot<long> Kept) TapForAudit()
    {
        Source<Order> orders = Source.From(new[] { new Order("a", 10m) });
        Flow<Order, Audit> toAudit = Flow.For<Order>().Select(o => new Audit($"seen {o.Id}"));

        RunnableGraph graph = orders
            .AlsoTo(toAudit.To(s => s.Ignore()))
            .Where(o => o.Amount > 5m)
            .To(s => s.Count(), "kept", out ResultSlot<long> kept);

        return (graph, kept);
    }

    /// <summary>Balance work across two identical processing branches.</summary>
    /// <returns>The graph, which declares no result.</returns>
    internal static RunnableGraph BalanceWorkers()
    {
        Source<int> jobs = Source.From(Enumerable.Range(0, 100));
        Flow<int, int> work = Flow.For<int>().Select(n => n * n);

        return jobs.BalanceTo(
            work.To(Sink.Ignore<int>()),
            work.To(Sink.Ignore<int>()));
    }

    /// <summary>Partition by predicate index; each class gets its own sink and slot.</summary>
    /// <returns>The graph, the count of small orders, and the count of large ones.</returns>
    internal static (RunnableGraph Graph, ResultSlot<long> Small, ResultSlot<long> Large) PartitionBySize()
    {
        Source<Order> orders = Source.From(new[] { new Order("a", 10m), new Order("b", 2000m) });

        RunnableGraph graph = orders.PartitionTo(
            o => o.Amount >= 1000m ? 1 : 0,
            Flow.For<Order>().To(s => s.Count(), "small", out ResultSlot<long> small),
            Flow.For<Order>().To(s => s.Count(), "large", out ResultSlot<long> large));

        return (graph, small, large);
    }

    /// <summary>Merge two sources; concat a third behind them.</summary>
    /// <returns>The graph and the count of everything that arrived.</returns>
    internal static (RunnableGraph Graph, ResultSlot<long> All) MergeAndConcat()
    {
        Source<int> fast = Source.From(Enumerable.Range(0, 10));
        Source<int> slow = Source.From(Enumerable.Range(100, 10));
        Source<int> tail = Source.From(new[] { -1 });

        return (fast.Merge(slow)
            .Concat(tail)
            .To(s => s.Count(), "all", out ResultSlot<long> all), all);
    }

    /// <summary>Zip prices with quantities into line totals.</summary>
    /// <returns>The graph and the sum of the line totals.</returns>
    internal static (RunnableGraph Graph, ResultSlot<decimal> Total) ZipPricesAndQuantities()
    {
        Source<decimal> prices = Source.From(Prices);
        Source<int> quantities = Source.From(Quantities);

        return (prices
            .Zip(quantities, (price, quantity) => price * quantity)
            .To(s => s.Aggregate(0m, (sum, line) => sum + line), "total", out ResultSlot<decimal> total), total);
    }

    /// <summary>The diamond: one stream, two derived views, rejoined positionally.</summary>
    /// <returns>The graph and the count of rejoined rows.</returns>
    internal static (RunnableGraph Graph, ResultSlot<long> Rows) DiamondForkZip()
    {
        Source<Order> orders = Source.From(new[] { new Order("a", 10m) });
        Flow<Order, string> ids = Flow.For<Order>().Select(o => o.Id);
        Flow<Order, decimal> amounts = Flow.For<Order>().Select(o => o.Amount);

        return (orders
            .Fork(ids, amounts)
            .Zip((id, amount) => $"{id}:{amount}")
            .To(s => s.Count(), "rows", out ResultSlot<long> rows), rows);
    }

    /// <summary>Unzip a pair stream to two differently-typed sinks.</summary>
    /// <returns>The graph, the count of names, and the count of ages.</returns>
    internal static (RunnableGraph Graph, ResultSlot<long> Names, ResultSlot<long> Ages) UnzipPairs()
    {
        Source<(string Name, int Age)> people = Source.From(new[] { ("ada", 36), ("alan", 41) });

        RunnableGraph graph = people.UnzipTo(
            Flow.For<string>().To(s => s.Count(), "names", out ResultSlot<long> names),
            Flow.For<int>().To(s => s.Count(), "ages", out ResultSlot<long> ages));

        return (graph, names, ages);
    }

    /// <summary>The merge diamond: race one stream down two paths, take results as they come.</summary>
    /// <returns>The graph and everything both paths produced.</returns>
    internal static (RunnableGraph Graph, ResultSlot<IReadOnlyList<string>> Seen) FastPathSlowPath()
    {
        Source<Order> orders = Source.From(new[] { new Order("a", 10m) });
        Flow<Order, string> fast = Flow.For<Order>().Select(o => $"cache:{o.Id}");
        Flow<Order, string> slow = Flow.For<Order>().SelectAsync(new ParallelismOptions { MaxConcurrency = 1 }, async (o, ct) =>
        {
            await Task.Delay(10, ct);
            return $"fetch:{o.Id}";
        });

        return (orders
            .ForkMerge(fast, slow)
            .To(s => s.Collect(new CollectOptions { MaxElements = 16 }), "seen", out ResultSlot<IReadOnlyList<string>> seen), seen);
    }

    /// <summary>An order, as the prototypes wrote one.</summary>
    /// <param name="Id">The order identity.</param>
    /// <param name="Amount">What it is worth.</param>
    internal sealed record class Order(string Id, decimal Amount);

    /// <summary>One line of the audit trail a tap writes.</summary>
    /// <param name="Line">The rendered line.</param>
    internal sealed record class Audit(string Line);
}
