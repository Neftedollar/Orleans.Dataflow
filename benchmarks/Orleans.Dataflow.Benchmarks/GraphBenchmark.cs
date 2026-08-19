using System.Diagnostics;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>What one scenario measured out at.</summary>
/// <param name="Scenario">The shape.</param>
/// <param name="Elements">How many elements each run carried.</param>
/// <param name="Runs">How many timed runs and how many weighed runs the medians are over.</param>
/// <param name="MedianMilliseconds">The median wall clock of a run, materialization included.</param>
/// <param name="AllocatedBytes">The median total allocation of a run.</param>
/// <param name="PeakLiveBytes">The median peak live heap of a run, above its own baseline.</param>
internal sealed record GraphMeasurement(
    GraphScenario Scenario,
    long Elements,
    int Runs,
    double MedianMilliseconds,
    long AllocatedBytes,
    long PeakLiveBytes)
{
    /// <summary>Gets how many elements a second the median run carried.</summary>
    internal double ElementsPerSecond => MedianMilliseconds <= 0 ? 0 : Elements * 1000.0 / MedianMilliseconds;

    /// <summary>Gets what one element cost in allocation.</summary>
    internal double AllocatedBytesPerElement => Elements == 0 ? 0 : (double)AllocatedBytes / Elements;
}

/// <summary>
/// Runs the measured graphs, in two passes that deliberately do not overlap.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two passes, because the instruments interfere.</b> The memory pass stops the world at every sample;
/// a run timed with that in it would report the collector's cost as the graph's. The timing pass therefore
/// runs with the probe disarmed and reads only counters that cost nothing to read — the wall clock and
/// the allocation total, which is a counter the runtime maintains anyway. Nothing is inferred across the
/// two: the peak comes from the weighed runs, the throughput from the timed ones.
/// </para>
/// <para>
/// <b>The graph is built once and materialized per run.</b> Building is authoring — it produces a document
/// and a fingerprint — and is not what a deployment repeats. Materializing is, so it is inside the timed
/// region: what a run costs includes compiling the document into an execution plan, and a harness that
/// hoisted that out would report a number no caller can obtain.
/// </para>
/// <para>
/// <b>One warmup run is discarded</b> before either pass. The first run through a graph pays for JIT of
/// every stage delegate, and reporting that as the median would say more about the tiered compiler than
/// about the runtime.
/// </para>
/// <para>
/// <b>Each weighed run settles the process before it starts.</b> See <see cref="HeapProbe"/>: a run that
/// has completed still holds its last accumulator through the thread pool until those threads are given
/// something else to do, and a baseline taken before that happens belongs to the previous run rather than
/// to this one.
/// </para>
/// </remarks>
internal static class GraphBenchmark
{
    /// <summary>Measures one scenario.</summary>
    /// <param name="scenario">The shape to measure.</param>
    /// <param name="elements">How many elements each run carries.</param>
    /// <param name="runs">How many runs each pass performs after the warmup.</param>
    /// <param name="cancellationToken">Cancels a run that has stopped making progress.</param>
    /// <returns>The measurement.</returns>
    internal static async Task<GraphMeasurement> MeasureAsync(
        GraphScenario scenario,
        long elements,
        int runs,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(scenario);

        LocalDataflowHost host = new();
        HeapProbe probe = new();
        RunnableGraph graph = scenario.Build(elements, probe);

        await probe.ArmAsync(elements, sampling: false);

        await RunAsync(host, graph, cancellationToken);

        List<double> milliseconds = [];
        List<long> allocated = [];

        for (int run = 0; run < runs; run++)
        {
            await probe.ArmAsync(elements, sampling: false);

            long before = GC.GetTotalAllocatedBytes(precise: true);
            long started = Stopwatch.GetTimestamp();

            await RunAsync(host, graph, cancellationToken);

            milliseconds.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            allocated.Add(GC.GetTotalAllocatedBytes(precise: true) - before);
        }

        List<long> peaks = [];

        for (int run = 0; run < runs; run++)
        {
            await probe.ArmAsync(elements, sampling: true);

            await RunAsync(host, graph, cancellationToken);

            peaks.Add(probe.PeakBytes);
        }

        return new GraphMeasurement(
            scenario,
            elements,
            runs,
            Statistics.Median(milliseconds),
            Statistics.Median(allocated),
            Statistics.Median(peaks));
    }

    /// <summary>Materializes a graph and waits for it to finish.</summary>
    /// <param name="host">The host.</param>
    /// <param name="graph">The graph.</param>
    /// <param name="cancellationToken">Cancels a run that has stopped making progress.</param>
    /// <returns>A task that completes when the run has ended and its resources are released.</returns>
    private static async Task RunAsync(
        LocalDataflowHost host,
        RunnableGraph graph,
        CancellationToken cancellationToken)
    {
        await using RunHandle run = await host.MaterializeAsync(graph, cancellationToken);

        await run.Completion.WaitAsync(cancellationToken);
    }
}
