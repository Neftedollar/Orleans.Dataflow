using System.Diagnostics;
using System.Globalization;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>What the recovery scenario measured out at.</summary>
/// <param name="Repetitions">How many kills the medians are over.</param>
/// <param name="Elements">How many elements each run delivered before its silo was killed.</param>
/// <param name="EveryElements">How many elements a run admitted between checkpoints.</param>
/// <param name="MedianLatencyMilliseconds">
/// The median wall clock from asking for the hosting silo's destruction to the resumed attempt's first
/// delivery, which includes this cluster's own teardown and is therefore an upper bound on it.
/// </param>
/// <param name="MedianReplayedElements">
/// The median number of elements the resumed attempt delivered a second time, which is the at-least-once
/// window the checkpoint cadence bought.
/// </param>
internal sealed record RecoveryMeasurement(
    int Repetitions,
    long Elements,
    int EveryElements,
    double MedianLatencyMilliseconds,
    long MedianReplayedElements);

/// <summary>
/// How long a durable run takes to start delivering again after the silo hosting it stops existing.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is timed.</b> From the moment the harness asks for the hosting silo's destruction to the moment
/// the recording sink is handed the resumed attempt's first element. The sink stamps that delivery itself,
/// so the harness's own polling interval never enters the number. Why the clock starts at the request
/// rather than at the kill call's return is in <see cref="BenchmarkCluster.KillHostOfAsync"/>, and it is
/// not a detail: measured the other way it produced a negative latency.
/// </para>
/// <para>
/// <b>What triggers the resume.</b> The client's ordinary completion poll and nothing else. The handle's
/// <c>Completion</c> is read straight after materialization, which is what a client that intends to wait
/// for a run does, so the loop is already running when the kill happens and no harness action stands
/// between the death and the recovery.
/// </para>
/// <para>
/// <b>Why the source parks.</b> A source that ends races the harness that wants to kill a silo underneath a
/// live run. This one emits its whole run, then waits on the stop token: the run is alive, its position is
/// committed, and the kill lands at a moment the harness chose rather than one it caught. The elements the
/// resumed attempt then delivers are precisely the window between the last checkpoint and the kill, which
/// is why the same measurement also reports the replay length — the cost of the cadence, in elements.
/// </para>
/// <para>
/// <b>The latency is bimodal, and a single number hides that.</b> The client's poll is what notices the
/// run is gone; a poll that was already airborne when its target's silo died is answered by nobody and
/// waits out the whole response timeout before the loop retries. So a recovery either takes tens of
/// milliseconds or about that timeout — measured over four runs of the same arrangement: 34, 40, 34, and
/// 5889 milliseconds. Nothing here is wrong when the large one appears; it is the cost of one unlucky
/// poll, and it is why <see cref="BenchmarkOptions.RecoveryRepetitions"/> defaults to five rather than
/// three, and why the median is the number reported.
/// </para>
/// <para>
/// <b>The element count is deliberately not a multiple of the checkpoint cadence.</b> If it were, the
/// stored cursor would sit at the last element and the resumed attempt would have nothing to deliver: it
/// would resume, find itself past the end, and park — and the measurement would wait forever for a first
/// delivery that is not owed. The cadence below leaves a window on purpose.
/// </para>
/// </remarks>
internal static class RecoveryBenchmark
{
    /// <summary>How long the harness waits for one step of the scenario before giving up on it.</summary>
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    /// <summary>How often the harness looks at a condition it is waiting for.</summary>
    /// <remarks>
    /// This does not enter the latency — the sink stamps its own deliveries — so it is set for cheapness
    /// rather than for resolution.
    /// </remarks>
    private static readonly TimeSpan Glance = TimeSpan.FromMilliseconds(2);

    /// <summary>Measures the recovery latency of a durable run over a cluster that loses its host.</summary>
    /// <param name="cluster">The deployed cluster.</param>
    /// <param name="elements">How many elements a run delivers before its silo is killed.</param>
    /// <param name="everyElements">How many elements a run admits between checkpoints.</param>
    /// <param name="repetitions">How many kills to measure.</param>
    /// <param name="cancellationToken">Cancels a scenario that has stopped making progress.</param>
    /// <returns>The measurement.</returns>
    internal static async Task<RecoveryMeasurement> MeasureAsync(
        BenchmarkCluster cluster,
        long elements,
        int everyElements,
        int repetitions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        List<double> latencies = [];
        List<long> replays = [];

        for (int repetition = 0; repetition < repetitions; repetition++)
        {
            (double latency, long replayed) =
                await MeasureOnceAsync(cluster, elements, everyElements, repetition, cancellationToken);

            latencies.Add(latency);
            replays.Add(replayed);

            await cluster.RestoreSilosAsync();
        }

        return new RecoveryMeasurement(
            repetitions,
            elements,
            everyElements,
            Statistics.Median(latencies),
            Statistics.Median(replays));
    }

    /// <summary>Measures one kill.</summary>
    /// <param name="cluster">The deployed cluster.</param>
    /// <param name="elements">How many elements the run delivers before its silo is killed.</param>
    /// <param name="everyElements">How many elements the run admits between checkpoints.</param>
    /// <param name="repetition">Which repetition this is, which is what keeps the identities apart.</param>
    /// <param name="cancellationToken">Cancels a scenario that has stopped making progress.</param>
    /// <returns>The latency in milliseconds and how many elements were replayed.</returns>
    private static async Task<(double Latency, long Replayed)> MeasureOnceAsync(
        BenchmarkCluster cluster,
        long elements,
        int everyElements,
        int repetition,
        CancellationToken cancellationToken)
    {
        string identity = string.Create(CultureInfo.InvariantCulture, $"benchmark-recovery-{repetition}");

        BenchmarkDeliveries.Clear(identity);

        PipelineDefinition pipeline = Pipeline(identity, elements, identity);

        OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = "measured", EveryElements = everyElements },
            cancellationToken);

        // Read once, here: this is the client's own poll loop, and it is what will notice the run is gone
        // and address it again. Starting it now rather than after the kill keeps the harness out of the
        // measured interval.
        Task completion = handle.Completion;

        await UntilAsync(
            () => BenchmarkDeliveries.Count(identity) >= elements,
            "the run delivered its whole sequence and parked",
            cancellationToken);

        long highest = BenchmarkDeliveries.Highest(identity);

        BenchmarkDeliveries.Arm(identity);

        long asked = await cluster.KillHostOfAsync(cluster.Run(handle));

        (long Timestamp, long Element) resumed = await ArmedAsync(identity, cancellationToken);

        double latency = Stopwatch.GetElapsedTime(asked, resumed.Timestamp).TotalMilliseconds;
        long replayed = highest - resumed.Element + 1;

        await handle.ShutdownAsync();

        // Awaited to a conclusion rather than abandoned. A run left draining while the next repetition
        // deploys over it would put the previous measurement's engine threads inside the next one, and a
        // fault raised after the timing is over is still a fault this harness should report rather than
        // leave unobserved. The shutdown releases the parked source, so this is a short wait.
        await completion.WaitAsync(Patience, cancellationToken);

        return (latency, replayed);
    }

    /// <summary>Builds the measured pipeline: a cursored source of numbers straight into a recording sink.</summary>
    /// <param name="id">The pipeline's identity, which is also its coordinator's key.</param>
    /// <param name="elements">How many numbers the source emits before it parks.</param>
    /// <param name="log">The ledger the sink writes its deliveries to.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// Deliberately plain. A source into a sink is one fused segment with no buffer in it, so an element is
    /// recorded before the run advances the cursor past it and the stored position and the ledger agree at
    /// every quiescent moment. A batch or a declared buffer in the middle would add a loss window of its
    /// own and blur the replay length this reports.
    /// </remarks>
    private static PipelineDefinition Pipeline(string id, long elements, string log)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(
                    BenchmarkVocabulary.Catalog(),
                    BenchmarkVocabulary.Range,
                    BenchmarkVocabulary.Number),
                "numbers",
                BenchmarkVocabulary.WriteRange(elements, park: true))
            .To(
                RegisteredStage.Sink(
                    BenchmarkVocabulary.Catalog(),
                    BenchmarkVocabulary.Record,
                    BenchmarkVocabulary.Number),
                "recorded",
                BenchmarkVocabulary.WriteRecord(log));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Waits for the delivery a ledger was armed for.</summary>
    /// <param name="log">Which ledger.</param>
    /// <param name="cancellationToken">Cancels a wait that has gone on too long.</param>
    /// <returns>The stamp of the first delivery after the arming.</returns>
    private static async Task<(long Timestamp, long Element)> ArmedAsync(
        string log,
        CancellationToken cancellationToken)
    {
        (long Timestamp, long Element)? armed = null;

        await UntilAsync(
            () => (armed = BenchmarkDeliveries.Armed(log)) is not null,
            "the resumed attempt delivered its first element",
            cancellationToken);

        return armed!.Value;
    }

    /// <summary>Waits for something to become true, or gives up loudly.</summary>
    /// <param name="condition">What is being waited for.</param>
    /// <param name="expectation">What it is, as a sentence a failure can be read as.</param>
    /// <param name="cancellationToken">Cancels a wait that has gone on too long.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="TimeoutException">The condition did not hold within the harness's patience.</exception>
    private static async Task UntilAsync(
        Func<bool> condition,
        string expectation,
        CancellationToken cancellationToken)
    {
        long started = Stopwatch.GetTimestamp();

        while (!condition())
        {
            if (Stopwatch.GetElapsedTime(started) > Patience)
            {
                throw new TimeoutException(
                    $"Waited {Patience.TotalSeconds:0} seconds and {expectation} never became true.");
            }

            await Task.Delay(Glance, cancellationToken);
        }
    }
}
