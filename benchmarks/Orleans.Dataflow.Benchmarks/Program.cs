namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// The Orleans.Dataflow benchmark harness: bounded-memory, throughput, and recovery evidence for
/// GOAL.md's seventh definition-of-done point.
/// </summary>
/// <remarks>
/// <para>
/// <b>Bespoke and dependency-free, and that is a decision rather than an omission.</b> The claims this
/// harness exists to support are boundedness and recovery, and both of them need a controlled arrangement:
/// a stream far longer than any declared bound, a heap read at known positions in it, a silo killed
/// underneath a live durable run. A microbenchmark runner is built for the opposite job — many short
/// iterations of a small body, statistically resolved — and would answer none of these questions while
/// making every one of them harder to set up. The throughput numbers here come along for the ride and are
/// reported at the grade they are worth: orders of magnitude, not percentages. See
/// <see cref="GraphScenario.Grade"/>.
/// </para>
/// <para>
/// <b>What it does not do.</b> It does not measure anything over a network, anything on more than one
/// machine, anything with a real persistence provider, or the memory of a cluster run. Those boundaries
/// are stated once here and again in docs/BENCHMARKS.md, where the numbers are published.
/// </para>
/// <para>
/// <b>Failing loudly is the contract.</b> Every scenario that does not run to completion is reported and
/// the process exits non-zero. That is what makes <c>--smoke</c> worth a step in CI: it asserts nothing
/// about timing, and everything about the harness still working.
/// </para>
/// </remarks>
internal static class Program
{
    /// <summary>Runs the harness.</summary>
    /// <param name="arguments">The command line.</param>
    /// <returns>Zero when every selected scenario ran to completion, and one otherwise.</returns>
    internal static async Task<int> Main(string[] arguments)
    {
        BenchmarkOptions options;

        try
        {
            options = BenchmarkOptions.Parse(arguments);
        }
        catch (ArgumentException failure)
        {
            await Console.Error.WriteLineAsync(failure.Message);

            return 1;
        }

        if (options.Usage is not null)
        {
            Console.WriteLine(options.Usage);

            return 0;
        }

        using CancellationTokenSource budget = new(options.Timeout);

        List<GraphScenario> selected =
        [
            .. GraphScenario.All.Where(scenario =>
                options.Only is null ||
                scenario.Name.Contains(options.Only, StringComparison.OrdinalIgnoreCase)),
        ];

        bool recovering = options.Only is null ||
            "recovery".Contains(options.Only, StringComparison.OrdinalIgnoreCase);

        if (selected.Count == 0 && !recovering)
        {
            // The same trap as an unrecognized switch, one level in: a filter that matches nothing would
            // otherwise run nothing, report success and exit zero, which is indistinguishable from a clean
            // run to anything reading the exit code.
            await Console.Error.WriteLineAsync(
                $"No scenario's name contains '{options.Only}', so this run would measure nothing.");

            return 1;
        }

        Report.Provenance(options);

        List<GraphMeasurement> measurements = [];
        int failures = 0;

        foreach (GraphScenario scenario in selected)
        {
            try
            {
                measurements.Add(await GraphBenchmark.MeasureAsync(
                    scenario,
                    scenario.Elements(options.Elements),
                    options.Runs,
                    budget.Token));
            }
            catch (Exception failure) when (failure is not OutOfMemoryException)
            {
                failures++;

                Report.Failure(scenario.Name, failure);
            }
        }

        if (measurements.Count > 0)
        {
            Report.ThroughputHeader();

            foreach (GraphMeasurement measurement in measurements)
            {
                Report.Throughput(measurement);
            }

            Report.MemoryHeader();

            foreach (GraphMeasurement measurement in measurements)
            {
                Report.Memory(measurement);
            }
        }

        if (recovering)
        {
            failures += await RecoverAsync(options, budget.Token);
        }

        Report.Pointers();
        Report.Verdict(failures);

        return failures == 0 ? 0 : 1;
    }

    /// <summary>Deploys a cluster, measures the recovery scenario on it, and takes it down again.</summary>
    /// <param name="options">What the harness was asked to do.</param>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>Zero when the scenario ran to completion, and one otherwise.</returns>
    /// <remarks>
    /// The cluster is deployed here rather than beside the graph scenarios because it costs seconds and
    /// nothing else in the harness needs one — and because a graph measurement taken while three silos were
    /// running in the same process would be measuring the silos as much as the graph.
    /// </remarks>
    private static async Task<int> RecoverAsync(BenchmarkOptions options, CancellationToken cancellationToken)
    {
        await using BenchmarkCluster cluster = new();

        try
        {
            await cluster.DeployAsync();

            Report.Recovery(await RecoveryBenchmark.MeasureAsync(
                cluster,
                options.RecoveryElements,
                options.RecoveryEveryElements,
                options.RecoveryRepetitions,
                cancellationToken));

            return 0;
        }
        catch (Exception failure) when (failure is not OutOfMemoryException)
        {
            Report.Failure("durable-run-silo-kill", failure);

            return 1;
        }
    }
}
