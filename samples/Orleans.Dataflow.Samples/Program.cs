using Orleans.Dataflow.Hosting;

namespace Orleans.Dataflow.Samples;

/// <summary>
/// The Orleans.Dataflow sample application: eight scenarios, each authored twice and compared.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every scenario is written twice on purpose.</b> The library's central claim is that C# and F# are
/// equal frontends over one graph algebra, producing byte-identical documents — so a sample that authored
/// each scenario once would demonstrate the operators and leave the interesting claim unexamined. Here both
/// authorings run in this process, and the runner prints their fingerprints side by side together with
/// whether they match. When any pair does not, the run fails.
/// </para>
/// <para>
/// <b>It is therefore self-verifying rather than decorative.</b> That is what makes the <c>--smoke</c> step
/// in CI worth having: it asserts nothing about timing and everything about the samples still being true.
/// </para>
/// <para>
/// <b>Public API only.</b> Nothing in either project reaches into the library's internals or into its
/// test-support package. Whatever a scenario needs and cannot reach is a finding about the public surface
/// rather than a licence to reach further, which is why this application implements its own checkpoint
/// store, its own stage factory, and its own silo registration.
/// </para>
/// </remarks>
internal static class Program
{
    /// <summary>Runs the samples.</summary>
    /// <param name="arguments">The command line.</param>
    /// <returns>Zero when every selected scenario ran and both authorings agreed, and one otherwise.</returns>
    internal static async Task<int> Main(string[] arguments)
    {
        SampleOptions options;

        try
        {
            options = SampleOptions.Parse(arguments);
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

        List<SampleScenario> selected =
        [
            .. SampleScenario.All.Where(scenario =>
                options.Only is null ||
                scenario.Name.Contains(options.Only, StringComparison.OrdinalIgnoreCase)),
        ];

        if (selected.Count == 0)
        {
            // The same trap as an unrecognized switch, one level in: a filter that matches nothing would
            // otherwise run nothing, report success and exit zero, which is indistinguishable from a clean
            // run to anything reading the exit code.
            await Console.Error.WriteLineAsync(
                $"No scenario's name contains '{options.Only}', so this run would demonstrate nothing.");

            return 1;
        }

        if (options.List)
        {
            Report.List(selected);

            return 0;
        }

        using CancellationTokenSource budget = new(options.Timeout);

        Report.Provenance(options, selected.Count);

        await using SampleCluster cluster = new();

        // Started once, and only when something selected asks for one. A reader running a single local
        // scenario should not wait several seconds for a silo nothing will use.
        if (selected.Exists(scenario => scenario.NeedsCluster))
        {
            await cluster.StartAsync(budget.Token);
        }

        SampleRun run = new(
            options.Scale,
            selected.Exists(scenario => scenario.NeedsCluster) ? cluster.Host : null,
            () => new SampleCheckpointStore());

        int failures = 0;
        int disagreements = 0;

        foreach (SampleScenario scenario in selected)
        {
            Report.ScenarioHeader(scenario);

            try
            {
                ScenarioOutcome inCSharp = await scenario.InCSharp(run, budget.Token);
                ScenarioOutcome? inFSharp = scenario.InFSharp is null
                    ? null
                    : await scenario.InFSharp(run, budget.Token);

                if (inFSharp is null)
                {
                    Report.NoTwin(scenario.WithoutTwin ?? "the reason was not recorded, which is itself a bug");
                }

                disagreements += Report.Fingerprints(inCSharp, inFSharp);
                disagreements += Report.Observations(inCSharp, inFSharp);
            }
            catch (Exception failure) when (failure is not OutOfMemoryException)
            {
                failures++;

                Report.Failure(scenario.Name, failure);
            }
        }

        Report.Verdict(selected.Count, failures, disagreements);

        return failures == 0 && disagreements == 0 ? 0 : 1;
    }
}
