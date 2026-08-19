using System.Globalization;
using System.Runtime;
using System.Runtime.InteropServices;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// What the harness prints.
/// </summary>
/// <remarks>
/// <para>
/// Tab-separated, one section per measurement kind, with every line that is not data beginning with a hash.
/// The format is chosen so that the output is readable as it scrolls past and pasteable into a document
/// without a conversion step, and so that a script that wants a number can find it with a field split
/// rather than a parser. Nothing about it is negotiable per run: the columns are the same in a smoke run
/// and a full one, so two runs can be diffed.
/// </para>
/// <para>
/// The provenance block is part of the result and not decoration. A throughput number without the machine,
/// the runtime, the collector and the build configuration beside it is not a measurement, and a document
/// that quoted one would be quoting a rumour.
/// </para>
/// </remarks>
internal static class Report
{
    /// <summary>Writes the provenance block that heads every report.</summary>
    /// <param name="options">What the harness was asked to do.</param>
    internal static void Provenance(BenchmarkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        Comment("Orleans.Dataflow benchmarks");
        Comment($"grade: {GraphScenario.Grade}");
        Comment($"utc: {DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)}");
        Comment($"os: {RuntimeInformation.OSDescription} ({RuntimeInformation.OSArchitecture})");
        Comment($"cpu: {Environment.ProcessorCount} logical processors");
        Comment($"runtime: {RuntimeInformation.FrameworkDescription}");
        Comment($"gc: {(GCSettings.IsServerGC ? "server" : "workstation")}, {(Concurrent ? "concurrent" : "non-concurrent")}, latency {GCSettings.LatencyMode}");
        Comment($"build: {Configuration}");
        Comment($"mode: {(options.Smoke ? "smoke" : "full")}");
        Comment($"elements: {options.Elements.ToString(CultureInfo.InvariantCulture)}, runs: {options.Runs.ToString(CultureInfo.InvariantCulture)}");
        Comment($"recovery: {options.RecoveryElements.ToString(CultureInfo.InvariantCulture)} elements, every {options.RecoveryEveryElements.ToString(CultureInfo.InvariantCulture)}, {options.RecoveryRepetitions.ToString(CultureInfo.InvariantCulture)} repetitions");
    }

    /// <summary>Writes the header of the throughput section.</summary>
    internal static void ThroughputHeader()
    {
        Section("throughput");
        Row("scenario", "elements", "runs", "median_ms", "elements_per_second", "allocated_bytes_per_element");
    }

    /// <summary>Writes one throughput row.</summary>
    /// <param name="measurement">The measurement.</param>
    internal static void Throughput(GraphMeasurement measurement)
    {
        ArgumentNullException.ThrowIfNull(measurement);

        Row(
            measurement.Scenario.Name,
            Number(measurement.Elements),
            Number(measurement.Runs),
            Decimal(measurement.MedianMilliseconds),
            Decimal(measurement.ElementsPerSecond),
            Decimal(measurement.AllocatedBytesPerElement));
    }

    /// <summary>Writes the header of the memory section.</summary>
    internal static void MemoryHeader()
    {
        Section("memory");
        Row("scenario", "elements", "runs", "peak_live_heap_bytes", "peak_live_heap_bytes_per_element", "bound");
    }

    /// <summary>Writes one memory row.</summary>
    /// <param name="measurement">The measurement.</param>
    internal static void Memory(GraphMeasurement measurement)
    {
        ArgumentNullException.ThrowIfNull(measurement);

        Row(
            measurement.Scenario.Name,
            Number(measurement.Elements),
            Number(measurement.Runs),
            Number(measurement.PeakLiveBytes),
            Decimal((double)measurement.PeakLiveBytes / measurement.Elements),
            measurement.Scenario.Bound);
    }

    /// <summary>Writes the recovery section.</summary>
    /// <param name="measurement">The measurement.</param>
    internal static void Recovery(RecoveryMeasurement measurement)
    {
        ArgumentNullException.ThrowIfNull(measurement);

        Section("recovery");
        Row("scenario", "elements", "every_elements", "kills", "median_latency_ms", "median_replayed_elements");
        Row(
            "durable-run-silo-kill",
            Number(measurement.Elements),
            Number(measurement.EveryElements),
            Number(measurement.Repetitions),
            Decimal(measurement.MedianLatencyMilliseconds),
            Number(measurement.MedianReplayedElements));
    }

    /// <summary>Writes the pointer section: evidence this harness deliberately does not re-measure.</summary>
    internal static void Pointers()
    {
        Section("pointers");
        Row("claim", "evidence");
        Row(
            "rolling upgrade",
            "tests/Orleans.Dataflow.OrleansTests/Cluster/RollingUpgradeTests.cs (M5.4), not re-measured here");
        Row(
            "bounded memory as a contract",
            "tests/Orleans.Dataflow.Tests/Runtime/BoundedMemoryTests.cs, which asserts on every build what this harness only prints");
    }

    /// <summary>Writes one scenario's failure, and keeps going.</summary>
    /// <param name="scenario">What was being measured.</param>
    /// <param name="failure">Why it stopped.</param>
    internal static void Failure(string scenario, Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);

        Section("failure");
        Row("scenario", scenario);
        Row("exception", failure.GetType().FullName ?? failure.GetType().Name);
        Row("message", Flatten(failure.Message));
    }

    /// <summary>Writes the verdict every run ends with.</summary>
    /// <param name="failures">How many scenarios failed to complete.</param>
    internal static void Verdict(int failures)
    {
        Section("result");
        Row("status", failures == 0 ? "ok" : "failed");
        Row("failed_scenarios", Number(failures));
    }

    /// <summary>Writes a section marker.</summary>
    /// <param name="name">What the section holds.</param>
    private static void Section(string name)
    {
        Console.WriteLine();
        Console.WriteLine(name);
    }

    /// <summary>Writes one tab-separated row.</summary>
    /// <param name="fields">The fields.</param>
    private static void Row(params string[] fields) => Console.WriteLine(string.Join('\t', fields));

    /// <summary>Writes one provenance line.</summary>
    /// <param name="text">The line.</param>
    private static void Comment(string text) => Console.WriteLine($"# {text}");

    /// <summary>Renders a count.</summary>
    /// <param name="value">The count.</param>
    /// <returns>The text.</returns>
    private static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>Renders a measurement.</summary>
    /// <param name="value">The measurement.</param>
    /// <returns>The text, to three significant decimals.</returns>
    private static string Decimal(double value) => value.ToString("0.###", CultureInfo.InvariantCulture);

    /// <summary>Puts a message on one line, so that a row stays a row.</summary>
    /// <param name="text">The message.</param>
    /// <returns>The message with its line breaks and tabs turned into spaces.</returns>
    private static string Flatten(string text) =>
        text.ReplaceLineEndings(" ").Replace('\t', ' ');

    /// <summary>Gets whether background collection is on.</summary>
    /// <remarks>
    /// Read from the runtime configuration rather than from <see cref="GCSettings"/>, which publishes the
    /// server/workstation choice and not this one. The switch is what the project file sets, so a build
    /// that stopped setting it would be visible in the report rather than silently changing the numbers.
    /// </remarks>
    private static bool Concurrent =>
        !AppContext.TryGetSwitch("System.GC.Concurrent", out bool concurrent) || concurrent;

    /// <summary>What configuration this harness was built in.</summary>
    /// <remarks>
    /// Read from the compiler rather than from a setting, because a number produced by a debug build is
    /// worth reporting only if the report says so.
    /// </remarks>
    private const string Configuration =
#if DEBUG
        "Debug (numbers from a debug build are not comparable with anything)";
#else
        "Release";
#endif
}
