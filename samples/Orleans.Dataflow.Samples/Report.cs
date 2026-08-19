using System.Globalization;
using System.Runtime.InteropServices;

namespace Orleans.Dataflow.Samples;

/// <summary>
/// What the sample application prints.
/// </summary>
/// <remarks>
/// <para>
/// Tab-separated, one section per kind of reading, with every line that is not data beginning with a hash —
/// the same shape the benchmark harness in this repository uses, for the same reasons: readable as it
/// scrolls past, pasteable into a document, and splittable by a script that wants one number. The columns
/// are the same in a smoke run and a full one, so two runs can be diffed.
/// </para>
/// <para>
/// Every number is labelled with what it is rather than with what produced it, because the reader this
/// output is written for has not read the library. "orders-the-filter-kept" is a sentence; "count" is a
/// variable name.
/// </para>
/// </remarks>
internal static class Report
{
    /// <summary>Writes the block that heads every run.</summary>
    /// <param name="options">What the application was asked to do.</param>
    /// <param name="scenarios">How many scenarios this run will go through.</param>
    internal static void Provenance(SampleOptions options, int scenarios)
    {
        ArgumentNullException.ThrowIfNull(options);

        Comment("Orleans.Dataflow samples");
        Comment($"utc: {DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)}");
        Comment($"os: {RuntimeInformation.OSDescription} ({RuntimeInformation.OSArchitecture})");
        Comment($"runtime: {RuntimeInformation.FrameworkDescription}");
        Comment($"build: {Configuration}");
        Comment($"mode: {(options.Smoke ? "smoke" : "full")}");
        Comment($"scenarios: {Number(scenarios)}");
        Comment(string.Empty);
        Comment("Every scenario below is authored twice: once in C#, in samples/Orleans.Dataflow.Samples,");
        Comment("and once in F#, in samples/Orleans.Dataflow.Samples.FSharp. Both authorings run in this");
        Comment("process. The two must build documents with the same fingerprint — the sha256 of the");
        Comment("canonical graph document — and their runs must report the same observations. A run in");
        Comment("which any pair disagrees exits non-zero, so this output is a check and not a brochure.");
    }

    /// <summary>Names every scenario without running any of them.</summary>
    /// <param name="scenarios">The scenarios.</param>
    internal static void List(IReadOnlyList<SampleScenario> scenarios)
    {
        ArgumentNullException.ThrowIfNull(scenarios);

        Section("scenarios");
        Row("name", "teaches");

        foreach (SampleScenario scenario in scenarios)
        {
            Row(scenario.Name, scenario.Teaches);
        }
    }

    /// <summary>Writes the header of one scenario.</summary>
    /// <param name="scenario">The scenario.</param>
    internal static void ScenarioHeader(SampleScenario scenario)
    {
        ArgumentNullException.ThrowIfNull(scenario);

        Section("scenario");
        Row("name", scenario.Name);
        Row("teaches", scenario.Teaches);
    }

    /// <summary>Writes the fingerprint of every graph both authorings built, and whether they match.</summary>
    /// <param name="inCSharp">What the C# authoring produced.</param>
    /// <param name="inFSharp">What the F# authoring produced, or null when there is no twin.</param>
    /// <returns>How many graphs the two authorings disagreed about.</returns>
    internal static int Fingerprints(ScenarioOutcome inCSharp, ScenarioOutcome? inFSharp)
    {
        ArgumentNullException.ThrowIfNull(inCSharp);

        Section("fingerprints");
        Row("graph", "identical", "csharp", "fsharp");

        IReadOnlyList<GraphReading> left = inCSharp.Graphs;
        IReadOnlyList<GraphReading> right = inFSharp?.Graphs ?? [];
        int disagreements = 0;

        for (int index = 0; index < Math.Max(left.Count, right.Count); index++)
        {
            GraphReading? one = index < left.Count ? left[index] : null;
            GraphReading? other = index < right.Count ? right[index] : null;

            bool identical = inFSharp is not null &&
                one is not null &&
                other is not null &&
                string.Equals(one.Name, other.Name, StringComparison.Ordinal) &&
                one.Fingerprint == other.Fingerprint;

            if (inFSharp is not null && !identical)
            {
                disagreements++;
            }

            Row(
                one?.Name ?? other?.Name ?? "(unnamed)",
                inFSharp is null ? "no-twin" : Yes(identical),
                one is null ? Missing : one.Fingerprint.ToString(),
                other is null ? Missing : other.Fingerprint.ToString());
        }

        return disagreements;
    }

    /// <summary>Writes what both runs produced, and whether they agree.</summary>
    /// <param name="inCSharp">What the C# authoring produced.</param>
    /// <param name="inFSharp">What the F# authoring produced, or null when there is no twin.</param>
    /// <returns>How many observations the two authorings disagreed about.</returns>
    internal static int Observations(ScenarioOutcome inCSharp, ScenarioOutcome? inFSharp)
    {
        ArgumentNullException.ThrowIfNull(inCSharp);

        Section("observations");
        Row("name", "agree", "csharp", "fsharp");

        IReadOnlyList<Observation> left = inCSharp.Observations;
        IReadOnlyList<Observation> right = inFSharp?.Observations ?? [];
        int disagreements = 0;

        for (int index = 0; index < Math.Max(left.Count, right.Count); index++)
        {
            Observation? one = index < left.Count ? left[index] : null;
            Observation? other = index < right.Count ? right[index] : null;

            bool agree = inFSharp is not null &&
                one is not null &&
                other is not null &&
                string.Equals(one.Name, other.Name, StringComparison.Ordinal) &&
                string.Equals(one.Value, other.Value, StringComparison.Ordinal);

            if (inFSharp is not null && !agree)
            {
                disagreements++;
            }

            Row(
                one?.Name ?? other?.Name ?? "(unnamed)",
                inFSharp is null ? "no-twin" : Yes(agree),
                one is null ? Missing : Flatten(one.Value),
                other is null ? Missing : Flatten(other.Value));
        }

        return disagreements;
    }

    /// <summary>Writes why a scenario has no F# twin.</summary>
    /// <param name="reason">The reason.</param>
    internal static void NoTwin(string reason)
    {
        Comment($"no F# twin: {Flatten(reason)}");
    }

    /// <summary>Writes one scenario's failure, and keeps going.</summary>
    /// <param name="scenario">What was being run.</param>
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
    /// <param name="scenarios">How many scenarios were selected.</param>
    /// <param name="failures">How many of them did not run to completion.</param>
    /// <param name="disagreements">How many readings the two authorings did not agree about.</param>
    internal static void Verdict(int scenarios, int failures, int disagreements)
    {
        Section("result");
        Row("status", failures == 0 && disagreements == 0 ? "ok" : "failed");
        Row("scenarios_run", Number(scenarios));
        Row("failed_scenarios", Number(failures));
        Row("disagreements", Number(disagreements));
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

    /// <summary>Writes one commentary line.</summary>
    /// <param name="text">The line.</param>
    private static void Comment(string text) =>
        Console.WriteLine(text.Length == 0 ? "#" : $"# {text}");

    /// <summary>Renders a count.</summary>
    /// <param name="value">The count.</param>
    /// <returns>The text.</returns>
    private static string Number(int value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>Renders an agreement.</summary>
    /// <param name="agree">Whether the two authorings agreed.</param>
    /// <returns>The text, shouted when they did not.</returns>
    private static string Yes(bool agree) => agree ? "yes" : "NO";

    /// <summary>What is printed where one side has nothing to say.</summary>
    private const string Missing = "(none)";

    /// <summary>Puts a value on one line, so that a row stays a row.</summary>
    /// <param name="text">The value.</param>
    /// <returns>The value with its line breaks and tabs turned into spaces.</returns>
    private static string Flatten(string text) => text.ReplaceLineEndings(" ").Replace('\t', ' ');

    /// <summary>What configuration this application was built in.</summary>
    private const string Configuration =
#if DEBUG
        "Debug";
#else
        "Release";
#endif
}
