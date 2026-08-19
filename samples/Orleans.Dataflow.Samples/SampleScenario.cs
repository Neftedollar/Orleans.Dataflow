namespace Orleans.Dataflow.Samples;

/// <summary>
/// One scenario: what it teaches, and the two authorings that must agree about it.
/// </summary>
/// <remarks>
/// <para>
/// The list below is the application. Everything else in this project is either an authoring, the printing,
/// or the silo one scenario needs; the order here is the order a reader should meet the library in, and it
/// is the order a run without arguments goes through.
/// </para>
/// <para>
/// A scenario with no F# twin carries the reason instead, and the runner prints it. There are none today,
/// and the field exists so that a surface F# cannot reach yet would have to be admitted in the output rather
/// than quietly skipped.
/// </para>
/// </remarks>
internal sealed record SampleScenario
{
    /// <summary>Gets the name <c>--only</c> matches against.</summary>
    internal required string Name { get; init; }

    /// <summary>Gets the one line that says what a reader learns here.</summary>
    internal required string Teaches { get; init; }

    /// <summary>Gets the C# authoring.</summary>
    internal required Func<SampleRun, CancellationToken, Task<ScenarioOutcome>> InCSharp { get; init; }

    /// <summary>Gets the F# authoring, or null when there is none.</summary>
    internal Func<SampleRun, CancellationToken, Task<ScenarioOutcome>>? InFSharp { get; init; }

    /// <summary>Gets why there is no F# twin, when there is none.</summary>
    internal string? WithoutTwin { get; init; }

    /// <summary>Gets whether this scenario needs the runner to have started a silo.</summary>
    /// <remarks>
    /// Starting one costs seconds, so the runner starts it only when a selected scenario says it needs one.
    /// A reader running <c>--only first-pipeline</c> should not wait for a cluster nothing will use.
    /// </remarks>
    internal bool NeedsCluster { get; init; }

    /// <summary>Gets every scenario, in the order a run without arguments goes through them.</summary>
    internal static IReadOnlyList<SampleScenario> All { get; } =
    [
        new()
        {
            Name = "first-pipeline",
            Teaches = "A source, a filter, a map and a fold, run locally, with one typed result slot. This is the README's snippet.",
            InCSharp = CSharp.FirstPipeline.RunAsync,
            InFSharp = FSharp.FirstPipeline.RunAsync,
        },
        new()
        {
            Name = "backpressure",
            Teaches = "A declared bound is what bounds memory, and a declared overflow policy is what decides who is dropped.",
            InCSharp = CSharp.Backpressure.RunAsync,
            InFSharp = FSharp.Backpressure.RunAsync,
        },
        new()
        {
            Name = "async-work",
            Teaches = "Asynchronous mapping runs exactly as concurrently as the graph declared, ordered or in completion order.",
            InCSharp = CSharp.AsyncWork.RunAsync,
            InFSharp = FSharp.AsyncWork.RunAsync,
        },
        new()
        {
            Name = "junctions",
            Teaches = "One stream broadcast into two branches, each ending in a terminal with a result slot of its own.",
            InCSharp = CSharp.Junctions.RunAsync,
            InFSharp = FSharp.Junctions.RunAsync,
        },
        new()
        {
            Name = "windowing",
            Teaches = "Grouping bounded by a count and a window, and a keyed operator that refuses a key past its declared maximum.",
            InCSharp = CSharp.Windowing.RunAsync,
            InFSharp = FSharp.Windowing.RunAsync,
        },
        new()
        {
            Name = "failure",
            Teaches = "A stage that throws inside a supervision scope: retries with a declared ladder, and a declared fallback.",
            InCSharp = CSharp.Failure.RunAsync,
            InFSharp = FSharp.Failure.RunAsync,
        },
        new()
        {
            Name = "cluster",
            Teaches = "The same pipeline on a real in-process silo, through the ordinary hosting API and no test facility.",
            InCSharp = CSharp.Cluster.RunAsync,
            InFSharp = FSharp.Cluster.RunAsync,
            NeedsCluster = true,
        },
        new()
        {
            Name = "durable",
            Teaches = "A durable run that dies, a second host that continues it, and the replay window that costs.",
            InCSharp = CSharp.Durable.RunAsync,
            InFSharp = FSharp.Durable.RunAsync,
        },
    ];
}
