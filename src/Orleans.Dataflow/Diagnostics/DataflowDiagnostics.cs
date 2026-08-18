using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Diagnostics;

/// <summary>
/// The one place this library talks to OpenTelemetry: a meter, an activity source, and the run registry
/// the observable instruments read.
/// </summary>
/// <remarks>
/// <para>
/// Everything is published under the name <c>Orleans.Dataflow</c> — one meter and one activity source —
/// so a deployment opts in with two lines: <c>AddMeter("Orleans.Dataflow")</c> and
/// <c>AddSource("Orleans.Dataflow")</c>. The class is internal because the names are the contract, not the
/// types: a subscriber names the meter, never this class.
/// </para>
/// <para>
/// <b>The counters are the run's own counters, read rather than duplicated.</b> The cumulative instruments
/// are observable: on each collection they sum every live run's counters with the totals runs left behind
/// when they settled, under one gate so a run is counted exactly once whether it is still live or already
/// folded. That is what keeps the readings monotonic — the property a counter must have — and what keeps
/// the emission entirely off the element hot path: a stage pays nothing for metrics nobody is collecting,
/// and the same nothing when they are, because the collector reads state the run already keeps. The only
/// eager emissions are one event per run start, one per run end, and one histogram sample per checkpoint
/// hold — all cold paths.
/// </para>
/// <para>
/// <b>Tags are bounded.</b> Every instrument carries <c>dataflow.graph</c> — the document fingerprint,
/// whose cardinality is the number of distinct graph shapes a deployment runs. A run's identity is
/// deliberately not a metric tag, because run ids are unbounded; it appears on activities, where
/// per-occurrence identity is the point.
/// </para>
/// <para>
/// <b>Telemetry never fails a run.</b> Every method here swallows everything: a listener that throws from
/// a measurement callback is a broken observer, and a run that died of being observed would be a worse
/// defect than any lost sample.
/// </para>
/// </remarks>
internal static class DataflowDiagnostics
{
    /// <summary>The name of both the meter and the activity source.</summary>
    internal const string SourceName = "Orleans.Dataflow";

    /// <summary>The tag carrying the graph document fingerprint. Bounded by the deployment's graph shapes.</summary>
    internal const string GraphTag = "dataflow.graph";

    /// <summary>The tag carrying how a run ended: <c>completed</c>, <c>failed</c>, or <c>canceled</c>.</summary>
    internal const string OutcomeTag = "dataflow.run.outcome";

    /// <summary>The tag carrying whether a start continued a stored position rather than beginning fresh.</summary>
    internal const string ResumedTag = "dataflow.run.resumed";

    /// <summary>The activity tag carrying a run's identity. Activities only; never a metric tag.</summary>
    internal const string RunIdTag = "dataflow.run.id";

    /// <summary>The activity tag carrying whether a materialization declared durability.</summary>
    internal const string DurableTag = "dataflow.run.durable";

    /// <summary>The activity source both hosts start spans from.</summary>
    internal static readonly ActivitySource Source = new(SourceName);

    private static readonly Meter Meter = new(SourceName);

    private static readonly Counter<long> Started = Meter.CreateCounter<long>(
        "orleans.dataflow.runs.started",
        unit: "{run}",
        description: "Run attempts started, fresh and resumed alike; the dataflow.run.resumed tag tells them apart.");

    private static readonly Counter<long> Ended = Meter.CreateCounter<long>(
        "orleans.dataflow.runs.ended",
        unit: "{run}",
        description: "Run attempts that reached a terminal state; the dataflow.run.outcome tag says which one.");

    private static readonly Histogram<double> Held = Meter.CreateHistogram<double>(
        "orleans.dataflow.checkpoint.hold.duration",
        unit: "s",
        description: "How long each checkpoint held its run quiescent, including holds whose write failed or was skipped.");

    /// <summary>The gate over <see cref="Live"/> and <see cref="Settled"/>.</summary>
    /// <remarks>
    /// One lock rather than concurrent structures, because both sides are cold — a run enters and leaves
    /// once, a collector reads once per export interval — and because the lock is what makes the readings
    /// monotonic: a run mid-handoff from live to settled is counted on exactly one side of it.
    /// </remarks>
    private static readonly object Gate = new();

    private static readonly HashSet<LocalRun> Live = new(ReferenceEqualityComparer.Instance);

    private static readonly Dictionary<string, SettledTotals> Settled = new(StringComparer.Ordinal);

    static DataflowDiagnostics()
    {
        _ = Meter.CreateObservableCounter(
            "orleans.dataflow.elements.dropped",
            static () => Observe(static run => run.DroppedElements, static totals => totals.Dropped),
            unit: "{element}",
            description: "Elements discarded by declared overflow policies, per graph, across every run of it this process hosted.");

        _ = Meter.CreateObservableCounter(
            "orleans.dataflow.failures.supervised",
            static () => Observe(static run => run.SupervisedFailures, static totals => totals.Supervised),
            unit: "{failure}",
            description: "Failures supervision scopes intercepted, one per failed attempt, per graph.");

        _ = Meter.CreateObservableCounter(
            "orleans.dataflow.elements.poison",
            static () => Observe(static run => run.PoisonElements, static totals => totals.Poisoned),
            unit: "{element}",
            description: "Elements that exhausted every retry attempt a scope declared, per graph.");

        _ = Meter.CreateObservableCounter(
            "orleans.dataflow.checkpoints.written",
            static () => Observe(static run => run.Checkpoints, static totals => totals.Checkpoints),
            unit: "{checkpoint}",
            description: "Checkpoint documents accepted by the store, per graph.");
    }

    /// <summary>Records that a run started, and starts the span that will cover its whole life.</summary>
    /// <param name="run">The run.</param>
    /// <param name="resumed">Whether the run continued a stored position.</param>
    /// <returns>The run's activity, or <see langword="null"/> when nothing is listening.</returns>
    /// <remarks>
    /// The activity is started with whatever ambient parent the materializing caller had — a client's
    /// materialize span, a grain call's span — and it is the caller's job to put its own
    /// <see cref="Activity.Current"/> back afterwards, because this span outlives the call that started it.
    /// </remarks>
    internal static Activity? RunStarted(LocalRun run, bool resumed)
    {
        try
        {
            lock (Gate)
            {
                _ = Live.Add(run);
            }

            string graph = run.Graph.ToString();

            Started.Add(
                1,
                new KeyValuePair<string, object?>(GraphTag, graph),
                new KeyValuePair<string, object?>(ResumedTag, resumed));

            Activity? activity = Source.StartActivity("dataflow.run");

            _ = activity?.SetTag(GraphTag, graph);
            _ = activity?.SetTag(ResumedTag, resumed);

            return activity;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Records that a run reached a terminal state, and ends its span.</summary>
    /// <param name="run">The run, whose counters are final.</param>
    /// <param name="activity">The span <see cref="RunStarted"/> opened, if anything was listening.</param>
    /// <param name="failure">The exception the run failed with, or <see langword="null"/>.</param>
    /// <param name="canceled">Whether the run was cancelled rather than ended.</param>
    /// <remarks>
    /// The run's final counters are folded into its graph's settled totals under the gate, so the
    /// observable instruments keep reporting what the run contributed after it is gone. Called from the one
    /// place that settles a run, after the counters stopped moving.
    /// </remarks>
    internal static void RunEnded(LocalRun run, Activity? activity, Exception? failure, bool canceled)
    {
        try
        {
            lock (Gate)
            {
                if (Live.Remove(run))
                {
                    string graph = run.Graph.ToString();

                    if (!Settled.TryGetValue(graph, out SettledTotals? totals))
                    {
                        totals = new SettledTotals();
                        Settled.Add(graph, totals);
                    }

                    totals.Dropped += run.DroppedElements;
                    totals.Supervised += run.SupervisedFailures;
                    totals.Poisoned += run.PoisonElements;
                    totals.Checkpoints += run.Checkpoints;
                }
            }

            string outcome = failure is not null ? "failed" : canceled ? "canceled" : "completed";

            Ended.Add(
                1,
                new KeyValuePair<string, object?>(GraphTag, run.Graph.ToString()),
                new KeyValuePair<string, object?>(OutcomeTag, outcome));

            if (activity is not null)
            {
                _ = activity.SetTag(OutcomeTag, outcome);

                if (failure is not null)
                {
                    _ = activity.SetStatus(ActivityStatusCode.Error, failure.Message);
                }
                else if (!canceled)
                {
                    _ = activity.SetStatus(ActivityStatusCode.Ok);
                }

                activity.Dispose();
            }
        }
        catch
        {
            // Telemetry never fails a run, and this is called from the one method that must not throw.
        }
    }

    /// <summary>Opens the span covering one materialization conversation.</summary>
    /// <param name="graph">The document fingerprint of the pipeline being materialized, as text.</param>
    /// <param name="durable">Whether the materialization declares durability.</param>
    /// <returns>The span, or <see langword="null"/> when nothing is listening.</returns>
    /// <remarks>
    /// The caller owns the span's end — the natural spelling is <c>using</c> over this call — and reports
    /// its outcome through <see cref="Materialized"/> or <see cref="MaterializeFailed"/>, because the run
    /// identity is not known until the cluster has answered.
    /// </remarks>
    internal static Activity? Materializing(string graph, bool durable)
    {
        try
        {
            Activity? activity = Source.StartActivity("dataflow.materialize");

            _ = activity?.SetTag(GraphTag, graph);
            _ = activity?.SetTag(DurableTag, durable);

            return activity;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Records that a materialization was accepted and which run it started.</summary>
    /// <param name="activity">The span <see cref="Materializing"/> opened.</param>
    /// <param name="runId">The identity of the started run.</param>
    internal static void Materialized(Activity? activity, string runId)
    {
        try
        {
            _ = activity?.SetTag(RunIdTag, runId);
            _ = activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch
        {
            // Telemetry never fails a materialization.
        }
    }

    /// <summary>Records that a materialization was refused or failed to be delivered.</summary>
    /// <param name="activity">The span <see cref="Materializing"/> opened.</param>
    /// <param name="failure">What refused it.</param>
    internal static void MaterializeFailed(Activity? activity, Exception failure)
    {
        try
        {
            _ = activity?.SetStatus(ActivityStatusCode.Error, failure.Message);
        }
        catch
        {
            // Telemetry never fails a materialization.
        }
    }

    /// <summary>Records how long one checkpoint held its run.</summary>
    /// <param name="graph">The fingerprint of the graph the run is of.</param>
    /// <param name="held">The hold, measured on the run's clock.</param>
    internal static void CheckpointHeld(GraphFingerprint graph, TimeSpan held)
    {
        try
        {
            Held.Record(
                held.TotalSeconds,
                new KeyValuePair<string, object?>(GraphTag, graph.ToString()));
        }
        catch
        {
            // A listener that throws is a broken observer, not a broken checkpoint.
        }
    }

    /// <summary>Reads one cumulative instrument: every live run's counter plus what settled runs left.</summary>
    /// <param name="live">The counter to read from a live run.</param>
    /// <param name="settled">The matching total to read from a graph's settled runs.</param>
    /// <returns>One measurement per graph this process has run.</returns>
    private static List<Measurement<long>> Observe(
        Func<LocalRun, long> live,
        Func<SettledTotals, long> settled)
    {
        List<Measurement<long>> measurements = [];

        try
        {
            lock (Gate)
            {
                Dictionary<string, long> byGraph = new(StringComparer.Ordinal);

                foreach (LocalRun run in Live)
                {
                    string graph = run.Graph.ToString();

                    byGraph[graph] = byGraph.GetValueOrDefault(graph) + live(run);
                }

                foreach (KeyValuePair<string, SettledTotals> graph in Settled)
                {
                    byGraph[graph.Key] = byGraph.GetValueOrDefault(graph.Key) + settled(graph.Value);
                }

                foreach (KeyValuePair<string, long> graph in byGraph)
                {
                    measurements.Add(new Measurement<long>(
                        graph.Value,
                        new KeyValuePair<string, object?>(GraphTag, graph.Key)));
                }
            }
        }
        catch
        {
            // A collection that failed reports what it gathered; the next interval reads again.
        }

        return measurements;
    }

    /// <summary>What runs of one graph left behind when they settled.</summary>
    private sealed class SettledTotals
    {
        /// <summary>Gets or sets the elements their overflow policies discarded.</summary>
        internal long Dropped { get; set; }

        /// <summary>Gets or sets the failures their supervision scopes intercepted.</summary>
        internal long Supervised { get; set; }

        /// <summary>Gets or sets the elements their retrying scopes gave up on.</summary>
        internal long Poisoned { get; set; }

        /// <summary>Gets or sets the checkpoints they wrote.</summary>
        internal long Checkpoints { get; set; }
    }
}
