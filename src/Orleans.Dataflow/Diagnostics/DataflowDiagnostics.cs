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
/// <b>Tags are bounded, and bounded by this class rather than by the deployment.</b> Every instrument
/// carries <c>dataflow.graph</c> — the document fingerprint. How many distinct fingerprints a process sees
/// is not a fact about how many graph shapes were <em>written</em>: a fingerprint covers every number in a
/// document, so a graph whose buffer capacity, take count, or collect bound comes from a request mints a
/// fresh one per request. Left alone that is unbounded cardinality on seven instruments and an entry per
/// value in the settled table, neither of which anything prunes. So the first
/// <see cref="MaxTaggedGraphs"/> distinct fingerprints a process sees keep their own tag value and every
/// one after that is reported under <see cref="OverflowGraph"/>, which is a real bucket and not a
/// discard — its totals are the sum of every graph that landed in it. A run's identity is deliberately not
/// a metric tag at all, because run ids are unbounded by construction; it appears on activities, where
/// per-occurrence identity is the point and no aggregation is paying for it.
/// </para>
/// <para>
/// The naming is first-come and permanent. A fingerprint that has been named keeps its name for the life of
/// the process and a fingerprint that overflowed stays overflowed, which is what keeps every cumulative
/// reading monotonic: each graph contributes to exactly one series, and the bucket a run's counters land in
/// cannot change between the run starting and settling.
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

    /// <summary>The tag carrying the graph document fingerprint, or <see cref="OverflowGraph"/>.</summary>
    internal const string GraphTag = "dataflow.graph";

    /// <summary>How many distinct graph fingerprints appear on metrics under their own name.</summary>
    /// <remarks>
    /// <para>
    /// A ceiling rather than an expectation. A deployment running a thousand distinct graph shapes in one
    /// process is already past what a metric tag can usefully carry, so the number is set where a real
    /// deployment never reaches it and a document parameterized from a request reaches it in minutes — and
    /// the point of the bound is entirely the second case.
    /// </para>
    /// <para>
    /// It is a bound on this process, not on the deployment. Two silos each name their own first thousand,
    /// so a fingerprint named on one and overflowed on another is possible and is the honest reading: what a
    /// tag says is what that process could still tell apart.
    /// </para>
    /// </remarks>
    internal const int MaxTaggedGraphs = 1024;

    /// <summary>The <see cref="GraphTag"/> value every graph past <see cref="MaxTaggedGraphs"/> shares.</summary>
    /// <remarks>
    /// Deliberately not a fingerprint, and deliberately spelled so that it cannot be mistaken for one: a
    /// fingerprint is <c>sha256:</c> and sixty-four hex digits, and this is the only value of this tag that
    /// is not. A dashboard that sees it is being told that this process ran more distinct graphs than it
    /// keeps series for, and that the counters under it are a sum across all of them.
    /// </remarks>
    internal const string OverflowGraph = "(other)";

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

    /// <summary>The gate over <see cref="Live"/>, <see cref="Settled"/>, and <see cref="Names"/>.</summary>
    /// <remarks>
    /// One lock rather than concurrent structures, because every side is cold — a run enters and leaves
    /// once, a checkpoint is held rarely, a collector reads once per export interval — and because the lock
    /// is what makes the readings monotonic: a run mid-handoff from live to settled is counted on exactly
    /// one side of it, and the name its counters are filed under is decided once, under this lock, rather
    /// than raced for by two threads that would then disagree about which series it belongs to.
    /// </remarks>
    private static readonly object Gate = new();

    private static readonly HashSet<LocalRun> Live = new(ReferenceEqualityComparer.Instance);

    /// <summary>What runs of each named graph left behind, keyed by tag value rather than by fingerprint.</summary>
    /// <remarks>
    /// Keyed by the tag and not by the graph, which is the whole of what bounds this table: every graph past
    /// the cap files under one key, so the table holds at most <see cref="MaxTaggedGraphs"/> entries plus the
    /// overflow bucket, whatever a deployment does to its documents.
    /// </remarks>
    private static readonly Dictionary<string, SettledTotals> Settled = new(StringComparer.Ordinal);

    private static readonly BoundedGraphNames Names = new(MaxTaggedGraphs);

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
    /// <para>
    /// The activity is started with whatever ambient parent the materializing caller had — a client's
    /// materialize span, a grain call's span — and it is the caller's job to put its own
    /// <see cref="Activity.Current"/> back afterwards, because this span outlives the call that started it.
    /// </para>
    /// <para>
    /// <b>This is where a graph earns its tag value or is folded into the bucket</b>, because it is the
    /// first thing every run does, and deciding once means every later emission for the same run agrees.
    /// The span keeps the fingerprint itself either way: a span is one occurrence and carries a run id
    /// already, so nothing aggregates over it and there is no cardinality to save by blurring it.
    /// </para>
    /// </remarks>
    internal static Activity? RunStarted(LocalRun run, bool resumed)
    {
        try
        {
            string graph = run.Graph.ToString();
            string tag;

            lock (Gate)
            {
                _ = Live.Add(run);

                tag = Names.Name(graph);
            }

            Started.Add(
                1,
                new KeyValuePair<string, object?>(GraphTag, tag),
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
            string tag;

            lock (Gate)
            {
                tag = Names.Name(run.Graph.ToString());

                if (Live.Remove(run))
                {
                    if (!Settled.TryGetValue(tag, out SettledTotals? totals))
                    {
                        totals = new SettledTotals();
                        Settled.Add(tag, totals);
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
                new KeyValuePair<string, object?>(GraphTag, tag),
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
    /// <remarks>
    /// The tag goes through the same naming as every other instrument's, so a histogram cannot be the one
    /// place a deployment's request-shaped fingerprints get through. The lookup takes the gate, which is
    /// affordable exactly because a checkpoint hold is rare — it is one of the three eager emissions this
    /// class makes, and all three are cold.
    /// </remarks>
    internal static void CheckpointHeld(GraphFingerprint graph, TimeSpan held)
    {
        try
        {
            string tag;

            lock (Gate)
            {
                tag = Names.Name(graph.ToString());
            }

            Held.Record(
                held.TotalSeconds,
                new KeyValuePair<string, object?>(GraphTag, tag));
        }
        catch
        {
            // A listener that throws is a broken observer, not a broken checkpoint.
        }
    }

    /// <summary>Reads one cumulative instrument: every live run's counter plus what settled runs left.</summary>
    /// <param name="live">The counter to read from a live run.</param>
    /// <param name="settled">The matching total to read from a graph's settled runs.</param>
    /// <returns>
    /// One measurement per tag value, which is one per named graph plus at most the overflow bucket, and
    /// therefore never more than <see cref="MaxTaggedGraphs"/> plus one however many graphs ran.
    /// </returns>
    /// <remarks>
    /// A live run's graph is named through the same table as a settled one's, so a run in flight and the
    /// totals it will leave behind land on the same series. That is not merely tidy: it is what stops a
    /// run's contribution from appearing to move between series when it settles, which a cumulative
    /// instrument may not do.
    /// </remarks>
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
                    string graph = Names.Name(run.Graph.ToString());

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

    /// <summary>
    /// The bounded naming of graphs on metric tags: the first few keep their fingerprint, the rest share one
    /// bucket.
    /// </summary>
    /// <param name="capacity">How many distinct graphs may be named before the bucket takes over.</param>
    /// <remarks>
    /// <para>
    /// A class of its own, with the capacity handed in, because the rule it states is worth being able to
    /// exercise at a size a test can reach. The production instance is created with
    /// <see cref="MaxTaggedGraphs"/> and there is exactly one of it, so a test that filled the real table
    /// would blur every other test's graphs into the bucket for the rest of the process; a test over an
    /// instance of its own proves the fold without touching what the process is reporting.
    /// </para>
    /// <para>
    /// <b>It never forgets and never evicts</b>, which is what a least-recently-used table would have done
    /// instead and is the wrong shape here. A cumulative counter's series may not move: a graph whose runs
    /// were counted under its own name and were later re-filed under the bucket would make both series jump
    /// — one down, which a counter must never do. Permanence costs a bounded table and buys monotonicity.
    /// </para>
    /// </remarks>
    internal sealed class BoundedGraphNames(int capacity)
    {
        private readonly HashSet<string> _named = new(StringComparer.Ordinal);

        /// <summary>Gets how many graphs currently have a tag value of their own.</summary>
        /// <value>A count of at most the capacity this instance was built with.</value>
        internal int Count => _named.Count;

        /// <summary>Answers the tag value one graph is reported under.</summary>
        /// <param name="graph">The graph's document fingerprint, as text.</param>
        /// <returns>
        /// <paramref name="graph"/> itself when it is already named or there is still room to name it;
        /// otherwise <see cref="OverflowGraph"/>.
        /// </returns>
        /// <remarks>
        /// Not thread-safe on its own and deliberately so: every caller holds <see cref="Gate"/>, which is
        /// the same lock that makes the live-to-settled handoff count once, and a second lock inside here
        /// would be a second answer to a question that already has one.
        /// </remarks>
        internal string Name(string graph)
        {
            if (_named.Contains(graph))
            {
                return graph;
            }

            if (_named.Count >= capacity)
            {
                return OverflowGraph;
            }

            _ = _named.Add(graph);

            return graph;
        }
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
