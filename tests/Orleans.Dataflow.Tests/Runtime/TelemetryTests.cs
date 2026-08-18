using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DurableFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What this library publishes to OpenTelemetry: one meter, one activity source, and the names a
/// deployment subscribes by.
/// </summary>
/// <remarks>
/// <para>
/// The names are the contract, so every one of them is spelled here as a literal rather than read from the
/// production constant. A test that echoed the constant back would pass for any rename, which is precisely
/// the change a subscriber cannot survive.
/// </para>
/// <para>
/// <b>The meter is process-global and this suite runs in parallel</b>, so every run of every other test in
/// this assembly is emitting into the very instruments these tests read. Nothing here may assert a global
/// total. Each test therefore runs a graph whose document — and so whose fingerprint — is its own, and every
/// assertion filters the measurements by the <c>dataflow.graph</c> tag equal to that fingerprint. The
/// distinctive constants in the graphs below (a buffer capacity nobody else uses, a slot named for the test)
/// exist for exactly that reason and for no behavioural one.
/// </para>
/// <para>
/// <b>Two emissions are deterministic and the tests lean on it.</b> A run's start event is emitted before
/// materialization hands the handle back, and its end event — together with the end of its activity — is
/// emitted inside the method that settles the run, before the completion task transitions. So a test that
/// has awaited <see cref="RunHandle.Completion"/> may read both without waiting for anything, and does.
/// </para>
/// </remarks>
public sealed class TelemetryTests
{
    /// <summary>The name of both the meter and the activity source a deployment subscribes to.</summary>
    private const string SourceName = "Orleans.Dataflow";

    /// <summary>The tag every instrument carries: the document fingerprint of the graph being run.</summary>
    private const string GraphTag = "dataflow.graph";

    /// <summary>The tag saying how a run ended.</summary>
    private const string OutcomeTag = "dataflow.run.outcome";

    /// <summary>The tag saying whether a start continued a stored position.</summary>
    private const string ResumedTag = "dataflow.run.resumed";

    [Fact]
    public async Task ACompletedRunEmitsOneStartAndOneEndUnderItsOwnGraph()
    {
        using MeterProbe meter = new();

        RunnableGraph graph = Buffered(7901, "telemetry-completed", out ResultSlot<long> total);
        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The start is emitted while the run is being launched, so it is already recorded by the time the
        // caller holds a handle. Fresh rather than resumed, which is the tag that tells the two apart.
        Assert.Equal(1, meter.Count("orleans.dataflow.runs.started", fingerprint));
        Assert.False(meter.Single("orleans.dataflow.runs.started", fingerprint).Resumed);

        await run.Completion;

        // No poll and no wait: the ended event is emitted before the completion task transitions, so a
        // caller that has awaited completion has already observed it.
        Assert.Equal(1, meter.Count("orleans.dataflow.runs.ended", fingerprint));
        Assert.Equal("completed", meter.Single("orleans.dataflow.runs.ended", fingerprint).Outcome);
        Assert.Equal(1d, meter.Single("orleans.dataflow.runs.ended", fingerprint).Value);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AFailedRunEndsWithTheFailedOutcome()
    {
        using MeterProbe meter = new();

        InvalidOperationException failure = new("the folder refuses");
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Buffer(new BufferOptions { Capacity = 7907, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(
                s => s.Aggregate(0L, (sum, value) => value == 2 ? throw failure : sum + value),
                "telemetry-failed",
                out ResultSlot<long> _);

        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Equal("failed", meter.Single("orleans.dataflow.runs.ended", fingerprint).Outcome);
        Assert.Equal(1, meter.Count("orleans.dataflow.runs.ended", fingerprint));
    }

    [Fact]
    public async Task ACancelledRunEndsWithTheCanceledOutcome()
    {
        using MeterProbe meter = new();

        Gate gate = new();
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Buffer(new BufferOptions { Capacity = 7919, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();

                        return sum + value;
                    }),
                "telemetry-canceled",
                out ResultSlot<long> _);

        string fingerprint = graph.Fingerprint.ToString();

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await gate.Reached;

        ValueTask disposing = run.DisposeAsync();

        gate.Open();
        await disposing;

        // Three outcomes and not two: a cancelled run has no ending, and the counter still records that it
        // stopped, because an operator watching starts against ends would otherwise see a run that never
        // finished.
        Assert.Equal("canceled", meter.Single("orleans.dataflow.runs.ended", fingerprint).Outcome);
    }

    [Fact]
    public async Task TheDropCounterReportsWhatOneGraphsRunsDiscarded()
    {
        using MeterProbe meter = new();

        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9);

        elements.PullBarrier = position =>
        {
            if (position == 9)
            {
                _ = exhausted.TrySetResult();
            }

            return position == 1 ? gate.Reached : null;
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = OverflowPolicy.DropOldest })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();

                        return sum + value;
                    }),
                "telemetry-dropped",
                out ResultSlot<long> _);

        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await exhausted.Task;

        gate.Open();
        await run.Completion;

        // Read after the run settled, which is the case worth proving: the cumulative instruments sum every
        // live run's counters with what settled runs left behind, so a run that is gone still contributes.
        meter.Collect();

        Assert.Equal(5d, meter.Latest("orleans.dataflow.elements.dropped", fingerprint));

        // And the reading is monotonic across collections, because the run is counted on exactly one side of
        // the live-to-settled handoff.
        meter.Collect();

        Assert.Equal(5d, meter.Latest("orleans.dataflow.elements.dropped", fingerprint));
    }

    [Fact]
    public async Task TheSupervisedFailureCounterSurvivesTheRunThatProducedIt()
    {
        using MeterProbe meter = new();

        List<int> observed = [];
        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Scan(0, (running, value) => running + value))
            .Buffer(new BufferOptions { Capacity = 7927, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(s => s.ForEach(observed.Add));

        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        meter.Collect();

        Assert.Equal([1, 4, 8], observed);
        Assert.Equal(1d, meter.Latest("orleans.dataflow.failures.supervised", fingerprint));
        Assert.Equal(0d, meter.Latest("orleans.dataflow.elements.poison", fingerprint));
    }

    [Fact]
    public async Task ADurableRunsCapturesAreCountedAndItsHoldsAreSampled()
    {
        using MeterProbe meter = new();

        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .Buffer(new BufferOptions { Capacity = 7933, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(TestSink.Marking<int>("mark", committed.Add));

        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "telemetry-durable", everyElements: 3),
            TestToken);
        await run.Completion;

        meter.Collect();

        Assert.Equal(2L, run.Checkpoints);
        Assert.Equal(2d, meter.Latest("orleans.dataflow.checkpoints.written", fingerprint));

        // One sample per hold, which is not the same number as the count of captures: a hold whose write was
        // skipped because the run was over is still a hold the run paid for. So the histogram is asserted to
        // have at least as many samples as there were captures, tagged with this graph and nobody else's.
        Assert.True(
            meter.Count("orleans.dataflow.checkpoint.hold.duration", fingerprint) >= 2,
            string.Create(
                CultureInfo.InvariantCulture,
                $"expected at least two hold samples for {fingerprint} and saw {meter.Count("orleans.dataflow.checkpoint.hold.duration", fingerprint)}"));

        Assert.All(
            meter.For("orleans.dataflow.checkpoint.hold.duration", fingerprint),
            sample => Assert.True(sample.Value >= 0d, sample.Value.ToString(CultureInfo.InvariantCulture)));
    }

    [Fact]
    public async Task ARunStartedFromACheckpointIsTaggedAsResumed()
    {
        using MeterProbe meter = new();

        InMemoryCheckpointStore store = new();

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .Buffer(new BufferOptions { Capacity = 7963, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(TestSink.Marking<int>("mark", static _ => { }));

        string fingerprint = graph.Fingerprint.ToString();

        await using (RunHandle first = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "telemetry-resumed", everyElements: 3),
            TestToken))
        {
            await first.Completion;
        }

        await using RunHandle again = await Host.MaterializeFromCheckpointAsync(
            graph,
            Durable(store, "telemetry-resumed", everyElements: 3),
            TestToken);

        await again.Completion;

        // Two starts of one graph, told apart by the one tag that exists to tell them apart. Without it a
        // deployment counting starts against ends would read a resume as a new run, which is exactly the
        // arithmetic an operator uses to decide whether anything is wrong.
        IReadOnlyList<Measured> started = meter.For("orleans.dataflow.runs.started", fingerprint);

        Assert.Equal(2, started.Count);
        Assert.False(started[0].Resumed);
        Assert.True(started[1].Resumed);

        // Both attempts ended, and both ended well: a resume of a finished position has nothing left to do
        // and completes, which is still an ending and still counted as one.
        Assert.Equal(2, meter.Count("orleans.dataflow.runs.ended", fingerprint));
        Assert.All(
            meter.For("orleans.dataflow.runs.ended", fingerprint),
            ended => Assert.Equal("completed", ended.Outcome));
    }

    [Fact]
    public async Task ARunThatWritesNothingSamplesNoHoldAtAll()
    {
        using MeterProbe meter = new();

        RunnableGraph graph = Buffered(7937, "telemetry-no-holds", out ResultSlot<long> _);
        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        meter.Collect();

        Assert.Equal(0, meter.Count("orleans.dataflow.checkpoint.hold.duration", fingerprint));
        Assert.Equal(0d, meter.Latest("orleans.dataflow.checkpoints.written", fingerprint));
    }

    [Fact]
    public async Task TheMeterPublishesSevenInstrumentsUnderTheNamesAndUnitsADashboardReads()
    {
        using MeterProbe meter = new();

        // A run of anything, so that the meter and its instruments exist before they are enumerated: a
        // listener is told about instruments that already exist when it starts and about ones created
        // afterwards, and this makes the first case the one under test.
        RunnableGraph graph = Buffered(7841, "telemetry-instruments", out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The names and the units are the contract a subscriber writes down, and a unit is not decoration:
        // a dashboard that divides by it renders the wrong number when it changes. Seven instruments, each
        // named once here, is also the statement that this library publishes no eighth thing a deployment
        // would have to discover by reading the source.
        Assert.Equal("{run}", meter.Unit("orleans.dataflow.runs.started"));
        Assert.Equal("{run}", meter.Unit("orleans.dataflow.runs.ended"));
        Assert.Equal("s", meter.Unit("orleans.dataflow.checkpoint.hold.duration"));
        Assert.Equal("{element}", meter.Unit("orleans.dataflow.elements.dropped"));
        Assert.Equal("{failure}", meter.Unit("orleans.dataflow.failures.supervised"));
        Assert.Equal("{element}", meter.Unit("orleans.dataflow.elements.poison"));
        Assert.Equal("{checkpoint}", meter.Unit("orleans.dataflow.checkpoints.written"));

        Assert.Equal(7, meter.Published.Count);
    }

    [Fact]
    public async Task ACompletedRunProducesOneSpanForItsWholeLife()
    {
        using ActivityProbe activities = new();

        RunnableGraph graph = Buffered(7949, "telemetry-span-ok", out ResultSlot<long> _);
        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Activity span = activities.Single("dataflow.run", fingerprint);

        Assert.Equal(ActivityStatusCode.Ok, span.Status);
        Assert.Equal("completed", span.GetTagItem(OutcomeTag) as string);
        Assert.False(span.GetTagItem(ResumedTag) as bool?);

        // The span covers the run rather than the call that started it, so it is stopped by the time the run
        // has ended and its duration is the run's.
        Assert.True(span.Duration >= TimeSpan.Zero, span.Duration.ToString());

        // The local host materializes in this process and talks to nobody, so there is no conversation to
        // span: the materialize span belongs to the clustered host, and asserting its absence here is what
        // keeps that difference stated rather than assumed.
        Assert.Empty(activities.Named("dataflow.materialize"));
    }

    [Fact]
    public async Task AFailedRunsSpanCarriesTheErrorAndTheFailedOutcome()
    {
        using ActivityProbe activities = new();

        InvalidOperationException failure = new("the folder refuses");
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Buffer(new BufferOptions { Capacity = 7951, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(
                s => s.Aggregate(0L, (sum, value) => value == 2 ? throw failure : sum + value),
                "telemetry-span-error",
                out ResultSlot<long> _);

        string fingerprint = graph.Fingerprint.ToString();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Activity span = activities.Single("dataflow.run", fingerprint);

        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("the folder refuses", span.StatusDescription);
        Assert.Equal("failed", span.GetTagItem(OutcomeTag) as string);
    }

    /// <summary>Builds the ordinary summing graph behind a buffer of a capacity nobody else declares.</summary>
    /// <param name="capacity">The capacity, which exists to make this graph's fingerprint the test's own.</param>
    /// <param name="slot">The name of the result, which exists for the same reason.</param>
    /// <param name="total">When this method returns, the slot the sum resolves.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// The buffer is declared with backpressure and a capacity no run here can fill, so it changes what the
    /// document says and nothing about what the run does. Two graphs of one shape share a fingerprint, and a
    /// test filtering on a shared fingerprint would be reading another test's runs.
    /// </remarks>
    private static RunnableGraph Buffered(int capacity, string slot, out ResultSlot<long> total) =>
        Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Buffer(new BufferOptions { Capacity = capacity, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), slot, out total);

    /// <summary>One measurement, flattened to the members these tests assert on.</summary>
    /// <param name="Instrument">The instrument's name.</param>
    /// <param name="Value">The value measured.</param>
    /// <param name="Graph">The <c>dataflow.graph</c> tag, which is how a test finds its own runs.</param>
    /// <param name="Outcome">The <c>dataflow.run.outcome</c> tag, for the end event.</param>
    /// <param name="Resumed">The <c>dataflow.run.resumed</c> tag, for the start event.</param>
    private sealed record Measured(
        string Instrument,
        double Value,
        string? Graph,
        string? Outcome,
        bool? Resumed);

    /// <summary>
    /// A subscriber to this library's meter, holding every measurement it published while it lived.
    /// </summary>
    /// <remarks>
    /// The listener subscribes by meter name exactly as a deployment's exporter does, and it is deliberately
    /// not filtered any further: which instruments exist under that name is part of what these tests read.
    /// Measurements arrive from whichever thread produced them — including other tests' run threads — so the
    /// list is guarded, and every query filters by graph.
    /// </remarks>
    private sealed class MeterProbe : IDisposable
    {
        private readonly List<Measured> _measurements = [];
        private readonly Dictionary<string, string?> _published = new(StringComparer.Ordinal);
        private readonly MeterListener _listener;

        /// <summary>Initializes a new instance of the <see cref="MeterProbe"/> class and starts listening.</summary>
        internal MeterProbe()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (string.Equals(instrument.Meter.Name, SourceName, StringComparison.Ordinal))
                    {
                        Publish(instrument);
                        listener.EnableMeasurementEvents(instrument);
                    }
                },
            };

            _listener.SetMeasurementEventCallback<long>(
                (instrument, measurement, tags, _) => Record(instrument, measurement, tags));

            _listener.SetMeasurementEventCallback<double>(
                (instrument, measurement, tags, _) => Record(instrument, measurement, tags));

            _listener.Start();
        }

        /// <summary>Gets the instruments this meter published, by name, with the unit each declares.</summary>
        /// <value>One entry per instrument, whatever any run of any graph has done.</value>
        internal Dictionary<string, string?> Published
        {
            get
            {
                lock (_published)
                {
                    return new Dictionary<string, string?>(_published, StringComparer.Ordinal);
                }
            }
        }

        /// <summary>Reads the unit one published instrument declares.</summary>
        /// <param name="instrument">The instrument's name.</param>
        /// <returns>The unit, having asserted that the instrument was published at all.</returns>
        internal string? Unit(string instrument)
        {
            Dictionary<string, string?> published = Published;

            Assert.True(published.ContainsKey(instrument), $"The meter published no instrument named '{instrument}'.");

            return published[instrument];
        }

        /// <summary>Asks every observable instrument for a reading.</summary>
        /// <remarks>
        /// The cumulative counters publish nothing on their own; a collector pulls them, and this is that
        /// pull. Calling it twice is how a test says a reading is stable rather than a side effect of being
        /// read.
        /// </remarks>
        internal void Collect() => _listener.RecordObservableInstruments();

        /// <summary>Counts the measurements one instrument published for one graph.</summary>
        /// <param name="instrument">The instrument's name.</param>
        /// <param name="graph">The graph fingerprint, as text.</param>
        /// <returns>How many measurements matched.</returns>
        internal int Count(string instrument, string graph) => For(instrument, graph).Count;

        /// <summary>Reads the measurements one instrument published for one graph.</summary>
        /// <param name="instrument">The instrument's name.</param>
        /// <param name="graph">The graph fingerprint, as text.</param>
        /// <returns>The measurements, oldest first.</returns>
        internal IReadOnlyList<Measured> For(string instrument, string graph)
        {
            lock (_measurements)
            {
                return
                [
                    .. _measurements.Where(measured =>
                        string.Equals(measured.Instrument, instrument, StringComparison.Ordinal) &&
                        string.Equals(measured.Graph, graph, StringComparison.Ordinal)),
                ];
            }
        }

        /// <summary>Reads the one measurement one instrument published for one graph.</summary>
        /// <param name="instrument">The instrument's name.</param>
        /// <param name="graph">The graph fingerprint, as text.</param>
        /// <returns>The measurement, having asserted that there is exactly one.</returns>
        internal Measured Single(string instrument, string graph) => Assert.Single(For(instrument, graph));

        /// <summary>Reads the most recent value one instrument reported for one graph.</summary>
        /// <param name="instrument">The instrument's name.</param>
        /// <param name="graph">The graph fingerprint, as text.</param>
        /// <returns>The value, or zero when this graph has never been reported.</returns>
        /// <remarks>
        /// Zero for an absent reading rather than a failure, because an observable counter reports a graph
        /// only once some run of it has existed: "nothing was reported" and "nothing has happened" are the
        /// same claim for a cumulative counter, and a test asserting zero means both.
        /// </remarks>
        internal double Latest(string instrument, string graph)
        {
            IReadOnlyList<Measured> measured = For(instrument, graph);

            return measured.Count == 0 ? 0d : measured[^1].Value;
        }

        /// <inheritdoc/>
        public void Dispose() => _listener.Dispose();

        /// <summary>Keeps the name and unit of one instrument this meter published.</summary>
        /// <param name="instrument">The instrument.</param>
        private void Publish(Instrument instrument)
        {
            lock (_published)
            {
                _published[instrument.Name] = instrument.Unit;
            }
        }

        /// <summary>Copies one measurement out of the callback's span and keeps it.</summary>
        /// <param name="instrument">The instrument that published it.</param>
        /// <param name="measurement">The value.</param>
        /// <param name="tags">The tags, which live only for the duration of the callback.</param>
        private void Record(
            Instrument instrument,
            double measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            string? graph = null;
            string? outcome = null;
            bool? resumed = null;

            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (string.Equals(tag.Key, GraphTag, StringComparison.Ordinal))
                {
                    graph = tag.Value as string;
                }
                else if (string.Equals(tag.Key, OutcomeTag, StringComparison.Ordinal))
                {
                    outcome = tag.Value as string;
                }
                else if (string.Equals(tag.Key, ResumedTag, StringComparison.Ordinal))
                {
                    resumed = tag.Value as bool?;
                }
            }

            lock (_measurements)
            {
                _measurements.Add(new Measured(instrument.Name, measurement, graph, outcome, resumed));
            }
        }
    }

    /// <summary>
    /// A subscriber to this library's activity source, holding every span it stopped while it lived.
    /// </summary>
    /// <remarks>
    /// Sampling is <see cref="ActivitySamplingResult.AllData"/>, because a span nobody samples is never
    /// created at all and the tests would then be asserting the absence of their own listener. Spans are
    /// collected when they stop rather than when they start, so the outcome tag and the status are already
    /// on them.
    /// </remarks>
    private sealed class ActivityProbe : IDisposable
    {
        private readonly List<Activity> _stopped = [];
        private readonly ActivityListener _listener;

        /// <summary>Initializes a new instance of the <see cref="ActivityProbe"/> class and starts listening.</summary>
        internal ActivityProbe()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = static source => string.Equals(source.Name, SourceName, StringComparison.Ordinal),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                SampleUsingParentId =
                    static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllData,
                ActivityStopped = Record,
            };

            ActivitySource.AddActivityListener(_listener);
        }

        /// <summary>Reads every stopped span of one name, whatever graph it belongs to.</summary>
        /// <param name="name">The span's operation name.</param>
        /// <returns>The spans, oldest first.</returns>
        internal IReadOnlyList<Activity> Named(string name)
        {
            lock (_stopped)
            {
                return [.. _stopped.Where(activity => string.Equals(activity.OperationName, name, StringComparison.Ordinal))];
            }
        }

        /// <summary>Reads the one stopped span of one name belonging to one graph.</summary>
        /// <param name="name">The span's operation name.</param>
        /// <param name="graph">The graph fingerprint, as text.</param>
        /// <returns>The span, having asserted that there is exactly one.</returns>
        internal Activity Single(string name, string graph) =>
            Assert.Single(
                Named(name),
                activity => string.Equals(activity.GetTagItem(GraphTag) as string, graph, StringComparison.Ordinal));

        /// <inheritdoc/>
        public void Dispose() => _listener.Dispose();

        /// <summary>Keeps one stopped span.</summary>
        /// <param name="activity">The span.</param>
        private void Record(Activity activity)
        {
            lock (_stopped)
            {
                _stopped.Add(activity);
            }
        }
    }
}
