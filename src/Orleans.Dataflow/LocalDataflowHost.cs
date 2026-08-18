using System.Globalization;
using System.Text;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow;

/// <summary>
/// The in-process host that turns a closed graph into a running one.
/// </summary>
/// <remarks>
/// <para>
/// Materializing is the only way work starts. Building a graph describes it and starts nothing; a host
/// takes that description, binds the author's delegates to it, and runs it. Materializing one graph twice
/// yields two independent runs: two enumerations of the source, two fold states starting from the same
/// seed, and two handles that answer only for themselves.
/// </para>
/// <para>
/// The host is stateless and holds no run. A run's lifetime is its <see cref="RunHandle"/>'s, and one host
/// instance can materialize any number of graphs from any number of threads.
/// </para>
/// <para>
/// <b>The clock is the host's.</b> Every stage of a run that reads a clock reads the one this host was
/// given, resolved when the graph is materialized and carried by the run from there (ADR 0005). The default
/// is <see cref="TimeProvider.System"/>; a test hands over a controlled one and the delays, the windows, the
/// timeouts, the rates, and the ticks of every run this host starts are measured by it. A document never
/// carries a clock, because a clock is runtime and not definition: two runs of one graph may be measured by
/// two different clocks and their fingerprints are the same.
/// </para>
/// </remarks>
public sealed class LocalDataflowHost
{
    private readonly IStageCatalog _catalog;
    private readonly StageRuntimeBinder _binder;
    private readonly TimeProvider _clock;

    /// <summary>Initializes a new instance of the <see cref="LocalDataflowHost"/> class.</summary>
    /// <remarks>
    /// The lambda-only host. It resolves exactly <see cref="LocalStageCatalog.Instance"/> and no registered
    /// provider, so a graph containing a registered stage is refused by name rather than half-executed. A
    /// host that has to run one takes the overload that registers the provider. Its clock is
    /// <see cref="TimeProvider.System"/>.
    /// </remarks>
    public LocalDataflowHost()
        : this(TimeProvider.System)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LocalDataflowHost"/> class with a clock.</summary>
    /// <param name="timeProvider">The clock every run this host starts measures time by.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The one option this host has, and it is a service rather than a setting: the operators that read a
    /// clock — <c>Delay</c>, <c>InitialDelay</c>, <c>Timeout</c>, <c>TakeWithin</c>, <c>SkipWithin</c>,
    /// <c>Throttle</c>, and <c>Source.Tick</c> — read this one and never
    /// <see cref="TimeProvider.System"/>, which is what makes a deterministic test of them possible at all.
    /// </para>
    /// <para>
    /// The clock is read when a graph is materialized and carried by the run, so a host handed a controlled
    /// clock measures every run it starts by it, and a graph is the same graph under either.
    /// </para>
    /// </remarks>
    public LocalDataflowHost(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);

        _catalog = LocalStageCatalog.Instance;
        _binder = StageRuntimeBinder.None;
        _clock = timeProvider;
    }

    /// <summary>Initializes a new instance of the <see cref="LocalDataflowHost"/> class with providers.</summary>
    /// <param name="configure">The registration of this host's catalogs, factories, and .NET push adapters.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configure"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configure"/> registered one binding name, one stage reference, or one provider twice.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The local half of "declare once, use twice". The very catalog, the very factory, and the very
    /// bindings a silo is given are given to this host, so one declaration serves both runtimes and a graph
    /// written against them runs in either — the runtime-factory seam's own claim, made checkable in a
    /// process with no cluster in it.
    /// </para>
    /// <para>
    /// The registrations are checked here, so a broken one stops the host from being constructed rather
    /// than surfacing at the first graph. What they resolve to is one immutable catalog and one immutable
    /// factory registry, shared by every graph this host materializes.
    /// </para>
    /// </remarks>
    public LocalDataflowHost(Action<ILocalDataflowBuilder> configure)
        : this(TimeProvider.System, configure)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LocalDataflowHost"/> class with a clock and providers.</summary>
    /// <param name="timeProvider">The clock every run this host starts measures time by.</param>
    /// <param name="configure">The registration of this host's catalogs, factories, and .NET push adapters.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="timeProvider"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configure"/> registered one binding name, one stage reference, or one provider twice.
    /// </exception>
    /// <remarks>
    /// The two options a host has, together. They are independent of each other: the clock reaches the local
    /// vocabulary's timing stages, and a registered stage receives the run's tokens and whatever its own
    /// provider gave it.
    /// </remarks>
    public LocalDataflowHost(TimeProvider timeProvider, Action<ILocalDataflowBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(configure);

        _clock = timeProvider;

        LocalRegistrations registrations = new();

        configure(registrations);
        registrations.Validate();

        // A configuration call that registered nothing leaves exactly the lambda-only host, down to the
        // catalog instance: merging the local vocabulary with nothing is the local vocabulary, and building
        // a copy of it would give this host a catalog of its own for no reason.
        if (!registrations.AnyRegistration)
        {
            _catalog = LocalStageCatalog.Instance;
            _binder = StageRuntimeBinder.None;

            return;
        }

        _catalog = registrations.Catalog;
        _binder = new StageRuntimeBinder(_catalog, new StageRuntimeRegistry(registrations.Factories));
    }

    /// <summary>Materializes a graph into a running run.</summary>
    /// <param name="graph">The closed graph to run.</param>
    /// <param name="cancellationToken">A token that cancels the run this call starts.</param>
    /// <returns>The handle of the started run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="graph"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The graph's document does not validate against <see cref="LocalStageCatalog.Instance"/>, or it is
    /// not the one linear chain of bound local stages this runtime executes.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The run is started before this method returns. There is no separate start step, because a
    /// materialized run that had not started would be a state with no use and one more thing to get wrong.
    /// </para>
    /// <para>
    /// The document is validated against the local stage catalog first. Every graph the authoring API can
    /// close passes, so the check is a defense rather than a gate: it is what stops a document from
    /// somewhere else from reaching a loop that assumes the vocabulary's shapes. A failed check throws and
    /// names every diagnostic, because a caller fixing a foreign document needs the whole report and not
    /// its first line.
    /// </para>
    /// <para>
    /// An already-canceled <paramref name="cancellationToken"/> does not make this call throw. The run
    /// starts, observes the token before its first pull, and ends canceled without ever enumerating the
    /// source, so the caller always receives a handle to await and dispose. Cancellation is an outcome of a
    /// run, not a failure of materialization.
    /// </para>
    /// </remarks>
    public ValueTask<RunHandle> MaterializeAsync(RunnableGraph graph, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(graph);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, _catalog);

        if (!report.IsValid)
        {
            throw new InvalidOperationException(Describe(report));
        }

        // A fresh identity per run rather than the graph's own, because two runs of one graph are two runs
        // and a source that publishes itself under this name must not publish two runs under one.
        LocalRunPlan plan = LocalRunPlanner.Compile(
            graph,
            _binder,
            string.Create(CultureInfo.InvariantCulture, $"local/{Guid.NewGuid():n}"),
            _clock);
        LocalRun run = LocalRun.Start(
            plan,
            graph.Fingerprint,
            graph.AuthoringNonce,
            durable: null,
            resumed: false,
            cancellationToken);

        return new ValueTask<RunHandle>(new RunHandle(run));
    }

    /// <summary>Materializes a graph into a running run that writes checkpoints.</summary>
    /// <param name="graph">The closed graph to run.</param>
    /// <param name="durable">Where this run's checkpoints go, what it is called, and when one is taken.</param>
    /// <param name="cancellationToken">A token that cancels the run this call starts.</param>
    /// <returns>The handle of the started run.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="graph"/> or <paramref name="durable"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <see cref="DurableRunOptions.RunId"/> is the default value, <see cref="DurableRunOptions.Interval"/> is
    /// not positive, or <see cref="DurableRunOptions.EveryElements"/> is not at least one.
    /// </exception>
    /// <exception cref="InvalidOperationException">The graph does not validate, or is not one this runtime executes.</exception>
    /// <remarks>
    /// <para>
    /// The ordinary materialization with one thing added: the run takes a checkpoint whenever its declared
    /// timing says one is due, by holding itself at the pause machinery's safe points, snapshotting its
    /// cursors, its durable scopes, and its commit marks, and writing all of it as one canonical document.
    /// <b>A run that declares neither an interval nor an element bound never touches the store</b>, and that
    /// is asserted rather than assumed.
    /// </para>
    /// <para>
    /// <b>Nothing is read here and the first write presents no ETag.</b> A fresh run believes the store holds
    /// nothing for its identity, so a run started under a name that already has a checkpoint is refused by
    /// the store at its first capture, loudly, with a
    /// <see cref="Hosting.CheckpointConflictException"/> that fails the run. That is the coordinator's
    /// fencing consequence rather than an extra check of this host's, and it is why starting fresh over a
    /// live run's identity cannot quietly overwrite it.
    /// </para>
    /// <para>
    /// <b>A clean end writes nothing.</b> A run that completes has an outcome and does not need a
    /// checkpoint, and a run that dies writes nothing by definition — which is exactly why the last stored
    /// capture is what a resume replays from, and why the duplicate window is measured from it.
    /// </para>
    /// </remarks>
    public ValueTask<RunHandle> MaterializeDurableAsync(
        RunnableGraph graph,
        DurableRunOptions durable,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(durable);

        LocalOptionGuard.Durable(durable, nameof(durable));

        return new ValueTask<RunHandle>(Start(graph, durable, checkpoint: null, etag: null, cancellationToken));
    }

    /// <summary>Materializes a graph into a run that continues the one a checkpoint describes.</summary>
    /// <param name="graph">The closed graph to run, which must be the one the checkpoint was taken of.</param>
    /// <param name="durable">Where the checkpoint is read from, what the run is called, and when the next one is taken.</param>
    /// <param name="cancellationToken">A token that cancels the run this call starts.</param>
    /// <returns>The handle of the started run.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="graph"/> or <paramref name="durable"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">The declared timing is not one this host can honor.</exception>
    /// <exception cref="InvalidOperationException">
    /// The store holds no checkpoint for that run, the stored document is not one this runtime can read, it
    /// was taken of a different graph or a different revision, or it names a node this graph has no seam
    /// for.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>Resume is the same run continuing.</b> The identity in <paramref name="durable"/> is the one the
    /// crashed attempt wrote under, the checkpoint is read with its ETag, and the resumed attempt presents
    /// that ETag at its own first capture — so a stale attempt still writing loses to this one exactly as a
    /// superseded coordinator does.
    /// </para>
    /// <para>
    /// <b>A different fingerprint is refused by name.</b> V1's rule is same-revision resume only: a
    /// checkpoint of another graph describes nodes that are not these nodes, so restoring a cursor into it
    /// would be restoring a position into a source that never counted it. Cross-revision migration is a
    /// recorded deferral (ADR 0007) and not a silent best effort.
    /// </para>
    /// <para>
    /// <b>What survives and what resets is exactly stated.</b> A source that declared a cursor reopens at
    /// the stored position; a durable scope's stages take back the state they exported; a marking sink takes
    /// back its count. <em>Everything else resets</em> — a scan outside a durable scope returns to its seed,
    /// a batch abandons its group, a distinct forgets its keys — because a resumed run builds every stage
    /// from the very factories a fresh run builds them from.
    /// </para>
    /// <para>
    /// <b>What the replay costs is at-least-once between commit marks.</b> Every element a source delivered
    /// after the last capture is delivered again, so a sink sees the elements between the stored cursor and
    /// the crash a second time. Nothing anywhere claims exactly-once. And where a graph holds elements
    /// between a cursor and its sink at capture time — a declared buffer, a junction — those elements are
    /// counted by the cursor and were not committed, so they are <em>lost</em> rather than replayed; the
    /// checkpoint carries both numbers so that the gap is a measurement rather than a surprise.
    /// </para>
    /// </remarks>
    public async ValueTask<RunHandle> MaterializeFromCheckpointAsync(
        RunnableGraph graph,
        DurableRunOptions durable,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(durable);

        LocalOptionGuard.Durable(durable, nameof(durable));

        StoredCheckpoint stored = await durable.Store
            .ReadAsync(graph.Document.Id, durable.RunId, cancellationToken)
            .ConfigureAwait(false) ??
            throw new InvalidOperationException(
                $"The checkpoint store holds nothing for the run '{durable.RunId}' of the graph '{graph.Document.Id}', so there is no run to continue. A run reaches its first checkpoint only once its declared timing has made one due; a run that crashed before that resumes by being started fresh.");

        if (!LocalCheckpointDocument.TryRead(
            stored.Document,
            out LocalCheckpoint? checkpoint,
            out IReadOnlyList<string> violations))
        {
            throw new InvalidOperationException(
                $"The checkpoint stored for the run '{durable.RunId}' of the graph '{graph.Document.Id}' is not one this runtime can read: {string.Join("; ", violations)}.");
        }

        if (checkpoint!.Graph != graph.Fingerprint)
        {
            throw new InvalidOperationException(
                $"The checkpoint stored for the run '{durable.RunId}' was taken of the graph {checkpoint.Graph} and this is a run of {graph.Fingerprint}. A resume continues the very graph the checkpoint describes: v1 resumes at the same revision only, and migrating a checkpoint across a changed document is a recorded deferral rather than something this host will guess at.");
        }

        if (checkpoint.Revision != graph.Document.Revision)
        {
            throw new InvalidOperationException(
                $"The checkpoint stored for the run '{durable.RunId}' was taken at revision {checkpoint.Revision} and this graph is revision {graph.Document.Revision}. A resume continues the same revision; cross-revision migration is a recorded deferral.");
        }

        return Start(graph, durable, checkpoint, stored.ETag, cancellationToken);
    }

    /// <summary>Compiles a graph, restores whatever a checkpoint carried, and starts the run.</summary>
    /// <param name="graph">The closed graph.</param>
    /// <param name="durable">The declared store, identity, and timing.</param>
    /// <param name="checkpoint">What a resume read, or <see langword="null"/> for a fresh run.</param>
    /// <param name="etag">The ETag the first capture presents, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">A token that cancels the run.</param>
    /// <returns>The handle of the started run.</returns>
    /// <remarks>
    /// The one path both durable spellings share, so that a resumed run and a fresh durable one differ in
    /// exactly two things — what the seams were handed before the first element, and which ETag the first
    /// capture presents — and in nothing else. The run identity handed to the planner is the author's rather
    /// than a fresh one per materialization, because a durable run is the same run continuing and anything
    /// composing its identity has to say so.
    /// </remarks>
    private RunHandle Start(
        RunnableGraph graph,
        DurableRunOptions durable,
        LocalCheckpoint? checkpoint,
        string? etag,
        CancellationToken cancellationToken)
    {
        GraphValidationReport report = GraphCompiler.Validate(graph.Document, _catalog);

        if (!report.IsValid)
        {
            throw new InvalidOperationException(Describe(report));
        }

        LocalRunPlan plan = LocalRunPlanner.Compile(graph, _binder, durable.RunId.Value, _clock);

        if (checkpoint is not null)
        {
            LocalResume.Restore(plan, checkpoint);
        }

        bool declared = durable.Interval is not null || durable.EveryElements is not null;

        LocalRun run = LocalRun.Start(
            plan,
            graph.Fingerprint,
            graph.AuthoringNonce,
            declared
                ? started => new LocalCheckpointer(
                    plan,
                    started.Pause,
                    _clock,
                    durable,
                    graph.Fingerprint,
                    graph.Document.Revision,
                    graph.Document.Id,
                    etag,
                    started.Faulted,
                    started.StopToken)
                : null,
            resumed: checkpoint is not null,
            cancellationToken);

        return new RunHandle(run);
    }

    /// <summary>Renders a failed validation report as the message of the exception that refuses the graph.</summary>
    /// <param name="report">The report, which is known to carry at least one diagnostic.</param>
    /// <returns>The message.</returns>
    /// <remarks>
    /// Every diagnostic appears, in the report's own deterministic order, as its stable rule identifier,
    /// its subject when it names one, and its message. The count is formatted with the invariant culture so
    /// that the text does not change with the ambient culture.
    /// </remarks>
    private static string Describe(GraphValidationReport report)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The graph does not validate against the local stage catalog and cannot be materialized. {report.Diagnostics.Count} diagnostic");

        if (report.Diagnostics.Count != 1)
        {
            message.Append('s');
        }

        message.Append(':');

        foreach (GraphValidationDiagnostic diagnostic in report.Diagnostics)
        {
            message.Append(CultureInfo.InvariantCulture, $" [{diagnostic.Rule}]");

            if (diagnostic.Subject is { } subject)
            {
                message.Append(CultureInfo.InvariantCulture, $" {subject}:");
            }

            message.Append(CultureInfo.InvariantCulture, $" {diagnostic.Message}");
        }

        return message.ToString();
    }
}
