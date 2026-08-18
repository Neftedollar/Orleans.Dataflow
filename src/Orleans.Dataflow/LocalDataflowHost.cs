using System.Globalization;
using System.Text;
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
    /// <param name="configure">The registration of this host's .NET push adapters.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configure"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="configure"/> registered one name twice.</exception>
    /// <remarks>
    /// <para>
    /// The local half of "declare once, use twice". The very bindings a silo is given are given to this
    /// host, so one declaration serves both runtimes and a graph written against them runs in either — the
    /// runtime-factory seam's own claim, made checkable in a process with no cluster in it.
    /// </para>
    /// <para>
    /// The registrations are checked here, so a broken one stops the host from being constructed rather
    /// than surfacing at the first graph. What they resolve to is one immutable catalog and one immutable
    /// factory registry, shared by every graph this host materializes.
    /// </para>
    /// </remarks>
    public LocalDataflowHost(Action<IDotnetDataflowBuilder> configure)
        : this(TimeProvider.System, configure)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LocalDataflowHost"/> class with a clock and providers.</summary>
    /// <param name="timeProvider">The clock every run this host starts measures time by.</param>
    /// <param name="configure">The registration of this host's .NET push adapters.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="timeProvider"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="configure"/> registered one name twice.</exception>
    /// <remarks>
    /// The two options a host has, together. They are independent of each other: the clock reaches the local
    /// vocabulary's timing stages, and a registered stage receives the run's tokens and whatever its own
    /// provider gave it.
    /// </remarks>
    public LocalDataflowHost(TimeProvider timeProvider, Action<IDotnetDataflowBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(configure);

        _clock = timeProvider;

        DotnetRegistrations registrations = new();

        configure(registrations);
        registrations.Validate();

        if (!registrations.Any)
        {
            _catalog = LocalStageCatalog.Instance;
            _binder = StageRuntimeBinder.None;

            return;
        }

        _catalog = StageCatalog.Create(
            [.. LocalStageCatalog.Instance.Specifications, .. registrations.Specifications]);
        _binder = new StageRuntimeBinder(_catalog, new StageRuntimeRegistry([registrations.Factory]));
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
        LocalRun run = LocalRun.Start(plan, graph.Fingerprint, graph.AuthoringNonce, cancellationToken);

        return new ValueTask<RunHandle>(new RunHandle(run));
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
