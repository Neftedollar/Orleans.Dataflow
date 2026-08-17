using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Orleans.Dataflow.Compilation;
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
/// <b>Scope of this checkpoint.</b> The host executes the local, lambda-implemented vocabulary as one
/// linear chain. Adjacent synchronous stages fuse into one loop holding one element, and a queue exists
/// only where the author asked for one with a buffer or an asynchronous stage. Pause and resume, a
/// controllable clock, and every operator that would need one — windows, throttling, timeouts — are later
/// milestones and are absent here rather than approximated.
/// </para>
/// </remarks>
public sealed class LocalDataflowHost
{
    /// <summary>Initializes a new instance of the <see cref="LocalDataflowHost"/> class.</summary>
    /// <remarks>
    /// The host has nothing to configure yet. Options that change how a run behaves belong to the run, so
    /// they will arrive on materialization rather than here, where they would silently apply to graphs
    /// materialized long after they were chosen.
    /// </remarks>
    public LocalDataflowHost()
    {
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
    [SuppressMessage(
        "Performance",
        "CA1822:Mark members as static",
        Justification = "A host is an instance by contract: 'host.MaterializeAsync(graph, ct)' is the documented spelling, and the host is where the configuration a run is materialized under will live. A static method would fix that shape for good.")]
    public ValueTask<RunHandle> MaterializeAsync(RunnableGraph graph, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(graph);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

        if (!report.IsValid)
        {
            throw new InvalidOperationException(Describe(report));
        }

        LocalRunPlan plan = LocalRunPlanner.Compile(graph);
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
