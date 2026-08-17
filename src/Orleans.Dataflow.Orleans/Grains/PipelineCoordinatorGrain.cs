using System.Globalization;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Runtime;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// The coordinator grain: one activation per pipeline, ordering its runs and issuing their ownership.
/// </summary>
/// <remarks>
/// <para>
/// Everything a start has to be sure of happens here, in one order, before anything is created: the bytes
/// are a canonical document, the document is this pipeline's, every stage resolves in this silo's catalog,
/// and every provider named has a factory to build it. A caller therefore learns that a deployment cannot
/// run a graph before a run identity exists, which is what makes "refused" and "failed" two different
/// outcomes rather than two spellings of one.
/// </para>
/// <para>
/// The epoch is issued from persisted state, which is the fencing. A superseded activation that reaches
/// the write hits the ETag conflict, is killed by the runtime, and the caller retries against the fresh
/// activation that read the truth. Nothing here implements that; it is what the state write already means.
/// </para>
/// </remarks>
internal sealed class PipelineCoordinatorGrain(
    [PersistentState("pipeline", OrleansDataflowStorage.CoordinatorProviderName)]
    IPersistentState<PipelineCoordinatorState> state,
    DataflowSiloRegistry registry) : Grain, IPipelineCoordinatorGrain
{
    /// <inheritdoc/>
    public async Task<PipelineRunTicket> StartRunAsync(byte[] canonicalDocument)
    {
        ArgumentNullException.ThrowIfNull(canonicalDocument);

        GraphDocument document = Read(canonicalDocument);
        string pipeline = this.GetPrimaryKeyString();

        if (!GraphId.TryCreate(pipeline, out GraphId addressed) || document.Id != addressed)
        {
            throw new PipelineRejectedException(
                $"The document declares the pipeline '{document.Id}' and this coordinator owns the pipeline '{pipeline}'. A coordinator starts runs of its own pipeline only, because the epochs it issues order claims to that pipeline and nothing else.");
        }

        Refuse(document);

        // Written before the run grain is told anything. The epoch is the claim, and a claim that was
        // handed out before it was recorded would survive a lost activation as a number nobody can
        // reproduce; recorded first, a start that fails afterwards costs an unused epoch and nothing else.
        state.State.LastEpoch++;

        long epoch = state.State.LastEpoch;
        RunId run = RunId.Create(Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
        GraphFingerprint fingerprint = GraphFingerprint.OfSerialized(canonicalDocument);

        state.State.Runs.Add(new PipelineRunRecord
        {
            RunId = run.Value,
            Epoch = epoch,
            GraphFingerprint = fingerprint.ToString(),
            StartedAt = DateTimeOffset.UtcNow,
        });

        await state.WriteStateAsync();

        await GrainFactory
            .GetGrain<IPipelineRunGrain>(RunKey(document.Id, run))
            .StartAsync(canonicalDocument, epoch);

        return new PipelineRunTicket
        {
            GraphId = document.Id.Value,
            RunId = run.Value,
            Epoch = epoch,
            GraphFingerprint = fingerprint.ToString(),
            CatalogFingerprint = registry.CatalogFingerprint.ToString(),
        };
    }

    /// <inheritdoc/>
    public Task<RunStatusSnapshot> GetStatusAsync(string runId, long epoch) => Run(runId).GetStatusAsync(epoch);

    /// <inheritdoc/>
    public Task ShutdownRunAsync(string runId, long epoch) => Run(runId).ShutdownAsync(epoch);

    /// <inheritdoc/>
    public Task CancelRunAsync(string runId, long epoch) => Run(runId).CancelAsync(epoch);

    /// <summary>Composes the key one run grain is addressed by.</summary>
    /// <param name="graph">The pipeline the run belongs to.</param>
    /// <param name="run">The run's identity.</param>
    /// <returns>The composed key.</returns>
    /// <remarks>
    /// A run is a run <em>of</em> a pipeline, so both halves are in the key. The separator is a slash,
    /// which is not a character of the identifier grammar, so the two halves can never be confused for one
    /// and the key can never be ambiguous.
    /// </remarks>
    internal static string RunKey(GraphId graph, RunId run) => $"{graph.Value}/{run.Value}";

    /// <summary>Decodes the document a caller sent.</summary>
    /// <param name="canonicalDocument">The bytes.</param>
    /// <returns>The document.</returns>
    /// <exception cref="PipelineRejectedException">The bytes are not a canonical graph document.</exception>
    private static GraphDocument Read(byte[] canonicalDocument)
    {
        try
        {
            return GraphDocumentSerializer.Deserialize(canonicalDocument);
        }
        catch (GraphDocumentFormatException malformed)
        {
            // The inner exception is deliberately dropped rather than attached. Orleans serializes an
            // exception's whole chain, and this one has no codec, so attaching it would replace the
            // diagnosis with a codec error on the caller's side. The message is what carries it across.
            throw new PipelineRejectedException(
                $"The bytes are not the canonical serialization of a graph document: {malformed.Message}");
        }
    }

    /// <summary>Refuses a document this silo cannot validate or cannot build.</summary>
    /// <param name="document">The decoded document.</param>
    /// <exception cref="PipelineRejectedException">
    /// The document does not validate against this silo's catalog, or a provider it names has no
    /// registered runtime factory.
    /// </exception>
    /// <remarks>
    /// The two checks are separate because they fail for different reasons and a deployment fixes them
    /// differently: an unknown stage is a document this silo's vocabulary does not contain, and a missing
    /// factory is a vocabulary this silo published but cannot execute. Both are reported in full — every
    /// diagnostic, every missing provider — because a caller reconciling a document with a deployment
    /// needs the whole list.
    /// </remarks>
    private void Refuse(GraphDocument document)
    {
        GraphValidationReport report = GraphCompiler.Validate(document, registry.Catalog);

        if (!report.IsValid)
        {
            throw new PipelineRejectedException(PipelineMaterializer.Describe(report));
        }

        List<string> unbuildable = [];

        foreach (StageNode node in document.Nodes)
        {
            if (!registry.Factories.TryGetFactory(node.Stage.Provider, out _))
            {
                unbuildable.Add($"'{node.Id}' is an occurrence of '{node.Stage}'");
            }
        }

        if (unbuildable.Count > 0)
        {
            throw new PipelineRejectedException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"This silo's catalog knows every stage of the document, but it registers no runtime factory for {unbuildable.Count} of its {document.Nodes.Count} nodes, so it could validate the document and not execute it: {string.Join("; ", unbuildable)}. The providers it can build are {(registry.Factories.Providers.Count == 0 ? "none" : string.Join(", ", registry.Factories.Providers))}."));
        }
    }

    /// <summary>Addresses one run of this pipeline.</summary>
    /// <param name="runId">The run identity from its ticket.</param>
    /// <returns>The run grain.</returns>
    /// <exception cref="ArgumentException"><paramref name="runId"/> is not a valid run identifier.</exception>
    private IPipelineRunGrain Run(string runId)
    {
        ArgumentNullException.ThrowIfNull(runId);

        if (!RunId.TryCreate(runId, out RunId run))
        {
            throw new ArgumentException(
                $"'{runId}' is not a valid run identifier, so it names no run this coordinator could have started.",
                nameof(runId));
        }

        return GrainFactory.GetGrain<IPipelineRunGrain>(RunKey(GraphId.Create(this.GetPrimaryKeyString()), run));
    }
}
