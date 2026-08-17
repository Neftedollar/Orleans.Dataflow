using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Turns one resolved stage node into something the engine can execute.
/// </summary>
/// <remarks>
/// <para>
/// This is the other half of the provider boundary ADR 0001 draws. A catalog says which stages exist and
/// what their documents may say; a factory says what one of them does when a run reaches it. Keeping them
/// apart is what lets a document be validated in a process that cannot execute it — a compiler needs the
/// catalog and nothing else — and what stops graph data from ever naming code.
/// </para>
/// <para>
/// A factory is registered per <see cref="Orleans.Dataflow.Identity.ProviderId"/> and is asked for every
/// node of that provider, so it dispatches on <see cref="StageRuntimeRequest.Node"/>'s stage reference
/// itself. That is one registration per provider rather than one per stage, which is how a provider ships
/// a vocabulary rather than a pile of unrelated entries.
/// </para>
/// <para>
/// Implementations must be safe to call from any thread and must build fresh per-run state on every call:
/// the seam is invoked once per node per materialization, and two runs of one pipeline must share nothing
/// but what the provider deliberately shared.
/// </para>
/// </remarks>
internal interface IStageRuntimeFactory
{
    /// <summary>Builds the executable form of one node.</summary>
    /// <param name="request">The node as the document declares it, and the specification it resolved to.</param>
    /// <returns>The runtime, in one of the engine's four executable shapes.</returns>
    /// <remarks>
    /// <para>
    /// The payload has already been validated against the specification's parameter contract by the graph
    /// compiler before a run is planned, so an implementation reads it rather than re-checking it; what it
    /// may still refuse is a stage reference of its provider that this build does not implement, which it
    /// reports by throwing.
    /// </para>
    /// <para>
    /// An exception here fails materialization rather than the run: a pipeline whose stages cannot be
    /// built has not started, and reporting that as a start failure rather than as a run failure is what
    /// lets a caller tell "this deployment cannot run this graph" from "this graph ran and went wrong".
    /// </para>
    /// </remarks>
    StageRuntime Create(StageRuntimeRequest request);
}

/// <summary>
/// What a runtime factory is told about the node it is asked to build.
/// </summary>
/// <param name="Node">
/// The node as the document declares it: its identifier, its stage reference, and its validated parameter
/// payload.
/// </param>
/// <param name="Specification">
/// The specification the node's stage reference resolved to in the host's catalog, which is where the
/// port names and the result contract are.
/// </param>
/// <remarks>
/// Deliberately two values and no more. A factory receives no document, no other node, no run identity,
/// and no services beyond what it was constructed with, so a stage's behavior can never depend on which
/// graph it happens to be standing in — the same narrowness
/// <see cref="Orleans.Dataflow.Definition.IStageParameterValidator"/> has, for the same reason.
/// </remarks>
internal readonly record struct StageRuntimeRequest(StageNode Node, StageSpecification Specification);
