using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// Builds the executable form of every stage of one provider.
/// </summary>
/// <remarks>
/// <para>
/// A host registers one factory per provider, and the factory is asked for every node whose stage
/// reference names that provider. That is one registration per vocabulary rather than one per stage, which
/// is how a provider ships something coherent: its stages share a payload format, a connection, and a set
/// of options, and a deployment that registered half of them would discover the other half missing at the
/// first element rather than when the run is planned.
/// </para>
/// <para>
/// One interface, two hosts. A silo registers a factory through
/// <c>IOrleansDataflowBuilder.AddFactory</c> and an in-process host registers the same value through
/// <see cref="ILocalDataflowBuilder.AddFactory"/>, so a provider writes its vocabulary once and it runs in
/// either runtime.
/// </para>
/// <para>
/// The catalog and the factory are separate on purpose (ADR 0001). A catalog says which stages exist and
/// what their documents may say, and validating a document needs nothing else — which is what lets a
/// process that cannot execute a graph still check one. A factory says what a stage does, and only a host
/// that will run the graph needs it.
/// </para>
/// <para>
/// Implementations must be thread-safe and must build fresh per-run state on every call: the factory is
/// invoked once per node per materialization, and two runs of one pipeline must share nothing the provider
/// did not deliberately share.
/// </para>
/// </remarks>
public interface IDataflowStageFactory
{
    /// <summary>Builds the executable form of one node.</summary>
    /// <param name="request">The node as the document declares it, and the specification it resolved to.</param>
    /// <returns>The runtime, in one of the four executable shapes.</returns>
    /// <remarks>
    /// <para>
    /// The payload has already been validated against the specification's parameter contract before a run
    /// is planned, so an implementation reads it rather than re-checking it. What it may still refuse is a
    /// stage reference of its own provider that this build does not implement, which it reports by
    /// throwing.
    /// </para>
    /// <para>
    /// An exception here fails materialization rather than the run. A pipeline whose stages cannot be
    /// built has not started, and reporting that as a start failure is what lets a caller tell "this
    /// deployment cannot run this graph" from "this graph ran and went wrong".
    /// </para>
    /// <para>
    /// A junction is built from this same request, and the ports it is wired at come from
    /// <see cref="DataflowStageRequest.Specification"/> rather than from anything the factory says: a
    /// fan-out's legs and a fan-in's inputs are the specification's own output and input ports, in its own
    /// canonical order, so a factory cannot disagree with the catalog entry that published it.
    /// </para>
    /// </remarks>
    DataflowStageRuntime Create(DataflowStageRequest request);
}

/// <summary>
/// What a stage factory is told about the node it is asked to build.
/// </summary>
/// <param name="Node">
/// The node as the document declares it: its identifier, its stage reference, and its validated parameter
/// payload.
/// </param>
/// <param name="Specification">
/// The specification the node's stage reference resolved to in the host's catalog, which is where the port
/// names, the port order, and the result contract are.
/// </param>
/// <remarks>
/// Two values and no more. A factory receives no document, no sibling node, no run identity, and no
/// services beyond what it was constructed with, so a stage's behavior can never depend on which graph it
/// happens to be standing in.
/// </remarks>
public readonly record struct DataflowStageRequest(StageNode Node, StageSpecification Specification);
