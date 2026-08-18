using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The bridge from a host's public stage factory to the engine's internal runtime-factory seam.
/// </summary>
/// <param name="factory">The registered factory.</param>
/// <remarks>
/// <para>
/// The whole of what the public shape costs, and it is one unwrap. The engine's executor vocabulary is
/// internal — publishing it would make every engine refactor a breaking change for every provider — so a
/// provider states its stage in the public mirror of it and this takes the mirror off. Nothing is
/// translated, because the two shapes are the same cases by construction.
/// </para>
/// <para>
/// It lives here rather than in either host so that a silo and an in-process host unwrap identically. Two
/// copies of one unwrap is exactly the kind of duplication that lets two hosts drift into accepting
/// different things from one provider.
/// </para>
/// </remarks>
internal sealed class DataflowStageFactoryAdapter(IDataflowStageFactory factory) : IStageRuntimeFactory
{
    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">
    /// The factory answered with nothing, which says neither that it built the stage nor why it could not.
    /// </exception>
    public StageRuntime Create(StageRuntimeRequest request)
    {
        DataflowStageRuntime built = factory.Create(
            new DataflowStageRequest(request.Node, request.Specification)) ??
            throw new InvalidOperationException(
                $"The {nameof(IDataflowStageFactory)} registered for the provider '{request.Node.Stage.Provider}' returned nothing for the node '{request.Node.Id}', an occurrence of '{request.Node.Stage}'. A factory either builds the stage or says why it cannot by throwing; a null runtime says neither.");

        return built.Runtime;
    }
}
