using System.Runtime.CompilerServices;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;

namespace Orleans.Dataflow.Samples;

/// <summary>
/// Builds the three stages of the sample vocabulary when a silo materializes a run of them.
/// </summary>
/// <remarks>
/// <para>
/// The runtime half of the seam whose definition half is <c>SampleVocabulary</c>. A document names a stage
/// and carries a payload; this is what turns that pair into something that runs. One factory per provider,
/// dispatching on the node's stage reference, which is the shape the published interface asks for and the
/// shape a real provider has.
/// </para>
/// <para>
/// Nothing here reads the graph, the run, or the cluster. A stage is handed its own node and answers with
/// its own behavior, which is what lets the same three stages be composed into a pipeline this factory has
/// never seen.
/// </para>
/// </remarks>
internal sealed class SampleStageFactory : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == SampleVocabulary.FeedStage)
        {
            int count = SampleVocabulary.ReadFeedCount(node.Parameters);

            return DataflowStageRuntime.Source(tokens => Orders(count, tokens));
        }

        if (node.Stage == SampleVocabulary.DiscountStage)
        {
            decimal percent = SampleVocabulary.ReadDiscountPercent(node.Parameters);

            // Constructed rather than written as a `with` expression over the mapping the other scenarios
            // use: these records are declared in F#, and an F# record carries no clone method for C#'s
            // `with` to call. Naming all four members is what the language leaves.
            return DataflowStageRuntime.Element(element =>
            {
                OrderEvent order = (OrderEvent)element!;

                return new OrderDocument(
                    order.Sequence,
                    order.OrderId,
                    order.Region,
                    order.Amount - (order.Amount * percent / 100m));
            });
        }

        if (node.Stage == SampleVocabulary.TallyStage)
        {
            decimal minimum = SampleVocabulary.ReadTallyMinimum(node.Parameters);

            // Every terminal is a fold, including the ones that look like something else. The seed is made
            // once per run rather than handed over as a value, so two runs of one pipeline never share it.
            return DataflowStageRuntime.Terminal(
                static () => 0L,
                (state, element) => ((OrderDocument)element!).Amount >= minimum ? (long)state! + 1L : state,
                finish: null,
                producesResult: true);
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }

    /// <summary>Emits the first orders of the sample feed.</summary>
    /// <param name="count">How many to emit.</param>
    /// <param name="tokens">The tokens of the run this enumeration belongs to.</param>
    /// <param name="cancellationToken">The enumeration's own token.</param>
    /// <returns>The orders.</returns>
    /// <remarks>
    /// The feed ends rather than parking, so the run this source is in completes on its own and the client
    /// watching it sees a completion rather than a shutdown. A source that never ended would be the shape a
    /// long-running deployment has, and would make this scenario about stopping runs instead.
    /// </remarks>
    private static async IAsyncEnumerable<object?> Orders(
        int count,
        DataflowRunTokens tokens,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = tokens;

        foreach (OrderEvent order in SampleOrders.Take(count))
        {
            cancellationToken.ThrowIfCancellationRequested();

            yield return order;

            await Task.Yield();
        }
    }
}
