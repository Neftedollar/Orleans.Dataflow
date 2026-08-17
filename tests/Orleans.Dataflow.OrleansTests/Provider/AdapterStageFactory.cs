using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// Builds the two stages that stand beside an Orleans adapter in these tests.
/// </summary>
/// <remarks>
/// A counting sink, so that a run over an adapter resolves a number a test can name, and a gate, so that a
/// test can hold a pipeline still while it does something to the world outside it. Both are ordinary
/// registered stages of an ordinary test provider; what makes them interesting is that their ports declare
/// the Orleans adapters' element contract, which is how a deployment's own vocabulary joins an adapter.
/// </remarks>
internal sealed class AdapterStageFactory : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == AdapterVocabulary.Count || node.Stage == AdapterVocabulary.DotnetCount)
        {
            JsonElement payload = node.Parameters.ToElement();
            string signal = payload.GetProperty("signal").GetString()!;
            int signalAt = payload.GetProperty("signalAt").GetInt32();

            return DataflowStageRuntime.Terminal(
                static () => 0L,
                (state, element) =>
                {
                    AdapterObservations.Counted.Enqueue(element);

                    long counted = (long)state! + 1L;

                    if (counted >= signalAt)
                    {
                        TestSignals.Raise(signal);
                    }

                    return counted;
                },
                finish: null,
                producesResult: true);
        }

        if (node.Stage == AdapterVocabulary.Gate)
        {
            JsonElement payload = node.Parameters.ToElement();
            string entered = payload.GetProperty("entered").GetString()!;
            string release = payload.GetProperty("release").GetString()!;

            return DataflowStageRuntime.Element(element =>
            {
                TestSignals.Raise(entered);

                // Blocking the segment's own thread, which is what that thread is for and what makes the
                // pipeline verifiably still: nothing behind this element moves until the test says so.
                TestSignals.Reached(release).GetAwaiter().GetResult();

                return element;
            });
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }
}
