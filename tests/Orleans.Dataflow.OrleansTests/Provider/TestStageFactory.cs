using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// Builds every stage of the test vocabulary.
/// </summary>
/// <remarks>
/// One factory for the whole provider, dispatching on the node's stage reference, which is the shape the
/// seam asks for and the shape a real provider has: a vocabulary ships together and is registered once.
/// </remarks>
internal sealed class TestStageFactory : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == TestVocabulary.Range)
        {
            if (!TestRangeParameters.TryRead(
                node.Parameters,
                out int count,
                out string? halt,
                out IReadOnlyList<string> violations))
            {
                throw new InvalidOperationException(
                    $"The range source '{node.Id}' carries parameters this provider cannot read: {string.Join("; ", violations)}.");
            }

            return DataflowStageRuntime.Source(tokens => Numbers(count, halt, tokens));
        }

        if (node.Stage == TestVocabulary.Double)
        {
            return DataflowStageRuntime.Element(static element => (long)element! * 2L);
        }

        if (node.Stage == TestVocabulary.Fail)
        {
            long at = FailAt(node);

            return DataflowStageRuntime.Element(element =>
                (long)element! == at
                    ? throw new InvalidOperationException(
                        string.Create(CultureInfo.InvariantCulture, $"the test flow was asked to fail at {at}"))
                    : element);
        }

        if (node.Stage == TestVocabulary.DoubleAsync)
        {
            return DataflowStageRuntime.ElementAsync(
                static (element, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    return ValueTask.FromResult<object?>((long)element! * 2L);
                },
                maxConcurrency: 2,
                ordered: true);
        }

        if (node.Stage == TestVocabulary.Collected)
        {
            // A mutable accumulator, which is what the seed factory exists for: a seed handed over as a
            // value would be one list two runs of this pipeline both appended to.
            return DataflowStageRuntime.Terminal(
                static () => new List<long>(),
                static (state, element) =>
                {
                    ((List<long>)state!).Add((long)element!);

                    return state;
                },
                static state => ((List<long>)state!).Sum(),
                producesResult: true);
        }

        if (node.Stage == TestVocabulary.Misplaced)
        {
            // A source where the document puts a flow. The catalog cannot catch this — a specification
            // describes ports and says nothing about what a factory will build — so the planner is what
            // has to, and this is the stage that proves it does.
            return DataflowStageRuntime.Source(static _ => Numbers(0, null, default));
        }

        if (node.Stage == TestVocabulary.Explode)
        {
            throw new InvalidOperationException(
                $"the test provider refuses to build '{node.Id}', which is what a provider does when a stage of its own vocabulary is one this build does not implement");
        }

        if (node.Stage == TestVocabulary.Sum)
        {
            return DataflowStageRuntime.Terminal(
                static () => 0L,
                static (state, element) => (long)state! + (long)element!,
                finish: null,
                producesResult: true);
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }

    /// <summary>Emits a run of consecutive numbers, and optionally waits instead of ending.</summary>
    /// <param name="count">How many numbers to emit, starting at one.</param>
    /// <param name="halt">The signal to raise after the last one, or <see langword="null"/> to end.</param>
    /// <param name="tokens">The tokens of the run this enumeration belongs to.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The halting form is what makes a drain provable. It emits exactly what it was asked for, says so,
    /// and then waits on the run's stop token: a graceful shutdown releases the wait and ends the sequence,
    /// so the sink has seen precisely those elements and the partial result is a number a test can name.
    /// A cancellation releases the same wait and raises instead, which is the other half of the same
    /// contract.
    /// </remarks>
    private static async IAsyncEnumerable<object?> Numbers(
        int count,
        string? halt,
        DataflowRunTokens tokens,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        for (long element = 1; element <= count; element++)
        {
            yield return element;
        }

        if (halt is null)
        {
            yield break;
        }

        TestSignals.Raise(halt);

        // Released by a graceful shutdown as well as by a cancellation, and the two are told apart
        // afterwards: a shutdown ends the sequence, a cancellation raises. That is the source half of the
        // drain-versus-abandon contract, written the way a real Orleans stream source would write it.
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, tokens.StopToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!tokens.RunToken.IsCancellationRequested)
        {
            yield break;
        }
    }

    /// <summary>Reads which element the failing flow was asked to fail at.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The element.</returns>
    private static long FailAt(StageNode node)
    {
        CanonicalJsonValue parameters = node.Parameters;

        if (parameters.IsDefault ||
            parameters.ToElement().ValueKind is not JsonValueKind.Object ||
            !parameters.ToElement().TryGetProperty(TestFailParameters.AtMember, out JsonElement at) ||
            !at.TryGetInt64(out long element))
        {
            throw new InvalidOperationException(
                $"The failing flow '{node.Id}' carries parameters this provider cannot read.");
        }

        return element;
    }
}
