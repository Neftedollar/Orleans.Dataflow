using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// Builds the two stages of the benchmark vocabulary.
/// </summary>
/// <remarks>
/// One factory for the provider, dispatching on the node's stage reference: the shape the published seam
/// asks for, and the shape a real provider has.
/// </remarks>
internal sealed class BenchmarkStageFactory : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == BenchmarkVocabulary.Range)
        {
            (long count, bool park) = BenchmarkVocabulary.ReadRange(node.Parameters);

            // One cursor per occurrence per materialization, closed over by the opener. Without it a
            // resumed attempt would start at the first element and every element after a kill would be a
            // replay, which would make the recovery number a statement about this source rather than about
            // the runtime that resumed it.
            CountingCursor cursor = new();

            return DataflowStageRuntime.Source(tokens => Numbers(count, park, cursor, tokens), cursor);
        }

        if (node.Stage == BenchmarkVocabulary.Record)
        {
            string log = BenchmarkVocabulary.ReadRecord(node.Parameters);

            return DataflowStageRuntime.Terminal(
                static () => null,
                (state, element) =>
                {
                    BenchmarkDeliveries.Record(log, (long)element!);

                    return state;
                },
                finish: null,
                producesResult: false);
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }

    /// <summary>Emits a run of consecutive numbers, and optionally waits instead of ending.</summary>
    /// <param name="count">How many numbers to emit, starting at one.</param>
    /// <param name="park">Whether to wait to be stopped after the last one instead of ending.</param>
    /// <param name="cursor">Where this source resumes from, and where it reports it has reached.</param>
    /// <param name="tokens">The tokens of the run this enumeration belongs to.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// <para>
    /// The parking form is what makes the recovery measurement deterministic. A source that ends races the
    /// harness that wants to kill a silo underneath a live run; a source that emits what it was asked for
    /// and then waits on the run's stop token is still alive at whatever moment the harness chooses, with a
    /// position already committed.
    /// </para>
    /// <para>
    /// A resumed run starts at the element after the one the cursor was restored to, which is this
    /// adapter's own promise: the numbers are generated rather than read from anywhere, so reopening at a
    /// position is arithmetic.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<object?> Numbers(
        long count,
        bool park,
        CountingCursor cursor,
        DataflowRunTokens tokens,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        for (long element = cursor.Reached + 1; element <= count; element++)
        {
            yield return element;
        }

        if (!park)
        {
            yield break;
        }

        // Released by a graceful shutdown as well as by a cancellation, and told apart afterwards the same
        // way every well-written source does it: a shutdown ends the sequence, a cancellation raises.
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, tokens.StopToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!tokens.RunToken.IsCancellationRequested)
        {
            yield break;
        }
    }

    /// <summary>The position of the benchmark range source: how many of its numbers the run has delivered.</summary>
    /// <remarks>
    /// <see cref="Delivered"/> is called once an element has travelled through the segment it entered, so
    /// the number is what was delivered rather than what was produced — which is what makes a stored
    /// position safe to resume after rather than one element optimistic.
    /// </remarks>
    private sealed class CountingCursor : DataflowSourceCursor
    {
        private long _delivered;

        /// <summary>Gets how many elements this source has delivered.</summary>
        internal long Reached => Interlocked.Read(ref _delivered);

        /// <inheritdoc/>
        public override CanonicalJsonValue Position =>
            CanonicalJsonValue.Parse(string.Create(
                CultureInfo.InvariantCulture,
                $"{{\"index\":{Interlocked.Read(ref _delivered)}}}"));

        /// <inheritdoc/>
        public override void Delivered() => _ = Interlocked.Increment(ref _delivered);

        /// <inheritdoc/>
        public override void RestoreTo(CanonicalJsonValue position)
        {
            if (position.IsDefault ||
                position.ToElement().ValueKind is not JsonValueKind.Object ||
                !position.ToElement().TryGetProperty("index", out JsonElement index) ||
                !index.TryGetInt64(out long from) ||
                from < 0)
            {
                throw new InvalidOperationException(
                    $"The checkpoint carries the position {position} for the benchmark range source, whose position is an object with an 'index' member holding a count of zero or more delivered elements.");
            }

            _delivered = from;
        }
    }
}
