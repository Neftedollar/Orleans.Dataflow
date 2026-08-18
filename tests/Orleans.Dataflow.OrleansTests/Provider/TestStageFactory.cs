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
                out string? gate,
                out int gateAt,
                out IReadOnlyList<string> violations))
            {
                throw new InvalidOperationException(
                    $"The range source '{node.Id}' carries parameters this provider cannot read: {string.Join("; ", violations)}.");
            }

            // The cluster twin of the local vocabulary's index cursor, and the crash suite's whole reason
            // for having one: a source that resumes from the top would make every element after a kill a
            // duplicate, so a window measured against it would be a statement about the source rather than
            // about the checkpoint. One cursor per occurrence per materialization, closed over by the
            // opener, which is the shape the seam asks for.
            CountingCursor counted = new();

            return DataflowStageRuntime.Source(
                tokens => Numbers(count, halt, gate, gateAt, counted, tokens),
                counted);
        }

        if (node.Stage == TestVocabulary.Record)
        {
            if (!TestRecordParameters.TryRead(
                node.Parameters,
                out string log,
                out IReadOnlyList<string> unreadable))
            {
                throw new InvalidOperationException(
                    $"The recording sink '{node.Id}' carries parameters this provider cannot read: {string.Join("; ", unreadable)}.");
            }

            return DataflowStageRuntime.Terminal(
                static () => null,
                (state, element) =>
                {
                    TestDeliveries.Record(log, (long)element!);

                    return state;
                },
                finish: null,
                producesResult: false);
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
            return DataflowStageRuntime.Source(
                static _ => Numbers(0, null, null, 0, new CountingCursor(), default));
        }

        if (node.Stage == TestVocabulary.Explode)
        {
            throw new InvalidOperationException(
                $"the test provider refuses to build '{node.Id}', which is what a provider does when a stage of its own vocabulary is one this build does not implement");
        }

        if (node.Stage == TestVocabulary.Split)
        {
            // A registered junction, built from the same seam every other stage of this provider is built
            // from. What it does is this provider's; where its legs go is the document's; what its ports are
            // called is the catalog's.
            return DataflowStageRuntime.Broadcast();
        }

        if (node.Stage == TestVocabulary.Bulk)
        {
            int bytes = BulkBytes(node);

            // A seed factory, so the block is this run's own: a result handed over as a shared array would
            // be one buffer two runs both claimed to have produced.
            return DataflowStageRuntime.Terminal(
                () => new byte[bytes],
                static (state, _) => state,
                finish: null,
                producesResult: true);
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
    /// <param name="gate">The signal to wait for partway, or <see langword="null"/> never to wait.</param>
    /// <param name="gateAt">Which element to wait before emitting, counting from one.</param>
    /// <param name="cursor">Where this source resumes from, and where it reports it has reached.</param>
    /// <param name="tokens">The tokens of the run this enumeration belongs to.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// <para>
    /// The halting form is what makes a drain provable. It emits exactly what it was asked for, says so,
    /// and then waits on the run's stop token: a graceful shutdown releases the wait and ends the sequence,
    /// so the sink has seen precisely those elements and the partial result is a number a test can name.
    /// A cancellation releases the same wait and raises instead, which is the other half of the same
    /// contract.
    /// </para>
    /// <para>
    /// A resumed run starts at the element after the one the cursor was restored to, which is this
    /// adapter's own promise and not the engine's: the numbers are generated rather than read from
    /// anywhere, so reopening at a position is arithmetic and the sequence is stable by construction. A real
    /// adapter earns the same promise from its own provider or declares no cursor.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<object?> Numbers(
        int count,
        string? halt,
        string? gate,
        int gateAt,
        CountingCursor cursor,
        DataflowRunTokens tokens,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        for (long element = cursor.Reached + 1; element <= count; element++)
        {
            if (gate is not null && element == gateAt)
            {
                TestSignals.Raise($"{gate}-reached");

                await TestSignals.Reached(gate).ConfigureAwait(false);
            }

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

    /// <summary>Reads how many bytes the bulk sink was asked to produce.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The size of the result, in bytes.</returns>
    private static int BulkBytes(StageNode node)
    {
        CanonicalJsonValue parameters = node.Parameters;

        if (parameters.IsDefault ||
            parameters.ToElement().ValueKind is not JsonValueKind.Object ||
            !parameters.ToElement().TryGetProperty(TestBulkParameters.BytesMember, out JsonElement bytes) ||
            !bytes.TryGetInt32(out int size))
        {
            throw new InvalidOperationException(
                $"The bulk sink '{node.Id}' carries parameters this provider cannot read.");
        }

        return size;
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

    /// <summary>The cursor of the test range source: how many of its numbers the run has delivered.</summary>
    /// <remarks>
    /// <para>
    /// The simplest position a source can have, and deliberately the same one the local vocabulary's
    /// <c>from-enumerable</c> declares, so that the cluster half of the cursor model is proved against a
    /// position whose arithmetic nobody has to argue about. What it is <em>not</em> is a stand-in for the
    /// real one: an Orleans stream's sequence token is the position ADR 0007 was designed around, and it
    /// lives in the adapter package with its own tests.
    /// </para>
    /// <para>
    /// <see cref="Delivered"/> is called by the run once an element has travelled through the segment it
    /// entered, so the number is what was delivered rather than what was produced — which is exactly the
    /// distinction that makes a stored position safe to resume after rather than one element optimistic.
    /// </para>
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
                    $"The checkpoint carries the position {position} for the test range source, whose position is an object with an 'index' member holding a count of zero or more delivered elements.");
            }

            _delivered = from;
        }
    }
}
