using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the checkpoint store contract promises: one document per run, and an ETag that refuses a superseded
/// writer rather than letting it overwrite the truth.
/// </summary>
/// <remarks>
/// <para>
/// The store is the coordinator's shape generalized, so these are the coordinator's own assertions read over
/// a checkpoint: a first write presents nothing, every later one presents what it last read, and a write
/// whose ETag has moved on is refused loudly. Nothing here runs a graph — a store is a store whether
/// anything is checkpointing into it or not, and proving that separately is what lets the run's own tests
/// assert about runs.
/// </para>
/// <para>
/// The implementation under test ships in the testing package, which is where ADR 0007 put it; a durable one
/// is the deployment's, exactly as the coordinator's is.
/// </para>
/// </remarks>
public sealed class CheckpointStoreTests
{
    private static readonly GraphId Graph = GraphId.Create("checkpoint-store-tests");

    [Fact]
    public async Task AStoreThatHoldsNothingAnswersNothingRatherThanFailing()
    {
        InMemoryCheckpointStore store = new();

        StoredCheckpoint? read = await store.ReadAsync(Graph, RunId.Create("absent"), TestToken);

        Assert.Null(read);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task AFirstWritePresentsNoETagAndIsAcceptedWithOne()
    {
        InMemoryCheckpointStore store = new();
        RunId run = RunId.Create("first");
        CanonicalJsonValue document = CanonicalJsonValue.Parse("""{"a":1}""");

        string etag = await store.WriteAsync(Graph, run, document, expectedETag: null, TestToken);
        StoredCheckpoint? read = await store.ReadAsync(Graph, run, TestToken);

        Assert.NotNull(read);
        Assert.Equal(etag, read.Value.ETag);
        Assert.Equal(document, read.Value.Document);
    }

    [Fact]
    public async Task AWritePresentingTheETagItReadIsAccepted()
    {
        InMemoryCheckpointStore store = new();
        RunId run = RunId.Create("sequence");

        string first = await store.WriteAsync(
            Graph,
            run,
            CanonicalJsonValue.Parse("""{"a":1}"""),
            expectedETag: null,
            TestToken);
        string second = await store.WriteAsync(
            Graph,
            run,
            CanonicalJsonValue.Parse("""{"a":2}"""),
            first,
            TestToken);

        StoredCheckpoint? read = await store.ReadAsync(Graph, run, TestToken);

        Assert.NotEqual(first, second);
        Assert.Equal(second, read!.Value.ETag);
        Assert.Equal(CanonicalJsonValue.Parse("""{"a":2}"""), read.Value.Document);
    }

    [Fact]
    public async Task AWritePresentingAStaleETagIsRefusedByNameAndChangesNothing()
    {
        InMemoryCheckpointStore store = new();
        RunId run = RunId.Create("superseded");
        CanonicalJsonValue kept = CanonicalJsonValue.Parse("""{"kept":true}""");

        string stale = await store.WriteAsync(Graph, run, kept, expectedETag: null, TestToken);

        store.Supersede(Graph, run);

        CheckpointConflictException refused = await Assert.ThrowsAsync<CheckpointConflictException>(
            async () => await store.WriteAsync(
                Graph,
                run,
                CanonicalJsonValue.Parse("""{"kept":false}"""),
                stale,
                TestToken));

        // Both ETags are on the exception, not only in its message: what a caller asserts on and what a
        // person reads in a log are two different needs.
        Assert.Equal(stale, refused.Presented);
        Assert.NotEqual(stale, refused.Stored);

        StoredCheckpoint? read = await store.ReadAsync(Graph, run, TestToken);

        Assert.Equal(kept, read!.Value.Document);
    }

    [Fact]
    public async Task AFirstWriteOverAStoreThatAlreadyHoldsTheRunIsRefused()
    {
        InMemoryCheckpointStore store = new();
        RunId run = RunId.Create("occupied");

        _ = await store.WriteAsync(
            Graph,
            run,
            CanonicalJsonValue.Parse("""{"a":1}"""),
            expectedETag: null,
            TestToken);

        // The case a fresh run started under a live run's identity produces, and the reason the host reads
        // nothing before starting one: the store is what refuses it, at the first capture, by name.
        CheckpointConflictException refused = await Assert.ThrowsAsync<CheckpointConflictException>(
            async () => await store.WriteAsync(
                Graph,
                run,
                CanonicalJsonValue.Parse("""{"a":2}"""),
                expectedETag: null,
                TestToken));

        Assert.Null(refused.Presented);
        Assert.NotNull(refused.Stored);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphAreTwoDocuments()
    {
        InMemoryCheckpointStore store = new();
        RunId first = RunId.Create("one");
        RunId second = RunId.Create("two");

        _ = await store.WriteAsync(
            Graph,
            first,
            CanonicalJsonValue.Parse("""{"which":1}"""),
            expectedETag: null,
            TestToken);
        _ = await store.WriteAsync(
            Graph,
            second,
            CanonicalJsonValue.Parse("""{"which":2}"""),
            expectedETag: null,
            TestToken);

        Assert.Equal(2, store.Count);
        Assert.Equal(
            CanonicalJsonValue.Parse("""{"which":1}"""),
            (await store.ReadAsync(Graph, first, TestToken))!.Value.Document);
    }

    [Fact]
    public async Task ClearingForgetsTheDocumentAndRefusesAStaleETag()
    {
        InMemoryCheckpointStore store = new();
        RunId run = RunId.Create("cleared");

        string etag = await store.WriteAsync(
            Graph,
            run,
            CanonicalJsonValue.Parse("""{"a":1}"""),
            expectedETag: null,
            TestToken);

        store.Supersede(Graph, run);

        _ = await Assert.ThrowsAsync<CheckpointConflictException>(
            async () => await store.ClearAsync(Graph, run, etag, TestToken));

        StoredCheckpoint? still = await store.ReadAsync(Graph, run, TestToken);

        await store.ClearAsync(Graph, run, still!.Value.ETag, TestToken);

        Assert.Null(await store.ReadAsync(Graph, run, TestToken));
        Assert.False(store.Holds(Graph, run));
    }

    [Fact]
    public async Task AStoredDocumentIsAValueRatherThanAReferenceTheWriterCanStillReach()
    {
        InMemoryCheckpointStore store = new();
        RunId run = RunId.Create("aliasing");
        CanonicalJsonValue written = CanonicalJsonValue.Parse("""{"b":2,"a":1}""");

        _ = await store.WriteAsync(Graph, run, written, expectedETag: null, TestToken);

        StoredCheckpoint? read = await store.ReadAsync(Graph, run, TestToken);

        // The round trip the coordinator store buys with a serializer, bought here by the payload type: a
        // canonical value holds its own bytes, so what came back is byte-identical to what went in and the
        // keys are in canonical order whatever order the writer spelled them in.
        Assert.Equal("""{"a":1,"b":2}""", read!.Value.Document.ToString());
        Assert.Equal(written, read.Value.Document);
    }
}
