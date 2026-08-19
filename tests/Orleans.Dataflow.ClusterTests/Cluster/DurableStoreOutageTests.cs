using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// A checkpoint store that can be told to stop answering, the way a real one does: transiently.
/// </summary>
/// <remarks>
/// <para>
/// The one failure mode the suite had no instrument for. Every store in these tests either works or answers
/// <see cref="CheckpointConflictException"/>, and those are the two cases the runtime already told apart; a
/// store that times out is the third, and it is the one a deployment actually meets — a blob store is
/// unavailable for a second at a time, an authentication token expires, a quota is briefly exceeded. What it
/// has in common with neither of the other two is that it says <em>nothing about who owns the run</em>.
/// </para>
/// <para>
/// The refusal is a <see cref="TimeoutException"/> because that is the plainest thing a store raises that is
/// not this library's own type: what the runtime keys on is "not a conflict", so a test that used a bespoke
/// exception would be testing a list of type names rather than the rule.
/// </para>
/// </remarks>
internal sealed class OutageCheckpointStore : ICheckpointStore
{
    private readonly InMemoryCheckpointStore _healthy = new();
    private int _failing;
    private int _refusals;
    private int _writes;

    /// <summary>Gets how many writes this store has been asked for since it was last reset.</summary>
    internal int Writes => Volatile.Read(ref _writes);

    /// <summary>Gets how many of those it refused.</summary>
    internal int Refusals => Volatile.Read(ref _refusals);

    /// <summary>Gets the healthy store underneath, for the reads a test makes on its own behalf.</summary>
    internal InMemoryCheckpointStore Healthy => _healthy;

    /// <summary>Makes the next writes fail, and forgets what has happened so far.</summary>
    /// <param name="writes">How many further writes refuse before the store recovers.</param>
    /// <remarks>
    /// Counters are reset here rather than in a fixture teardown so that every arrangement in a test starts
    /// from a number the test itself named. The store is shared by a collection, so a count that carried over
    /// would make one test's assertion depend on another's arithmetic.
    /// </remarks>
    internal void Fail(int writes)
    {
        Volatile.Write(ref _writes, 0);
        Volatile.Write(ref _refusals, 0);
        Volatile.Write(ref _failing, writes);
    }

    /// <inheritdoc/>
    public ValueTask<StoredCheckpoint?> ReadAsync(
        GraphId graph,
        RunId run,
        CancellationToken cancellationToken = default) =>
        _healthy.ReadAsync(graph, run, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<string> WriteAsync(
        GraphId graph,
        RunId run,
        CanonicalJsonValue checkpoint,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        _ = Interlocked.Increment(ref _writes);

        if (Interlocked.Decrement(ref _failing) >= 0)
        {
            _ = Interlocked.Increment(ref _refusals);

            throw new TimeoutException("the checkpoint store did not answer");
        }

        return _healthy.WriteAsync(graph, run, checkpoint, expectedETag, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default) =>
        _healthy.ClearAsync(graph, run, expectedETag, cancellationToken);
}

/// <summary>
/// One in-process cluster whose checkpoint store can be made to stop answering.
/// </summary>
/// <remarks>
/// A fixture of its own rather than a switch on the shared one: a store that refuses writes would make every
/// other durable test in the collection a test about outages, and the arrangement here — arm, run, heal — is
/// stateful in a way a shared cluster's tests must not have to know about.
/// </remarks>
public sealed class DurableOutageCluster : IAsyncLifetime
{
    /// <summary>Gets the store this cluster's durable runs write to.</summary>
    /// <remarks>
    /// Static because the silo configuration delegate must not close over the fixture instance: a delegate
    /// that did would tie the silo's registration to the object graph a test collection happens to build,
    /// which is exactly the coupling the shared fixture avoids by the same means.
    /// </remarks>
    internal static OutageCheckpointStore Store { get; } = new();

    /// <summary>Gets the deployed cluster.</summary>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test here materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory())
                .UseCheckpointStore(_ => Store));
        });

        builder.ConfigureClientHost(client =>
            client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(10)));

        Cluster = builder.Build();

        await Cluster.DeployAsync();

        Host = Cluster.Client.ServiceProvider.GetRequiredService<OrleansDataflowHost>();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.DisposeAsync();
        }
    }
}

/// <summary>
/// The collection the store-outage tests share one cluster through.
/// </summary>
[CollectionDefinition(Name)]
public sealed class DurableOutageClusterCollectionDefinition : ICollectionFixture<DurableOutageCluster>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "orleans-dataflow-durable-outage";
}

/// <summary>
/// What a checkpoint store that stops answering does to the run writing to it, and what it must not do to
/// the run's declaration.
/// </summary>
/// <remarks>
/// <para>
/// <b>The three answers a store can give are three different facts and used to be two.</b> An accepted write
/// is progress. A <see cref="CheckpointConflictException"/> is somebody else owning the run, and the
/// documented consequence is that the stale writer dies at once rather than retrying — retrying would
/// overwrite the position a fresh attempt is building. Anything else is a store that did not answer, which
/// says nothing at all about ownership; treating it as the third case is what these tests pin.
/// </para>
/// <para>
/// <b>What made it worth fixing was the second half.</b> A capture that gave up faulted the run, the run
/// grain reported that as how the run <em>ended</em>, and the coordinator wrote it onto the declaration — so
/// a one-second store outage retired a durable run permanently, and the only documented way back was a
/// replacement, whose first act is to clear the very checkpoints the outage was about. A long pipeline paid
/// for a store hiccup with all of its progress, through an operator action that reads like recovery.
/// </para>
/// </remarks>
[Collection(DurableOutageClusterCollectionDefinition.Name)]
public sealed class DurableStoreOutageTests(DurableOutageCluster cluster)
{
    /// <summary>Gets the token that fails a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AStoreThatMissesOneWriteIsRetriedAndTheRunNeverNoticesTheOutage()
    {
        const string Log = "outage-absorbed";
        const string Run = "absorbed";

        TestDeliveries.Clear(Log);

        // One refused write, which is what a store that is briefly unavailable does and is far less than the
        // policy allows for. Armed before the run exists, so the very first capture is the one that meets it.
        DurableOutageCluster.Store.Fail(writes: 1);

        PipelineDefinition pipeline = TestPipelines.Recording("outage-absorbed", count: 8, Log);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        await Deadline.Within(handle.Completion, $"the run {handle.RunId} completed through the outage");

        // Nothing about the run changed: every element was delivered once, in order, and the store holds a
        // position. The refusal cost a wait inside one capture's hold and nothing else.
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L], TestDeliveries.Of(Log));
        Assert.Equal(1, DurableOutageCluster.Store.Refusals);
        Assert.True(
            DurableOutageCluster.Store.Writes > DurableOutageCluster.Store.Refusals,
            "the refused write was presented again rather than ending the run");

        // And the declaration says the run completed, which is what it should say: this attempt reached a
        // terminal state of its own, so the outage is invisible in the register as well as in the log.
        DurableRunClaim? claim = await Coordinator(pipeline).ClaimDurableRunAsync(Run);

        Assert.Equal(RunPhase.Completed, claim?.Outcome);
    }

    [Fact]
    public async Task AnOutageBeyondTheRetriesFailsTheAttemptAndLeavesTheDeclarationResumable()
    {
        const string Log = "outage-exhausted";
        const string Run = "exhausted";
        const string Gate = "outage-exhausted-gate";
        const string Halt = "outage-exhausted-halted";

        TestDeliveries.Clear(Log);

        // The gate is what makes the arrangement a rendezvous rather than a length of time: the run has to
        // have written a checkpoint before the store is taken away, so that "resumed" and "restarted" are
        // two visibly different sequences afterwards. Twelve elements at a capture every five with the gate
        // at the seventh is the crash suite's own arrangement, and it leaves the stored cursor at five —
        // a capture due at five completes once the sixth element has been produced, and the source then
        // parks at the seventh. The halt is the source's own way of saying it has reached the end of the
        // stream without ending, which is how a resumed attempt announces that it got there.
        PipelineDefinition pipeline = TestPipelines.Recording(
            "outage-exhausted",
            count: 12,
            Log,
            halt: Halt,
            gate: Gate,
            gateAt: 7);

        OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 5 },
            Token);

        long started = handle.Epoch;

        await TestSignals.Reached($"{Gate}-reached");

        Assert.Equal(5L, await StoredCursorAsync(pipeline, Run));

        // From here the store answers nothing at all, so the capture due at ten exhausts every attempt the
        // policy allows and the run is held for the whole of it — which is also why the source cannot race
        // past the gate to the end of the stream.
        DurableOutageCluster.Store.Fail(writes: int.MaxValue);

        TestSignals.Raise(Gate);

        PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        // The attempt fails, and it fails as itself: the caller learns that a store stopped answering and
        // which store exception said so, rather than a wrapped nothing.
        Assert.Equal(typeof(CheckpointWriteFailedException).FullName, failed.FailureType);
        Assert.Contains(typeof(TimeoutException).FullName!, failed.FailureMessage, StringComparison.Ordinal);
        Assert.Contains(Run, failed.FailureMessage, StringComparison.Ordinal);
        Assert.True(
            DurableOutageCluster.Store.Refusals >= 2,
            $"the capture presented its document {DurableOutageCluster.Store.Refusals} times, so it was not retried at all.");

        // The half that matters. Nothing has been written onto the declaration, so the run is not finished —
        // it has an attempt that stranded, which is a different fact and one a later attempt can act on.
        DurableRunClaim? claim = await Coordinator(pipeline).ClaimDurableRunAsync(Run);

        Assert.NotNull(claim);
        Assert.Null(claim!.Outcome);

        // And the position the store did accept is still there, untouched by the failed writes.
        Assert.Equal(5L, await StoredCursorAsync(pipeline, Run));

        // The store recovers, and the operator does the ordinary thing rather than the destructive one: the
        // same declaration again. Before this fix that answered with the retired run's failure forever, and
        // the only way past it cleared the checkpoint.
        DurableOutageCluster.Store.Fail(writes: 0);

        await using OrleansRunHandle again = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 5 },
            Token);

        // The halt is raised by the source after its last element, and only the continued attempt ever gets
        // there — the one that stranded stopped six elements short — so this waits for a fact rather than
        // for a length of time.
        await Deadline.Within(
            TestSignals.Reached(Halt),
            "the continued attempt reached the end of the stream");

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Contains(12L),
            "the continued attempt delivered the last element");

        IReadOnlyList<long> delivered = TestDeliveries.Of(Log);

        // Resumed and not restarted, stated as a sequence rather than as a total. The first element was
        // delivered exactly once, which a run starting from the beginning could not manage; the sixth was
        // delivered twice, which is precisely the window between the stored cursor and the attempt that
        // stranded. Both numbers are what the checkpoint says, so neither is a coincidence of timing.
        Assert.Equal(1, delivered.Count(element => element == 1L));
        Assert.Equal(2, delivered.Count(element => element == 6L));

        // The continuation is a fresh claim to the same run, exactly as a resume after a silo death is.
        Assert.True(
            again.Epoch > started,
            $"the continued attempt holds {again.Epoch} and the attempt it followed held {started}, so it took no fresh claim.");

        await again.ShutdownAsync();
        await Deadline.Within(again.Completion, $"the continued run {again.RunId} drained and completed");
    }

    [Fact]
    public async Task ARefusedWriteStillKillsTheAttemptOnItsFirstRefusalAndIsNotRetried()
    {
        const string Log = "outage-fenced";
        const string Run = "outage-fenced";
        const string Gate = "outage-fenced-gate";

        TestDeliveries.Clear(Log);

        // The contrast this file exists to draw. A conflict is the store saying somebody else owns the run,
        // so the answer is unchanged from M3: the stale writer dies at its first refusal. Retrying it would
        // be a superseded attempt insisting over the top of a fresh one.
        PipelineDefinition pipeline = TestPipelines.Recording(
            "outage-fenced",
            count: 12,
            Log,
            halt: "outage-fenced-halted",
            gate: Gate,
            gateAt: 7);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 5 },
            Token);

        await TestSignals.Reached($"{Gate}-reached");

        Assert.Equal(5L, await StoredCursorAsync(pipeline, Run));

        // Counters reset and the store left healthy: what the next capture meets is not an outage but the
        // state a second attempt of this run would have left behind.
        DurableOutageCluster.Store.Fail(writes: 0);
        DurableOutageCluster.Store.Healthy.Supersede(GraphId.Create(pipeline.Id.Value), RunId.Create(Run));

        TestSignals.Raise(Gate);

        PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        Assert.Equal(typeof(CheckpointConflictException).FullName, failed.FailureType);

        // One presentation and not five, which is what "not retried" means as a number rather than as a
        // length of time: the capture that met the conflict asked the store once.
        Assert.Equal(1, DurableOutageCluster.Store.Writes);
    }

    /// <summary>Addresses the coordinator of one pipeline.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <returns>The coordinator grain.</returns>
    private IPipelineCoordinatorGrain Coordinator(PipelineDefinition pipeline) =>
        cluster.Cluster.Client.GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value);

    /// <summary>Reads the cursor the store currently holds for one durable run.</summary>
    /// <param name="pipeline">The pipeline the run belongs to.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The stored position, or zero when the store holds nothing for that pair.</returns>
    /// <remarks>
    /// Asked of the store rather than of the run, for the reason the crash suite asks it of the store: every
    /// claim these tests make about a position is a claim about what was written down, and a number read off
    /// a live run would only say what that run believes.
    /// </remarks>
    private static async Task<long> StoredCursorAsync(PipelineDefinition pipeline, string run)
    {
        StoredCheckpoint? stored = await DurableOutageCluster.Store.ReadAsync(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(run),
            Token);

        if (stored is not { } held)
        {
            return 0L;
        }

        Assert.True(
            LocalCheckpointDocument.TryRead(
                held.Document,
                out LocalCheckpoint? checkpoint,
                out IReadOnlyList<string> violations),
            $"The stored checkpoint for '{run}' does not read: {string.Join("; ", violations)}.");

        Assert.Single(checkpoint!.Cursors);

        foreach (KeyValuePair<NodeId, CanonicalJsonValue> cursor in checkpoint.Cursors)
        {
            return cursor.Value.ToElement().GetProperty("index").GetInt64();
        }

        throw new InvalidOperationException(string.Create(
            CultureInfo.InvariantCulture,
            $"The checkpoint of '{run}' carries no cursor, which the assertion above has already refused."));
    }
}
