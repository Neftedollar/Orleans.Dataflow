using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// The bounds on everything a caller hands a coordinator, and the way out of the one that fills up.
/// </summary>
/// <remarks>
/// <para>
/// <b>A coordinator activation is one turn wide and serves a whole pipeline</b>, so every unbounded input it
/// accepts is a cost one caller imposes on every other caller of that pipeline. Two of the three bounds here
/// are about a single call — a document's bytes and its node count, both of which are decoded and compiled on
/// this activation's own turn, and <c>StartRunAsync</c> does not interleave because it issues epochs. The
/// third is about accumulation: the register of declared durable run identities keeps the whole document of
/// every name it holds, is rewritten as one state document on every declaration, and had no way to remove a
/// record at all — so a deployment that named durable runs after a tenant or a day grew a state document
/// until its storage provider refused it, after which the coordinator could not write at all and ordinary
/// starts of that pipeline stopped with it.
/// </para>
/// <para>
/// Every bound here is generous by construction — four mebibytes, ten thousand nodes, a thousand names — so
/// what these tests pin is not the numbers but that the refusals exist, name themselves, and in the one case
/// where a deployment can legitimately outgrow the bound, name the way out.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class CoordinatorLimitsTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that fails a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADocumentLargerThanACoordinatorDecodesIsRefusedBeforeItIsDecoded()
    {
        const string Pipeline = "limits-oversized";

        // Larger than the bound by one byte more than a rounding error, and deliberately not a valid document
        // at all: what is being pinned is that the length is measured before anything parses the bytes, so a
        // refusal that named the format instead of the size would mean the decode had already happened — and
        // the decode is the cost the bound exists to prevent.
        byte[] oversized = new byte[PipelineCoordinatorGrain.MaximumDocumentBytes + 1];

        Array.Fill(oversized, (byte)' ');

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Cluster.Client
                .GetGrain<IPipelineCoordinatorGrain>(Pipeline)
                .StartRunAsync(oversized));

        Assert.Contains(
            PipelineCoordinatorGrain.MaximumDocumentBytes.ToString("N0", CultureInfo.InvariantCulture),
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            oversized.Length.ToString("N0", CultureInfo.InvariantCulture),
            refused.Message,
            StringComparison.Ordinal);
        Assert.DoesNotContain("canonical serialization", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentDeclaringMoreNodesThanACoordinatorExecutesIsRefusedByCount()
    {
        const string Pipeline = "limits-node-count";

        // Under the byte bound and over the node bound, which is why both bounds exist: a compact encoding
        // carries far more nodes per byte than a verbose one, so neither number implies the other.
        byte[] document = LinearDocument(Pipeline, PipelineCoordinatorGrain.MaximumNodes + 1);

        Assert.True(
            document.Length <= PipelineCoordinatorGrain.MaximumDocumentBytes,
            $"the document is {document.Length:N0} bytes, so this test would be measuring the byte bound instead.");

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Cluster.Client
                .GetGrain<IPipelineCoordinatorGrain>(Pipeline)
                .StartRunAsync(document));

        Assert.Contains(
            PipelineCoordinatorGrain.MaximumNodes.ToString("N0", CultureInfo.InvariantCulture),
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains("nodes", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARefusalNamesTheFirstDiagnosticsAndCountsTheRestRatherThanSpellingOutAll()
    {
        const string Pipeline = "limits-refusal-size";
        const int Nodes = 400;

        // A document this silo's catalog knows nothing of, so validation has something to say about every one
        // of its nodes. The report is unbounded by design — one diagnostic per thing wrong with a document,
        // and a document is an input — and the refusal built from it used to be the whole report: a
        // 200,000-node document produced a 23,600,112-character exception message, serialized back to the
        // caller, which is a refusal larger than the thing being refused.
        byte[] document = LinearDocument(Pipeline, Nodes);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Cluster.Client
                .GetGrain<IPipelineCoordinatorGrain>(Pipeline)
                .StartRunAsync(document));

        // The count is honest and the text is bounded, which is the pair that matters: a caller reads how
        // much of the report it is holding rather than a list that silently stops.
        Assert.Contains("diagnostic", refused.Message, StringComparison.Ordinal);
        Assert.Contains(
            "more, which this message does not spell out",
            refused.Message,
            StringComparison.Ordinal);
        Assert.True(
            refused.Message.Length < 20_000,
            $"the refusal is {refused.Message.Length:N0} characters, so the diagnostics were not capped.");

        // And the cap is the one the runtime declares rather than a number this test invented.
        Assert.Contains(
            $"names the first {PipelineMaterializer.ReportedDiagnosticLimit}",
            refused.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARetirementClearsTheCheckpointRemovesTheRecordAndIsSafeToRepeat()
    {
        const string Pipeline = "limits-retire";
        const string Run = "retired";
        const string Log = "limits-retire";
        const string Halt = "limits-retire-halted";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(Pipeline, count: 4, Log, halt: Halt);

        OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        await TestSignals.Reached(Halt);

        Assert.True(cluster.Checkpoints.Holds(GraphId.Create(Pipeline), RunId.Create(Run)));
        Assert.Equal([1L, 2L, 3L, 4L], TestDeliveries.Of(Log));

        // The runbook operation: everything the identity holds is destroyed and the identity itself is given
        // up. A replacement takes a name forward onto another document; this is the one that makes room in a
        // register, which is why it is the remedy the cap's refusal names.
        Assert.True(await cluster.Host.RetireDurableRunAsync(Pipeline, Run, Token));

        Assert.False(cluster.Checkpoints.Holds(GraphId.Create(Pipeline), RunId.Create(Run)));
        Assert.Null(await Coordinator(Pipeline).ClaimDurableRunAsync(Run));

        // Idempotent, because a runbook step that has already been taken has to be safe to take again: what
        // the second call reports is that there was nothing left to retire, not that something went wrong.
        Assert.False(await cluster.Host.RetireDurableRunAsync(Pipeline, Run, Token));

        // And the name is free. Declaring it again is a first declaration rather than a resume — there is no
        // record and no checkpoint — so the run starts from the beginning, which is exactly what a name whose
        // history was destroyed should do.
        await using OrleansRunHandle fresh = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 8,
            "the run declared under the retired name started from the beginning");

        Assert.Equal([1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L], TestDeliveries.Of(Log));

        await handle.DisposeAsync();
    }

    [Fact]
    public async Task ARegisterThatIsFullRefusesANewNameAndNamesBothTheCapAndTheRemedy()
    {
        const string Pipeline = "limits-register-full";

        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling(Pipeline, count: 1);

        byte[] document = GraphDocumentSerializer.Serialize(pipeline.Document);
        IPipelineCoordinatorGrain coordinator = Coordinator(Pipeline);

        // Filled to the brim through the ordinary door, one deliberate name at a time. Nothing about this is
        // hostile: it is what a deployment that names a durable run per tenant does over a long enough
        // period, and what it used to reach was a state document the storage provider would eventually refuse
        // — after which this coordinator could not write at all and every start of this pipeline stopped.
        for (int index = 0; index < PipelineCoordinatorGrain.MaximumDurableRuns; index++)
        {
            _ = await coordinator.DeclareDurableRunAsync(
                document,
                new DurableRunDeclaration { RunId = $"n{index:D6}" });
        }

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => coordinator.DeclareDurableRunAsync(
                document,
                new DurableRunDeclaration { RunId = "one-too-many" }));

        Assert.Contains(
            PipelineCoordinatorGrain.MaximumDurableRuns.ToString("N0", CultureInfo.InvariantCulture),
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains(nameof(IPipelineCoordinatorGrain.RetireDurableRunAsync), refused.Message, StringComparison.Ordinal);
        Assert.Contains("one-too-many", refused.Message, StringComparison.Ordinal);

        // A name already in the register still declares, which is the property that keeps a full register
        // usable rather than merely refused: the runs already in it can be addressed, resumed and finished.
        _ = await coordinator.DeclareDurableRunAsync(
            document,
            new DurableRunDeclaration { RunId = "n000000" });

        // And the remedy works. One retirement makes room for exactly one name, which is what "retire the
        // identities that are finished with" means as an operation rather than as advice.
        Assert.True(await cluster.Host.RetireDurableRunAsync(Pipeline, "n000000", Token));

        PipelineRunTicket admitted = await coordinator.DeclareDurableRunAsync(
            document,
            new DurableRunDeclaration { RunId = "one-too-many" });

        Assert.Equal("one-too-many", admitted.RunId);
    }

    /// <summary>Addresses the coordinator of one pipeline.</summary>
    /// <param name="pipeline">The pipeline's identity.</param>
    /// <returns>The coordinator grain.</returns>
    private IPipelineCoordinatorGrain Coordinator(string pipeline) =>
        cluster.Cluster.Client.GetGrain<IPipelineCoordinatorGrain>(pipeline);

    /// <summary>Builds a canonical document of one chain of nodes no catalog here knows.</summary>
    /// <param name="graph">The pipeline identity the document declares.</param>
    /// <param name="nodes">How many nodes it declares.</param>
    /// <returns>The canonical bytes.</returns>
    /// <remarks>
    /// Written by hand rather than authored, which is the point: the authoring API cannot produce a document
    /// of ten thousand unknown stages, and a bound at the coordinator's edge is exactly a bound on documents
    /// the authoring API did not produce. The shape is the smallest thing the canonical reader accepts, so
    /// the node count and the byte count can be varied independently of each other.
    /// </remarks>
    private static byte[] LinearDocument(string graph, int nodes)
    {
        StringBuilder text = new();

        _ = text.Append("{\"formatVersion\":1,\"graphId\":\"").Append(graph)
            .Append("\",\"revision\":1,\"capabilities\":[],\"nodes\":[");

        for (int index = 0; index < nodes; index++)
        {
            if (index > 0)
            {
                _ = text.Append(',');
            }

            _ = text.Append("{\"nodeId\":\"n").Append(index.ToString("D5", CultureInfo.InvariantCulture))
                .Append("\",\"stageRef\":{\"providerId\":\"p\",\"stageId\":\"s\",\"majorVersion\":1},")
                .Append("\"parameterContract\":{\"contractId\":\"c\",\"majorVersion\":1},\"parameters\":{},")
                .Append("\"executionPolicyContract\":null,\"executionPolicy\":null}");
        }

        _ = text.Append("],\"edges\":[");

        for (int index = 0; index + 1 < nodes; index++)
        {
            if (index > 0)
            {
                _ = text.Append(',');
            }

            _ = text.Append("{\"from\":{\"nodeId\":\"n").Append(index.ToString("D5", CultureInfo.InvariantCulture))
                .Append("\",\"portId\":\"out\"},\"to\":{\"nodeId\":\"n")
                .Append((index + 1).ToString("D5", CultureInfo.InvariantCulture))
                .Append("\",\"portId\":\"in\"}}");
        }

        _ = text.Append("],\"resultSlots\":[]}");

        return Encoding.UTF8.GetBytes(text.ToString());
    }
}
