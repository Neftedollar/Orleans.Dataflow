using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the catalog does with a boundary payload the authoring API would never have written.
/// </summary>
/// <remarks>
/// <para>
/// Putting the options in the document is what makes these tests possible and necessary at once. A
/// capacity that lived only in a binding table could not be wrong in a document, and could not be checked
/// by anything; a capacity in a payload can be zero, can be a string, and can be missing, and each of
/// those has to be a diagnostic naming the node rather than a channel of some silently chosen size.
/// </para>
/// <para>
/// Every document here is hand-built, because every one of them is unreachable through the authoring API,
/// whose operators check their options before they build anything.
/// </para>
/// </remarks>
public sealed class BoundaryPayloadTests
{
    [Theory]
    [InlineData("""{"capacity":0,"overflowPolicy":"backpressure"}""", "is 0, and it is a positive integer")]
    [InlineData("""{"capacity":-4,"overflowPolicy":"fail"}""", "is -4, and it is a positive integer")]
    [InlineData("""{"capacity":"8","overflowPolicy":"fail"}""", "is a string, and it is a positive integer")]
    [InlineData("""{"overflowPolicy":"fail"}""", "the member 'capacity' is missing")]
    [InlineData("""{"capacity":3}""", "the member 'overflowPolicy' is missing")]
    [InlineData("""{"capacity":3,"overflowPolicy":"drop-everything"}""", "'drop-everything'")]
    [InlineData("""{"capacity":3,"overflowPolicy":7}""", "is a number, and it is one of five policy names")]
    [InlineData("""{"capacity":3,"overflowPolicy":"fail","spare":1}""", "'spare' is not one this stage declares")]
    [InlineData("""[]""", "the payload is an array")]
    [InlineData("""{"capacity":4294967296,"overflowPolicy":"fail"}""", "no greater than 2147483647")]
    public async Task ABufferPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Buffered(payload);

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("stage-2", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"maxConcurrency":0}""", "is 0, and it is a positive integer")]
    [InlineData("""{}""", "the member 'maxConcurrency' is missing")]
    [InlineData("""{"maxConcurrency":2,"ordered":true}""", "'ordered' is not one this stage declares")]
    [InlineData("""{"maxConcurrency":null}""", "is null, and it is a positive integer")]
    public async Task AParallelismPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Mapped(payload);

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AValidatorReportsEveryViolationOfOnePayloadRatherThanTheFirst()
    {
        GraphValidationReport report = GraphCompiler.Validate(
            Buffered("""{"capacity":0,"overflowPolicy":"louder","spare":1}""").Document,
            LocalStageCatalog.Instance);

        Assert.False(report.IsValid);
        Assert.Equal(3, report.Diagnostics.Count);
        Assert.All(report.Diagnostics, diagnostic => Assert.Equal("invalid-parameters", diagnostic.Rule));
        Assert.All(report.Diagnostics, diagnostic => Assert.Equal("stage-2", diagnostic.Subject));
    }

    [Fact]
    public async Task ABufferCarryingTheDelegateOnlyParameterContractIsAContractMismatch()
    {
        // The contract is checked before the payload is, so a payload written for another stage is
        // reported as the mismatch it is rather than as a shape complaint about a check it was never
        // meant for.
        RunnableGraph graph = Graph(
            Document(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "buffer"), Node("stage-3", "ignore")],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Buffer(new BufferOptions { Capacity = 1 })),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[parameter-contract-mismatch]", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnAsynchronousStageBoundToSomethingThatIsNotACallbackIsRefused()
    {
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "select-async", "local-parallelism-parameters", """{"maxConcurrency":1}"""),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.SelectAsync(
                    new ParallelismOptions { MaxConcurrency = 1 },
                    (Func<int, int>)(value => value))),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(
            "must be bound to a Func<TIn, CancellationToken, Task<TOut>>",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnAsynchronousCallbackThatProducesNoTaskFaultsTheRunRatherThanFailingObscurely()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2))
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 1 }, (value, _) => (Task<int>)null!)
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("returned no task", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>Builds a hand-made source-to-buffer-to-sink graph with the payload under test.</summary>
    /// <param name="payload">The buffer's parameter payload as JSON text.</param>
    /// <returns>The graph.</returns>
    private static RunnableGraph Buffered(string payload) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "buffer", "local-buffer-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Buffer(new BufferOptions { Capacity = 1 })),
                ("stage-3", LocalStageDescriptor.Ignore())));

    /// <summary>Builds a hand-made source-to-asynchronous-stage-to-sink graph with the payload under test.</summary>
    /// <param name="payload">The stage's parameter payload as JSON text.</param>
    /// <returns>The graph.</returns>
    private static RunnableGraph Mapped(string payload) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "select-async-unordered", "local-parallelism-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.SelectAsyncUnordered(
                    new ParallelismOptions { MaxConcurrency = 1 },
                    (Func<int, CancellationToken, Task<int>>)((value, _) => Task.FromResult(value)))),
                ("stage-3", LocalStageDescriptor.Ignore())));
}
