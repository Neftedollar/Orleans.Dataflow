using System.Globalization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What flattening promises: which elements come out and in what order, how much of an inner sequence the
/// run ever holds, and what a stop does part way through one.
/// </summary>
/// <remarks>
/// <para>
/// This is the first shape of the vocabulary that answers one element with several, so the tests that matter
/// are the ones about the sequence rather than about the elements: the inner elements are pushed one at a
/// time and never collected, so a boundary below the stage paces the enumeration and a stop lands between
/// two inner elements rather than after the whole sequence. Both are asserted through a recording sequence,
/// which reports how far it was actually read.
/// </para>
/// <para>
/// Order is a function of the input alone here, which is what makes this concat-map rather than merge-map:
/// one inner sequence is read to its end before the next element is asked for.
/// </para>
/// </remarks>
public sealed class FlatteningTests
{
    [Fact]
    public async Task SelectManyFlattensEachSequenceInOrder()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .SelectMany(value => Enumerable.Repeat(value, value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // One inner sequence read to its end before the next element is asked for, so the order of the
        // result is a function of the input and of nothing else.
        Assert.Equal([1, 2, 2, 3, 3, 3], observed);
    }

    [Fact]
    public async Task AnEmptySequenceDropsItsElement()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Range(1, 6)
            .SelectMany(value => value % 2 == 0 ? [value] : Array.Empty<int>())
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Filtering is a special case of flattening rather than a second operator.
        Assert.Equal([2, 4, 6], observed);
    }

    [Fact]
    public async Task SelectManyChangesTheElementType()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.From([12, 34])
            .SelectMany(value => value
                .ToString(CultureInfo.InvariantCulture)
                .Select(digit => digit.ToString()))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(["1", "2", "3", "4"], observed);
    }

    [Fact]
    public async Task SelectManyOverAnEmptyStreamNeverCallsItsFunction()
    {
        int calls = 0;

        RunnableGraph graph = Source.Empty<int>()
            .SelectMany(value =>
            {
                calls++;

                return new[] { value };
            })
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task AFunctionAnsweringNullFailsTheRun()
    {
        RunnableGraph graph = Source.From([1])
            .SelectMany<int>(_ => null!)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Contains("empty sequence", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFailingFunctionFailsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("no sequence for this one");

        RunnableGraph graph = Source.From([1])
            .SelectMany<int>(_ => throw failure)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task AFailingInnerSequenceFailsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("the sequence broke");

        RunnableGraph graph = Source.From([1])
            .SelectMany(_ => Failing(failure))
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task ATakeBelowAFlatteningStageStopsInTheMiddleOfAnInnerSequence()
    {
        RecordingEnumerable<int> inner = new(1, 2, 3, 4, 5, 6, 7, 8);
        List<int> observed = [];

        RunnableGraph graph = Source.From([0])
            .SelectMany(_ => inner)
            .Take(3)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The inner sequence is enumerated element by element rather than collected, so the take ends the
        // stream part way through it and nothing after that is read at all.
        Assert.Equal([1, 2, 3], observed);
        Assert.Equal(3, inner.Pulls);
    }

    [Fact]
    public async Task AnEndlessInnerSequenceIsPacedByWhateverIsBelowIt()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([0])
            .SelectMany(_ => Endless())
            .Take(5)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The run does not disappear into the sequence: what is below the stage is what says when to stop.
        Assert.Equal([0, 1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AnInnerSequenceIsEnumeratedOncePerElementItCameFrom()
    {
        RecordingEnumerable<int> inner = new(1, 2, 3);

        RunnableGraph graph = Source.From([0, 0])
            .SelectMany(_ => inner)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The function answers a sequence per element and the run enumerates each answer once, so a
        // sequence an author reuses across elements is enumerated once per element rather than shared.
        Assert.Equal(2, inner.Enumerations);
        Assert.Equal(6, inner.Pulls);
    }

    [Fact]
    public async Task ACancelledRunAbandonsTheRestOfTheInnerSequence()
    {
        using CancellationTokenSource cancellation = new();
        List<int> observed = [];

        RunnableGraph graph = Source.From([0])
            .SelectMany(_ => Endless())
            .To(s => s.ForEach(value =>
            {
                observed.Add(value);

                if (value == 2)
                {
                    cancellation.Cancel();
                }
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        // The token is examined between two inner elements, exactly as it is between two elements of a
        // source, so the run stops inside the sequence rather than after it.
        Assert.True(observed.Count < 100, $"the run read {observed.Count} elements after being cancelled");
    }

    [Fact]
    public async Task PausingARunInsideAnInnerSequenceHoldsItThere()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([0])
            .SelectMany(_ => Endless())
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.PauseAsync(TestToken).WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        int held = observed.Count;

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        // The gate is examined between two inner elements, so a paused run holding a sequence takes no
        // further step through it.
        Assert.Equal(held, observed.Count);
        Assert.True(run.IsPaused);

        await run.ResumeAsync();
        await run.DisposeAsync();
    }

    [Fact]
    public async Task SelectManyComposesInsideAReusableFlow()
    {
        Flow<int, int> exploded = Flow.For<int>().SelectMany(value => new[] { value, -value });
        List<int> first = [];
        List<int> second = [];

        RunnableGraph one = Source.From([1, 2]).Via(exploded).To(s => s.ForEach(first.Add));
        RunnableGraph two = Source.From([3]).Via(exploded).To(s => s.ForEach(second.Add));

        await using (RunHandle run = await Host.MaterializeAsync(one, TestToken))
        {
            await run.Completion;
        }

        await using (RunHandle run = await Host.MaterializeAsync(two, TestToken))
        {
            await run.Completion;
        }

        Assert.Equal([1, -1, 2, -2], first);
        Assert.Equal([3, -3], second);
    }

    [Fact]
    public async Task AProbeReadsThroughAFlatteningStageOneElementAtATime()
    {
        RunnableGraph graph = Source.From([1, 2])
            .SelectMany(value => new[] { value, value * 10 })
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);

        Assert.Equal(1, await sink.ReceiveAsync(TestToken));
        Assert.Equal(10, await sink.ReceiveAsync(TestToken));
        Assert.Equal(2, await sink.ReceiveAsync(TestToken));
        Assert.Equal(20, await sink.ReceiveAsync(TestToken));
        await sink.ExpectCompletedAsync(TestToken);

        await run.Completion;
    }

    /// <summary>A sequence that fails when it is read.</summary>
    /// <param name="failure">The exception it raises.</param>
    /// <returns>The sequence.</returns>
    private static IEnumerable<int> Failing(Exception failure)
    {
        yield return 1;

        throw failure;
    }

    /// <summary>A sequence that never ends.</summary>
    /// <returns>The sequence, counting up from zero.</returns>
    private static IEnumerable<int> Endless()
    {
        for (int value = 0; ; value++)
        {
            yield return value;
        }
    }
}
