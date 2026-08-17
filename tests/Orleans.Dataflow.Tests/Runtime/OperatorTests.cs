using System.Globalization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What each element operator promises: which elements come out, what it remembers between them, and what
/// it does about an empty stream, a failure, a cancellation, and a second run of the same graph.
/// </summary>
/// <remarks>
/// <para>
/// The delivered sequence is asserted rather than a count or a sum wherever the two could differ, because
/// an operator that emitted the right number of the wrong elements would pass a count. Where an operator
/// carries state between elements, the same graph is materialized twice and both runs are asserted, because
/// state that leaked from one run to the next is exactly what a single run cannot show.
/// </para>
/// <para>
/// Early completion is the subject of <see cref="EarlyCompletionTests"/> and is deliberately not restated
/// here; what these tests pin is which elements each operator emits before its stream ends.
/// </para>
/// </remarks>
public sealed class OperatorTests
{
    [Fact]
    public async Task ScanEmitsEveryIntermediateStateAndNeverTheSeed()
    {
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Scan(100L, (sum, value) => sum + value)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Three elements in, three states out, and the seed is not among them: the first thing downstream
        // sees is what the first element made of it.
        Assert.Equal([101L, 103L, 106L], observed);
    }

    [Fact]
    public async Task ScanOverAnEmptyStreamEmitsNothingAtAll()
    {
        List<long> observed = [];

        RunnableGraph graph = Source.Empty<int>()
            .Scan(100L, (sum, value) => sum + value)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task EveryRunOfAScanStartsFromTheSeedAgain()
    {
        List<long> first = [];
        List<long> second = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2))
            .Scan(0L, (sum, value) => sum + value)
            .To(s => s.ForEach(value => (first.Count < 2 ? first : second).Add(value)));

        await using (RunHandle one = await Host.MaterializeAsync(graph, TestToken))
        {
            await one.Completion;
        }

        await using (RunHandle two = await Host.MaterializeAsync(graph, TestToken))
        {
            await two.Completion;
        }

        Assert.Equal([1L, 3L], first);
        Assert.Equal([1L, 3L], second);
    }

    [Fact]
    public async Task AScanWhoseStateIsANullReferenceIsARunningStateLikeAnyOther()
    {
        // The seed is a value of the author's state type and may legitimately be null; a runtime that
        // treated a null state as "no state" would fail on the first element.
        List<string?> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Scan(
                (string?)null,
                (text, value) => text is null
                    ? value.ToString(CultureInfo.InvariantCulture)
                    : text + value.ToString(CultureInfo.InvariantCulture))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(["1", "12", "123"], observed);
    }

    [Fact]
    public async Task TakeDeliversExactlyItsCountFromTheFrontOfTheStream()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .Take(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task SkipDropsExactlyItsCountAndPassesEverythingAfterIt()
    {
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Source.From(elements)
            .Skip(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The skipped elements were still produced: skipping is not a way to avoid work upstream of it.
        Assert.Equal([3, 4, 5], observed);
        Assert.Equal(5, elements.Pulls);
    }

    [Theory]
    [InlineData(0, new[] { 1, 2, 3 })]
    [InlineData(3, new int[0])]
    [InlineData(9, new int[0])]
    public async Task SkipOfAnyCountKeepsTheTailItLeaves(int count, int[] expected)
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Skip(count)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(expected, observed);
    }

    [Fact]
    public async Task TakeWhileIsExclusiveAndTakeThroughIsInclusiveOverTheSamePredicate()
    {
        // Side by side, over one sequence and one predicate, because the whole difference between the two
        // is whether the element that ended the stream was delivered first.
        List<int> exclusive = [];
        List<int> inclusive = [];

        RunnableGraph stopsBefore = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .TakeWhile(value => value < 3)
            .To(s => s.ForEach(exclusive.Add));

        RunnableGraph stopsAfter = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .TakeThrough(value => value < 3)
            .To(s => s.ForEach(inclusive.Add));

        await using (RunHandle run = await Host.MaterializeAsync(stopsBefore, TestToken))
        {
            await run.Completion;
        }

        await using (RunHandle run = await Host.MaterializeAsync(stopsAfter, TestToken))
        {
            await run.Completion;
        }

        Assert.Equal([1, 2], exclusive);
        Assert.Equal([1, 2, 3], inclusive);
    }

    [Fact]
    public async Task ATakeWhileThatNeverRejectsAnythingEndsWithItsSource()
    {
        // The case a stage that ends streams must not get wrong: a predicate that always holds is not a
        // completion, and the run has to end the ordinary way, with everything delivered.
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);

        RunnableGraph graph = Source.From(elements)
            .TakeWhile(_ => true)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3, 4], observed);
        Assert.Equal(4, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ATakeThroughThatNeverRejectsAnythingEndsWithItsSourceToo()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .TakeThrough(_ => true)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task SkipWhilePassesEverythingAfterTheFirstElementItRejectsWhateverThePredicateSaysNext()
    {
        // The exclusive prefix rule, stated where a filter would behave differently: 1 and 2 come back
        // after the prefix ended, and a filter would have dropped them.
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 9, 1, 2, 8))
            .SkipWhile(value => value < 5)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([9, 1, 2, 8], observed);
    }

    [Fact]
    public async Task SkipWhileThatRejectsNothingPassesEverything()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .SkipWhile(_ => false)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task DistinctPassesTheFirstOccurrenceOfEveryElementAndDropsTheRepeats()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 1, 3, 2, 1))
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task DistinctFaultsAtTheElementOneKeyPastItsBound()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .Distinct(new DistinctOptions { MaxTrackedKeys = 3 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        TrackedKeyOverflowException overflow =
            await Assert.ThrowsAsync<TrackedKeyOverflowException>(() => run.Completion);

        Assert.Contains("3 keys", overflow.Message, StringComparison.Ordinal);

        // The three that fitted were delivered, and the one that did not is not delivered by a stage that
        // then failed on it.
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task DistinctSpendsNoCapacityOnARepeatedElement()
    {
        // A bound of two and six elements, four of which are repeats: a stage that counted elements rather
        // than keys would have failed at the third.
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 1, 1, 2, 2, 1))
            .Distinct(new DistinctOptions { MaxTrackedKeys = 2 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task DistinctComparesElementsWithTheElementTypesOwnEquality()
    {
        // The two keys differ as objects and are equal to EqualityComparer<T>.Default, which is what the
        // stage is documented to use. A stage that compared boxes by reference would emit both, and one
        // that called object.Equals would emit both as well, because this type's own override deliberately
        // disagrees with its IEquatable implementation.
        List<Key> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<Key>(new Key(1, "a"), new Key(1, "b")))
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([new Key(1, "a")], observed);
    }

    [Fact]
    public async Task EveryRunOfAStatefulOperatorStartsWithNothingRemembered()
    {
        // One graph, two runs, three stateful operators. State that survived a run would show up as a
        // second run that skipped nothing, deduplicated against the first, or took nothing at all.
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 1, 2, 3, 4))
            .Skip(1)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 4 })
            .Take(3)
            .To(s => s.ForEach(observed.Add));

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;
        }

        Assert.Equal([1, 2, 3], observed);

        observed.Clear();

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            await second.Completion;
        }

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task EveryNewSynchronousOperatorFusesIntoOneLoopWithOneElementInFlight()
    {
        // The checkpoint 1 bound restated for the operators added since: no boundary was written, so there
        // is no queue anywhere, whatever the chain does to its elements. Every element survives every
        // stage on purpose — the in-flight count is the number of elements the source handed over and the
        // terminal has not finished with, and an element a stage dropped would never be finished with.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6);
        Gate gate = new();

        RunnableGraph graph = Source.From(elements)
            .Scan(0L, (sum, value) => sum + value)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .SkipWhile(value => value < 0L)
            .TakeWhile(value => value < 1000L)
            .Take(6)
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        // Five stages between the source and the terminal, no boundary between any of them, and therefore
        // no queue: the source has been asked for one element and no more.
        Assert.Equal(1, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal(1, elements.PeakInFlight);
        Assert.Equal(6, elements.Pulls);
        Assert.Equal(1L + 3L + 6L + 10L + 15L + 21L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AScanThatThrowsFaultsTheRunWithTheExceptionItThrew()
    {
        InvalidOperationException failure = new("the scan refuses the second element");
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Scan(0L, (sum, value) => value == 2 ? throw failure : sum + value)
            .To(Sink.Ignore<long>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal(2, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Theory]
    [InlineData("take-while")]
    [InlineData("take-through")]
    [InlineData("skip-while")]
    public async Task APredicateThatThrowsFaultsTheRunWithTheExceptionItThrew(string operatorName)
    {
        InvalidOperationException failure = new("the predicate refuses");
        RecordingEnumerable<int> elements = new(1, 2, 3);
        Source<int> source = Source.From(elements);
        Func<int, bool> predicate = value => value == 2 ? throw failure : true;

        RunnableGraph graph = (operatorName switch
        {
            "take-while" => source.TakeWhile(predicate),
            "take-through" => source.TakeThrough(predicate),
            _ => source.SkipWhile(predicate),
        }).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal(2, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task CancellationStopsAChainOfTheNewOperatorsAndResolvesNothing()
    {
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        List<long> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);

        RunnableGraph graph = Source.From(elements)
            .Scan(0L, (sum, value) => sum + value)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .Take(4)
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;
        await cancellation.CancelAsync();

        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));

        Assert.Equal([1L], observed);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task DistinctOverAScanDeduplicatesTheStatesRatherThanTheElements()
    {
        // The composition worth pinning because it is the one that reads wrongly: the keys the distinct
        // stage remembers are running sums, so a repeated element that changes the sum passes and an
        // element that leaves it unchanged does not.
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 0, 2, 0, 3))
            .Scan(0L, (sum, value) => sum + value)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1L, 3L, 6L], observed);
    }

    [Fact]
    public async Task DistinctTreatsANullElementAsAnElementLikeAnyOther()
    {
        // Null is a value a stream can carry, so it is a key like any other: the first one passes and the
        // second is a repeat. A stage that could not hash or compare it would fail rather than deduplicate.
        List<string?> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<string?>(null, "a", null, "a"))
            .Distinct(new DistinctOptions { MaxTrackedKeys = 4 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([null, "a"], observed);
    }

    [Fact]
    public async Task AStageThatEndedTheStreamEndsItEvenWhenALaterStageDropsThatElement()
    {
        // The rule that has to survive the rest of the push: the take reached its bound on the second
        // element, the filter then dropped that element, and the stream is over all the same.
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Source.From(elements)
            .Take(2)
            .Where(value => value < 2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1], observed);
        Assert.Equal(2, elements.Pulls);
    }

    [Fact]
    public async Task AFlowCarryingAStatefulOperatorCountsSeparatelyInEveryPlaceItIsComposedInto()
    {
        // Two occurrences of one flow are two stages with two states, not one shared between them: the
        // second take sees the two elements the first one let through and takes both.
        Flow<int, int> firstTwo = Flow.For<int>().Take(2);
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .Via(firstTwo)
            .Via(firstTwo)
            .To(s => s.ForEach(observed.Add));

        Assert.Equal(2, graph.Document.Nodes.Count(node => node.Stage.Stage.Value == "take"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2], observed);
    }

    /// <summary>A key whose two notions of equality disagree, so that only one of them can pass a test.</summary>
    /// <param name="value">The part <see cref="EqualityComparer{T}.Default"/> compares.</param>
    /// <param name="tag">The part only the object-based override compares.</param>
    /// <remarks>
    /// Pathological on purpose. A type whose <see cref="IEquatable{T}"/> implementation and
    /// <see cref="object.Equals(object?)"/> override agree — every ordinary type — could not tell the two
    /// apart, and the claim under test is which of them a distinct stage uses.
    /// </remarks>
    private readonly struct Key(int value, string tag) : IEquatable<Key>
    {
        /// <summary>Gets the part the typed comparison uses.</summary>
        internal int Value { get; } = value;

        /// <summary>Gets the part the object-based comparison also uses.</summary>
        internal string Tag { get; } = tag;

        /// <summary>Compares two keys the way <see cref="EqualityComparer{T}.Default"/> does.</summary>
        /// <param name="left">The first key.</param>
        /// <param name="right">The second key.</param>
        /// <returns><see langword="true"/> when their values are equal.</returns>
        public static bool operator ==(Key left, Key right) => left.Equals(right);

        /// <summary>Compares two keys the way <see cref="EqualityComparer{T}.Default"/> does.</summary>
        /// <param name="left">The first key.</param>
        /// <param name="right">The second key.</param>
        /// <returns><see langword="true"/> when their values differ.</returns>
        public static bool operator !=(Key left, Key right) => !left.Equals(right);

        /// <inheritdoc/>
        public bool Equals(Key other) => Value == other.Value;

        /// <inheritdoc/>
        public override bool Equals(object? obj) =>
            obj is Key other && Value == other.Value && string.Equals(Tag, other.Tag, StringComparison.Ordinal);

        /// <inheritdoc/>
        public override int GetHashCode() => Value;
    }
}
