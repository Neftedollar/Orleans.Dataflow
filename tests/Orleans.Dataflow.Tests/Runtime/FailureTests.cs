using System.Collections;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a failure anywhere in a run does to the run.
/// </summary>
/// <remarks>
/// The claim is the same for every stage and for the source itself: the run faults with the very exception
/// that was thrown, the results fault with it too, nothing downstream of the failure is reached, and the
/// enumerator is released anyway. Instance identity is asserted rather than type identity, because a
/// runtime that wrapped an author's exception would still pass a type check.
/// </remarks>
public sealed class FailureTests
{
    [Fact]
    public async Task AThrowingSelectorFaultsTheRunWithTheExceptionItThrew()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        InvalidOperationException failure = new("the selector refuses the third element");
        int highestSeen = 0;

        RunnableGraph graph = Source.From(elements)
            .Select(value =>
            {
                highestSeen = Math.Max(highestSeen, value);

                return value == 3 ? throw failure : value;
            })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal(TaskStatus.Faulted, run.Completion.Status);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));

        // The run stopped at the failing element: the fourth was never pulled and never seen.
        Assert.Equal(3, highestSeen);
        Assert.Equal(3, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AThrowingPredicateFaultsTheRunWithTheExceptionItThrew()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        InvalidOperationException failure = new("the predicate refuses");

        RunnableGraph graph = Source.From(elements)
            .Where(value => value == 2 ? throw failure : true)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
        Assert.Equal(2, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AThrowingFolderFaultsTheRunWithTheExceptionItThrew()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        InvalidOperationException failure = new("the folder refuses");

        RunnableGraph graph = Source.From(elements)
            .To(
                s => s.Aggregate(0L, (sum, value) => value == 2 ? throw failure : sum + value),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
        Assert.Equal(2, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ASourceThatThrowsWhilePullingFaultsTheRun()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3)
        {
            PullFailure = position => position == 1 ? new InvalidOperationException("the source refuses") : null,
        };

        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Equal("the source refuses", failure.Message);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
        Assert.Equal(1, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ASourceThatCannotBeEnumeratedFaultsTheRunAndReleasesNothing()
    {
        InvalidOperationException failure = new("the sequence refuses to be enumerated");
        RecordingEnumerable<int> elements = new(1, 2, 3) { EnumerationFailure = failure };

        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
        Assert.Equal(1, elements.Enumerations);

        // There is no enumerator to release when producing one is what failed.
        Assert.Equal(0, elements.Releases);
    }

    [Fact]
    public async Task ASourceThatThrowsWhileBeingReleasedFaultsAnOtherwiseSuccessfulRun()
    {
        InvalidOperationException failure = new("the sequence refuses to be released");
        RecordingEnumerable<int> elements = new(1, 2, 3) { ReleaseFailure = failure };

        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The fold reached its final state, and the run still fails: a release that throws is a failure of
        // the run, and the result is not handed out as though nothing had happened.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
        Assert.Equal(3, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ASourceThatThrowsWhileBeingReleasedDoesNotReplaceAFailureTheRunAlreadyHad()
    {
        InvalidOperationException stage = new("the selector refuses");
        RecordingEnumerable<int> elements = new(1, 2, 3)
        {
            ReleaseFailure = new InvalidOperationException("the sequence refuses to be released"),
        };

        RunnableGraph graph = Source.From(elements)
            .Select<int>(_ => throw stage)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(stage, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ASourceThatProducesNoEnumeratorFaultsTheRunRatherThanFailingObscurely()
    {
        RunnableGraph graph = Summing(new SequenceWithoutAnEnumerator(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("produced no enumerator", failure.Message, StringComparison.Ordinal);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
    }

    [Fact]
    public async Task AFailedRunFaultsEveryWaiterWithTheSameException()
    {
        Gate gate = new();
        InvalidOperationException failure = new("the folder refuses");

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(
                s => s.Aggregate<long>(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();

                        throw failure;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task<long> first = run.GetValueAsync(total, TestToken);
        Task<long> second = run.GetValueAsync(total, TestToken);

        gate.Open();

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => first));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => second));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    /// <summary>A sequence that claims to be enumerable and then hands back nothing to enumerate.</summary>
    /// <remarks>
    /// Contract-breaking on purpose. Without a check of its own the runtime would dereference the missing
    /// enumerator and report a null reference from inside its own loop, which says nothing about whose
    /// fault it is.
    /// </remarks>
    private sealed class SequenceWithoutAnEnumerator : IEnumerable<int>
    {
        /// <inheritdoc/>
        public IEnumerator<int> GetEnumerator() => null!;

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => null!;
    }
}
