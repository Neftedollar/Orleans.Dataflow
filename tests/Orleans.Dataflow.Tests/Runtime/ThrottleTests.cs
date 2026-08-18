using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a throttle promises: which elements pass, when they pass, and what happens to the one the declared
/// rate has no budget for.
/// </summary>
/// <remarks>
/// The bucket is asserted through the moments elements are emitted at rather than through a count, because
/// a rate is a statement about moments: an operator that let every element through eventually would pass any
/// assertion about which ones arrived. Every test here reads the run's own clock at the sink, and the clock
/// moves only when the test moves it.
/// </remarks>
public sealed class ThrottleTests
{
    [Fact]
    public async Task ThrottlePacesAStreamToTheDeclaredRate()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // The bucket starts full, so the first element is not paced at all; every one after it waits for
        // the budget its own cost needs.
        await Reaches(() => observed.Count == 1, "the first element passing the throttle", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 2, "the second element passing the throttle", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await run.Completion;

        Assert.Equal(
            [(1, start), (2, start + Second), (3, start + (Second * 2))],
            observed);
    }

    [Fact]
    public async Task ThrottleAdmitsABurstUpToItsDeclaredMaximum()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second, MaximumBurst = 3 })
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await Reaches(() => observed.Count == 3, "the burst passing the throttle", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await run.Completion;

        // Three at once because the bucket holds three, and the fourth a whole period later because the
        // bucket is then empty: the burst is what an idle stream banks, not a rate of its own.
        Assert.Equal(
            [(1, start), (2, start), (3, start), (4, start + Second)],
            observed);
    }

    [Fact]
    public async Task ThrottleRefillsContinuouslyRatherThanInSteps()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Throttle(new ThrottleOptions { Elements = 2, Per = Second })
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await Reaches(() => observed.Count == 2, "the bucket emptying", TestToken);
        await clock.AdvanceAsync(1, TimeSpan.FromMilliseconds(500), TestToken);
        await run.Completion;

        // Two per second means one every half second and not two at each second's edge. A throttle that
        // refilled in steps would have held the third element for a whole period.
        Assert.Equal(
            [(1, start), (2, start), (3, start + TimeSpan.FromMilliseconds(500))],
            observed);
    }

    [Fact]
    public async Task AnEnforcingThrottleFailsTheRunOnTheFirstElementItHasNoBudgetFor()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second, Mode = ThrottleMode.Enforcing })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        RateLimitExceededException failure =
            await Assert.ThrowsAsync<RateLimitExceededException>(() => run.Completion);

        // Nothing waits and nothing is dropped: the element the rate had no budget for ends the run, with
        // the rate it broke in the message.
        Assert.Equal([1], observed);
        Assert.Contains("1 per 00:00:01", failure.Message, StringComparison.Ordinal);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task ThrottleChargesWhatTheCostFunctionAnswers()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.From([4, 4, 1])
            .Throttle(new ThrottleOptions { Elements = 4, Per = Second }, cost: value => value)
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await Reaches(() => observed.Count == 1, "the first element spending the whole bucket", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 2, "the second element spending it again", TestToken);
        await clock.AdvanceAsync(1, TimeSpan.FromMilliseconds(250), TestToken);
        await run.Completion;

        // Four units per second, charged by what each element is worth: one element of cost four spends the
        // whole second, and one of cost one spends a quarter of it.
        Assert.Equal(
            [(4, start), (4, start + Second), (1, start + Second + TimeSpan.FromMilliseconds(250))],
            observed);
    }

    [Theory]
    [InlineData(ThrottleMode.Shaping)]
    [InlineData(ThrottleMode.Enforcing)]
    public async Task AnElementCostingMoreThanTheBurstFailsTheRunInEitherMode(ThrottleMode mode)
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([5])
            .Throttle(
                new ThrottleOptions { Elements = 2, Per = Second, Mode = mode },
                cost: value => value)
            .To(s => s.Count(), "counted", out ResultSlot<long> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        RateLimitExceededException failure =
            await Assert.ThrowsAsync<RateLimitExceededException>(() => run.Completion);

        // A shaping throttle waits for budget, and this is the one element it cannot wait for: the bucket
        // is bounded below what the element costs, so waiting would be waiting forever.
        Assert.Contains("no amount of waiting", failure.Message, StringComparison.Ordinal);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task ANegativeCostFailsTheRunRatherThanGivingBudgetBack()
    {
        LocalDataflowHost host = Timed(out TestClock _);

        RunnableGraph graph = Source.From([-1])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second }, cost: value => value)
            .To(s => s.Count(), "counted", out ResultSlot<long> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("cost is zero or more", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnElementCostingNothingIsNotPacedAtAll()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<DateTimeOffset> observed = [];

        RunnableGraph graph = Source.From([0, 0, 0, 0])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second }, cost: value => value)
            .To(s => s.ForEach(_ => observed.Add(clock.GetUtcNow())));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Zero is a real cost and not a refusal: an element that costs nothing spends nothing and waits for
        // nothing, which is what a cost function is for.
        Assert.Equal([start, start, start, start], observed);
    }

    [Fact]
    public async Task AThrottleStartsFromTheRunAndNotFromItsFirstElement()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<DateTimeOffset> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
            .To(s => s.ForEach(_ => observed.Add(clock.GetUtcNow())));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        clock.Advance(Second * 5);

        await probe.EmitAsync(1, TestToken);
        await probe.EmitAsync(2, TestToken);
        await Reaches(() => observed.Count == 1, "the first element passing an idle throttle", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 2, "the second element passing the throttle", TestToken);

        probe.Complete();

        await run.Completion;

        // Two things at once, and they are the same statement. The accounting starts when the run does
        // rather than when the first element arrives, so an element arriving after five idle seconds is not
        // paced. And the bucket is capped at the declared burst, so those five seconds bank one element's
        // worth and not five: the second element waits a whole period.
        Assert.Equal([start + (Second * 5), start + (Second * 6)], observed);
    }
}
