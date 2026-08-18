using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the tick source promises: when it emits, what it emits, and what it does when the consumer is
/// slower than the interval.
/// </summary>
/// <remarks>
/// The missed-tick contract is the interesting half and the one a real clock could not test at all: a tick
/// that comes due while the run is busy is skipped rather than queued, and the number of the next one says
/// how many were skipped. A controlled clock is what makes "the consumer was busy for three intervals" a
/// fact of the test rather than a hope about scheduling.
/// </remarks>
public sealed class TickSourceTests
{
    [Fact]
    public async Task TickEmitsItsNumbersAtItsIntervalAfterTheInitialDelay()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(long Number, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.Tick(Second, TimeSpan.FromMilliseconds(500))
            .Take(3)
            .To(s => s.ForEach(number => observed.Add((number, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 1, "the first tick", TestToken);
        await clock.AdvanceAsync(1, TimeSpan.FromMilliseconds(500), TestToken);
        await Reaches(() => observed.Count == 2, "the second tick", TestToken);
        await clock.AdvanceAsync(1, TimeSpan.FromMilliseconds(500), TestToken);

        await run.Completion;

        // Tick n is due at the initial delay plus n intervals, and the numbers start at zero: the first
        // element is the first tick and not a count of the ticks so far.
        Assert.Equal(
            [
                (0L, start + Second),
                (1L, start + Second + TimeSpan.FromMilliseconds(500)),
                (2L, start + Second + TimeSpan.FromSeconds(1)),
            ],
            observed);
    }

    [Fact]
    public async Task TickSkipsTheTicksASlowConsumerMissedAndItsNumbersSayHowMany()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<long> observed = [];
        Gate slow = new();

        RunnableGraph graph = Source.Tick(Second, Second)
            .Select(number =>
            {
                if (number == 1L)
                {
                    slow.Wait();
                }

                return number;
            })
            .Take(3)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 1, "the first tick", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);

        // The consumer is now inside the second tick and holding the run's own thread there, which is the
        // one state a test can hold a pull-paced source in without racing it: the source is not waiting on
        // the clock at all, so the four seconds that follow pass with the run standing still.
        await slow.Reached.WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        try
        {
            Assert.Equal(0, clock.PendingTimers);

            clock.Advance(Second * 4);
        }
        finally
        {
            // Opened whatever happens: a run left holding its own thread inside a gate could never be
            // disposed, and a failing assertion would hang the suite instead of reporting.
            slow.Open();
        }

        await Reaches(() => observed.Count == 3, "the tick after the slow consumer", TestToken);
        await run.Completion;

        // Ticks two, three and four came due while the consumer was busy, and they are gone rather than
        // queued: the next element is the tick that is due now, and its number says that three were missed.
        // A queue would have answered 2 here and grown by one per idle interval.
        Assert.Equal([0L, 1L, 5L], observed);
    }

    [Fact]
    public async Task AnExactlyPacedConsumerMissesNoTickAtAll()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<long> observed = [];

        RunnableGraph graph = Source.Tick(Second, Second)
            .Take(5)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        for (int tick = 1; tick <= 5; tick++)
        {
            await clock.AdvanceAsync(1, Second, TestToken);
            await Reaches(() => observed.Count == tick, $"tick {tick - 1}", TestToken);
        }

        await run.Completion;

        // The skipping rule is about whole intervals, so a consumer that keeps up loses nothing to it: five
        // ticks arrive as zero through four with no gap in the numbering.
        Assert.Equal([0L, 1L, 2L, 3L, 4L], observed);
    }

    [Fact]
    public async Task TwoRunsOfOneTickGraphTickIndependently()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.Tick(Second, Second)
            .Take(2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "ticks", out ResultSlot<IReadOnlyList<long>> ticks);

        await using RunHandle first = await host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await host.MaterializeAsync(graph, TestToken);

        // Two timers, one per run, waited for before each advance: a tick that came due while its own
        // source had not yet armed its next wait would be a missed tick, and missed ticks are skipped.
        await clock.AdvanceAsync(2, Second, TestToken);
        await Reaches(() => clock.PendingTimers == 2, "both sources waiting for their second tick", TestToken);

        clock.Advance(Second);

        await first.Completion;
        await second.Completion;

        // Two runs, two tick sources, two independent numberings starting at zero: a tick source belongs to
        // its run the way an enumerator does, and nothing is shared between them but the clock.
        Assert.Equal([0L, 1L], await first.GetValueAsync(ticks, TestToken));
        Assert.Equal([0L, 1L], await second.GetValueAsync(ticks, TestToken));
    }

    [Fact]
    public async Task ATickSourceIsBoundedByWhatIsWrittenBelowIt()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        List<long> observed = [];

        RunnableGraph graph = Source.Tick(Second, Second)
            .Select(number => number * 10L)
            .TakeWhile(number => number < 30L)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        for (int tick = 1; tick <= 3; tick++)
        {
            await clock.AdvanceAsync(1, Second, TestToken);
            await Reaches(() => observed.Count == tick, $"tick {tick - 1}", TestToken);
        }

        await clock.AdvanceAsync(1, Second, TestToken);
        await run.Completion;

        // An endless source ends where the author says it ends, exactly as an unfold does; nothing about
        // the tick source itself has an end in it.
        Assert.Equal([0L, 10L, 20L], observed);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }
}
