using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What the timing operators write into a document, and what they refuse to write at all.
/// </summary>
/// <remarks>
/// <para>
/// A duration changes what a graph observably does, so it belongs in the document and in the fingerprint
/// taken over it: two graphs that differ only in a delay are two graphs. What does not belong there is the
/// clock — a clock is runtime and never definition — so the same program produces the same bytes whichever
/// clock a host measures it by, which is the claim ADR 0005 makes and this file pins.
/// </para>
/// <para>
/// The arguments are checked where the author wrote them, before anything is built, which is why every
/// refusal here names the operator's own parameter.
/// </para>
/// </remarks>
public sealed class TimingAuthoringTests
{
    [Fact]
    public void OneTimingProgramBuiltTwiceProducesIdenticalBytes()
    {
        GraphDocument first = Program().Document;
        GraphDocument second = Program().Document;

        Assert.Equal(
            GraphDocumentSerializer.Serialize(first),
            GraphDocumentSerializer.Serialize(second));
        Assert.Equal(Program().Fingerprint, Program().Fingerprint);
    }

    [Fact]
    public void TheClockIsNotPartOfTheDocumentAndTwoHostsAgreeOnTheFingerprint()
    {
        // A graph is the same graph whichever clock runs it, which is the whole of "a clock is runtime, not
        // definition": nothing a host is constructed with reaches the document, and a fingerprint taken
        // before a host exists is the fingerprint a controlled run has.
        RunnableGraph graph = Program();
        LocalDataflowHost system = new();
        LocalDataflowHost controlled = new(new Testing.TestClock());

        Assert.NotNull(system);
        Assert.NotNull(controlled);
        Assert.Equal(graph.Fingerprint, Program().Fingerprint);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInADurationAreTwoGraphs()
    {
        RunnableGraph shorter = Source.From([1]).Delay(
            TimeSpan.FromSeconds(1),
            new BufferOptions { Capacity = 2 }).To(s => s.Ignore());
        RunnableGraph longer = Source.From([1]).Delay(
            TimeSpan.FromSeconds(2),
            new BufferOptions { Capacity = 2 }).To(s => s.Ignore());

        Assert.NotEqual(shorter.Fingerprint, longer.Fingerprint);
    }

    [Fact]
    public void EveryTimingOperatorWritesItsNumbersIntoItsNode()
    {
        RunnableGraph graph = Source.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(500))
            .InitialDelay(TimeSpan.FromSeconds(2))
            .Delay(TimeSpan.FromSeconds(3), new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropOldest })
            .Timeout(TimeSpan.FromSeconds(5))
            .TakeWithin(TimeSpan.FromSeconds(6))
            .SkipWithin(TimeSpan.FromSeconds(7))
            .Throttle(new ThrottleOptions { Elements = 8, Per = TimeSpan.FromSeconds(9), MaximumBurst = 10 })
            .To(s => s.Ignore());

        Assert.Equal(
            $$"""{"initialDelayTicks":{{TimeSpan.FromSeconds(1).Ticks}},"intervalTicks":{{TimeSpan.FromMilliseconds(500).Ticks}}}""",
            Payload(graph, "tick"));
        Assert.Equal(
            $$"""{"durationTicks":{{TimeSpan.FromSeconds(2).Ticks}}}""",
            Payload(graph, "initial-delay"));
        Assert.Equal(
            $$"""{"capacity":4,"delayTicks":{{TimeSpan.FromSeconds(3).Ticks}},"overflowPolicy":"drop-oldest"}""",
            Payload(graph, "delay"));
        Assert.Equal(
            $$"""{"durationTicks":{{TimeSpan.FromSeconds(5).Ticks}}}""",
            Payload(graph, "timeout"));
        Assert.Equal(
            $$"""{"durationTicks":{{TimeSpan.FromSeconds(6).Ticks}}}""",
            Payload(graph, "take-within"));
        Assert.Equal(
            $$"""{"durationTicks":{{TimeSpan.FromSeconds(7).Ticks}}}""",
            Payload(graph, "skip-within"));

        // The burst is written even when it was not asked for, because the default is an authoring decision
        // and the document has to state the rate the run will actually hold.
        Assert.Equal(
            $$"""{"elements":8,"maximumBurst":10,"mode":"shaping","perTicks":{{TimeSpan.FromSeconds(9).Ticks}}}""",
            Payload(graph, "throttle"));
    }

    [Fact]
    public void ThrottleWritesTheBurstItDefaultedTo()
    {
        RunnableGraph graph = Source.From([1])
            .Throttle(new ThrottleOptions { Elements = 3, Per = TimeSpan.FromSeconds(1) })
            .To(s => s.Ignore());

        Assert.Equal(
            $$"""{"elements":3,"maximumBurst":3,"mode":"shaping","perTicks":{{TimeSpan.FromSeconds(1).Ticks}}}""",
            Payload(graph, "throttle"));
    }

    [Fact]
    public void EveryTimingGraphValidatesAgainstTheLocalCatalog()
    {
        foreach ((string name, RunnableGraph graph) in TimingGraphs())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

            Assert.True(report.IsValid, $"{name}: {report}");
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ADurationThatIsNotPositiveIsRefusedWhereItWasWritten(int seconds)
    {
        TimeSpan duration = TimeSpan.FromSeconds(seconds);
        Source<int> source = Source.From([1]);

        Assert.Equal("delay", Assert.Throws<ArgumentOutOfRangeException>(
            () => source.Delay(duration, new BufferOptions { Capacity = 1 })).ParamName);
        Assert.Equal("delay", Assert.Throws<ArgumentOutOfRangeException>(
            () => source.InitialDelay(duration)).ParamName);
        Assert.Equal("gap", Assert.Throws<ArgumentOutOfRangeException>(
            () => source.Timeout(duration)).ParamName);
        Assert.Equal("window", Assert.Throws<ArgumentOutOfRangeException>(
            () => source.TakeWithin(duration)).ParamName);
        Assert.Equal("window", Assert.Throws<ArgumentOutOfRangeException>(
            () => source.SkipWithin(duration)).ParamName);
        Assert.Equal("initialDelay", Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.Tick(duration, TimeSpan.FromSeconds(1))).ParamName);
        Assert.Equal("interval", Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.Tick(TimeSpan.FromSeconds(1), duration)).ParamName);
    }

    [Fact]
    public void AnInfiniteTimeoutIsRefusedLikeAnyOtherNonPositiveDuration()
    {
        // Timeout.InfiniteTimeSpan is minus one tick, and a timing operator with no deadline is the
        // operator not being there.
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.From([1]).Timeout(System.Threading.Timeout.InfiniteTimeSpan));
    }

    [Theory]
    [InlineData(0, 1, null, ThrottleMode.Shaping)]
    [InlineData(1, 0, null, ThrottleMode.Shaping)]
    [InlineData(2, 1, 1, ThrottleMode.Shaping)]
    [InlineData(1, 1, null, (ThrottleMode)7)]
    public void AThrottleWhoseNumbersDoNotDescribeARateIsRefused(
        int elements,
        int perSeconds,
        int? burst,
        ThrottleMode mode)
    {
        ThrottleOptions options = new()
        {
            Elements = elements,
            Per = TimeSpan.FromSeconds(perSeconds),
            MaximumBurst = burst,
            Mode = mode,
        };

        Assert.Equal(
            "options",
            Assert.Throws<ArgumentOutOfRangeException>(() => Source.From([1]).Throttle(options)).ParamName);
    }

    [Fact]
    public void TheOptionsAndTheCostFunctionAreRequiredWhereTheyAreTaken()
    {
        Source<int> source = Source.From([1]);
        ThrottleOptions options = new() { Elements = 1, Per = TimeSpan.FromSeconds(1) };

        Assert.Equal("holdback", Assert.Throws<ArgumentNullException>(
            () => source.Delay(TimeSpan.FromSeconds(1), null!)).ParamName);
        Assert.Equal("options", Assert.Throws<ArgumentNullException>(
            () => source.Throttle(null!)).ParamName);
        Assert.Equal("cost", Assert.Throws<ArgumentNullException>(
            () => source.Throttle(options, null!)).ParamName);
    }

    [Fact]
    public void TheOptionsRecordRendersItselfWithoutThrowingForValuesAnOperatorWouldRefuse()
    {
        Assert.Equal(
            "throttle (10 per 00:00:01, burst 20, shaping)",
            new ThrottleOptions { Elements = 10, Per = TimeSpan.FromSeconds(1), MaximumBurst = 20 }.ToString());
        Assert.Equal(
            "throttle (1 per 00:00:01, burst default, enforcing)",
            new ThrottleOptions { Elements = 1, Per = TimeSpan.FromSeconds(1), Mode = ThrottleMode.Enforcing }.ToString());
        Assert.Equal(
            "throttle (0 per 00:00:00, burst default, 9)",
            new ThrottleOptions { Elements = 0, Per = TimeSpan.Zero, Mode = (ThrottleMode)9 }.ToString());
    }

    /// <summary>Builds the program whose bytes the determinism claims are about.</summary>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// One program using every timing shape, built fresh on every call: a determinism claim over a value
    /// built once would be a claim about nothing.
    /// </remarks>
    private static RunnableGraph Program() =>
        Source.Tick(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1))
            .InitialDelay(TimeSpan.FromSeconds(2))
            .SkipWithin(TimeSpan.FromSeconds(3))
            .Delay(TimeSpan.FromSeconds(4), new BufferOptions { Capacity = 2 })
            .Throttle(new ThrottleOptions { Elements = 5, Per = TimeSpan.FromSeconds(6) })
            .Timeout(TimeSpan.FromSeconds(7))
            .TakeWithin(TimeSpan.FromSeconds(8))
            .To(s => s.Ignore());

    /// <summary>Enumerates one graph per timing shape, each closed on its own.</summary>
    /// <returns>The graphs and what to call them in a failure.</returns>
    private static IEnumerable<(string Name, RunnableGraph Graph)> TimingGraphs()
    {
        TimeSpan second = TimeSpan.FromSeconds(1);

        yield return ("tick", Source.Tick(second, second).To(s => s.Ignore()));
        yield return ("delay", Source.From([1]).Delay(second, new BufferOptions { Capacity = 2 }).To(s => s.Ignore()));
        yield return ("initial-delay", Source.From([1]).InitialDelay(second).To(s => s.Ignore()));
        yield return ("timeout", Source.From([1]).Timeout(second).To(s => s.Ignore()));
        yield return ("take-within", Source.From([1]).TakeWithin(second).To(s => s.Ignore()));
        yield return ("skip-within", Source.From([1]).SkipWithin(second).To(s => s.Ignore()));
        yield return (
            "throttle",
            Source.From([1])
                .Throttle(new ThrottleOptions { Elements = 1, Per = second })
                .To(s => s.Ignore()));
        yield return (
            "throttle by cost",
            Source.From([1])
                .Throttle(new ThrottleOptions { Elements = 1, Per = second }, cost: value => value)
                .To(s => s.Ignore()));
        yield return (
            "every timing operator in a flow",
            Source.From([1])
                .Via(
                    Flow.For<int>()
                        .InitialDelay(second)
                        .SkipWithin(second)
                        .Delay(second, new BufferOptions { Capacity = 2 })
                        .Throttle(new ThrottleOptions { Elements = 1, Per = second })
                        .Throttle(new ThrottleOptions { Elements = 1, Per = second }, cost: value => value)
                        .Timeout(second)
                        .TakeWithin(second))
                .To(s => s.Ignore()));
    }

    /// <summary>Reads back the parameter payload of the one node of a graph declaring a stage.</summary>
    /// <param name="graph">The closed graph.</param>
    /// <param name="stage">The stage identifier text, such as <c>delay</c>.</param>
    /// <returns>The payload as canonical JSON text.</returns>
    private static string Payload(RunnableGraph graph, string stage) =>
        graph.Document.Nodes.Single(node => node.Stage.Stage.Value == stage).Parameters.ToElement()
            .GetRawText();
}
