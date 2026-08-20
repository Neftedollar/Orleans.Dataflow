using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a junction call rejects, and where it says the mistake was.
/// </summary>
/// <remarks>
/// <para>
/// Every junction argument is checked before anything is composed, so a rejected call leaves the program
/// exactly as it found it. The arity bounds are the interesting ones: a junction's arity is stated by its
/// edges rather than by a payload, so a call outside the declared port list would otherwise build a document
/// naming a port no specification declares — and be reported by the graph compiler, one layer too late and
/// with a diagnostic about ports instead of about the call.
/// </para>
/// <para>
/// The mistakes that are compile errors are not here and cannot be: a branch of the wrong element type, a
/// result-bearing sink with no slot name, and a fork rejoined by nothing are all refused by the compiler,
/// which is where ADR 0006 wanted them.
/// </para>
/// </remarks>
public sealed class JunctionArgumentTests
{
    [Fact]
    public void AFanOutRejectsNoBranchesAndOneBranch()
    {
        // One branch is a chain written the long way and none is a discarding sink. Both are refused rather
        // than composed into a junction that is not one.
        Branch<int> discard = Flow.For<int>().To(Sink.Ignore<int>());

        ArgumentException none = Assert.Throws<ArgumentException>(() => Source.From<int>([1]).BroadcastTo());
        ArgumentException one = Assert.Throws<ArgumentException>(() => Source.From<int>([1]).BalanceTo(discard));

        Assert.Equal("branches", none.ParamName);
        Assert.Equal("branches", one.ParamName);
        Assert.Contains("between 2 and 8 branches", one.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AFanOutRejectsMoreLegsThanTheJunctionDeclares()
    {
        Branch<int>[] branches = [.. Enumerable.Range(0, 9).Select(_ => Flow.For<int>().To(Sink.Ignore<int>()))];

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.From<int>([1]).BroadcastTo(branches));

        Assert.Equal("branches", rejected.ParamName);
        Assert.Contains("this call has 9", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryJunctionCallRejectsANullArgument()
    {
        Source<int> numbers = Source.From<int>([1]);
        Branch<int> discard = Flow.For<int>().To(Sink.Ignore<int>());
        Flow<int, int> identity = Flow.For<int>();

        // Every null here is cast rather than written bare, because a fan-out close has two overloads that a
        // bare null fits — the branch array of the unnamed one and the occurrence name of the named one.
        // Overload resolution reports that as a compile error and never as a wrong call, which is the price
        // of the one spelling that lets a closing junction be named at all; a real call writes a string
        // literal or a list and is unambiguous.
        Assert.Throws<ArgumentNullException>(() => numbers.BroadcastTo((Branch<int>[])null!));
        Assert.Throws<ArgumentNullException>(() => numbers.BalanceTo((Branch<int>[])null!));
        Assert.Throws<ArgumentNullException>(() => numbers.BroadcastTo("tee", (Branch<int>[])null!));
        Assert.Throws<ArgumentNullException>(() => numbers.BalanceTo("tee", (Branch<int>[])null!));
        Assert.Throws<ArgumentNullException>(() => numbers.BroadcastTo(discard, null!));
        Assert.Throws<ArgumentNullException>(() => numbers.PartitionTo(null!, discard, discard));
        Assert.Throws<ArgumentNullException>(() => numbers.PartitionTo(null!, "route", discard, discard));

        // A null occurrence name is refused by the same guard a null slot name is, and names its own
        // parameter rather than the array that follows it.
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentNullException>(
                () => numbers.BroadcastTo((string)null!, discard, discard)).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentNullException>(
                () => numbers.BalanceTo((string)null!, discard, discard)).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentNullException>(
                () => numbers.PartitionTo(static value => value, (string)null!, discard, discard)).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentNullException>(
                () => Source.From<(int Left, int Right)>([]).UnzipTo((string)null!, discard, discard)).ParamName);
        Assert.Equal(
            "occurrenceName",
            Assert.Throws<ArgumentNullException>(() => numbers.Named(null!)).ParamName);
        Assert.Throws<ArgumentNullException>(() => numbers.AlsoTo(null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Merge(null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Merge(numbers, null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Concat(null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Interleave(null!, 1));
        Assert.Throws<ArgumentNullException>(() => numbers.Zip<int>(null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Zip(numbers, (Func<int, int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => numbers.CombineLatest(numbers, (Func<int, int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Fork((Flow<int, int>)null!, identity));
        Assert.Throws<ArgumentNullException>(() => numbers.Fork(identity, (Flow<int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => numbers.ForkMerge(identity, (Flow<int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => numbers.Fork(identity, identity).Zip<int>(null!));
        Assert.Throws<ArgumentNullException>(
            () => Source.UnzipTo(null!, discard, Flow.For<int>().To(Sink.Ignore<int>())));
        Assert.Throws<ArgumentNullException>(
            () => Source.From<(int Left, int Right)>([]).UnzipTo(null!, discard));
        Assert.Throws<ArgumentNullException>(
            () => Source.From<(int Left, int Right)>([]).UnzipTo(discard, null!));
    }

    [Fact]
    public void AnInterleaveRejectsASegmentThatNeverAdvances()
    {
        ArgumentOutOfRangeException rejected = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.From<int>([1]).Interleave(Source.From<int>([2]), 0));

        Assert.Equal("segmentSize", rejected.ParamName);
    }

    [Fact]
    public void EveryBranchTerminationRejectsANullArgument()
    {
        Flow<int, int> identity = Flow.For<int>();

        Assert.Throws<ArgumentNullException>(() => identity.To((Sink<int>)null!));
        Assert.Throws<ArgumentNullException>(() => identity.To((Func<SinkFactory<int>, Sink<int>>)null!));
        Assert.Throws<ArgumentNullException>(
            () => identity.To((SinkWithResult<int, long>)null!, "counted", out ResultSlot<long> _));
        Assert.Throws<ArgumentNullException>(
            () => identity.To(
                (Func<SinkFactory<int>, SinkWithResult<int, long>>)null!,
                "counted",
                out ResultSlot<long> _));
        Assert.Throws<ArgumentNullException>(
            () => Flow.For<OrderDocument>().To(null!, "index-out", RegisteredFixtures.IndexParameters));
        Assert.Throws<ArgumentNullException>(
            () => Flow.For<OrderDocument>().To(
                null!,
                "count-out",
                RegisteredFixtures.CountParameters,
                "counted",
                out ResultSlot<long> _));
    }

    [Fact]
    public void ASinkFactoryThatChoosesNothingIsRejectedWithTheSpellingToUse()
    {
        ArgumentException resultless = Assert.Throws<ArgumentException>(
            () => Flow.For<int>().To(_ => (Sink<int>)null!));
        ArgumentException resulting = Assert.Throws<ArgumentException>(
            () => Flow.For<int>().To(_ => (SinkWithResult<int, long>)null!, "counted", out ResultSlot<long> _));

        Assert.Equal("sink", resultless.ParamName);
        Assert.Equal("sink", resulting.ParamName);
        Assert.Contains("s => s.Ignore()", resultless.Message, StringComparison.Ordinal);
        Assert.Contains("s => s.Aggregate(seed, folder)", resulting.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionCallOnARegisteredBranchRejectsTheSameNamesTheChainDoes()
    {
        // The registered overloads are the chain's rules on a branch: an occurrence name is a node
        // identifier segment, a slot name is a result slot identifier, and neither may be blank.
        Assert.Throws<ArgumentException>(
            () => Flow.For<OrderDocument>().To(
                RegisteredFixtures.IndexSink,
                "Not A Node Id",
                RegisteredFixtures.IndexParameters));

        Assert.Throws<ArgumentException>(
            () => Flow.For<OrderDocument>().To(
                RegisteredFixtures.CountSink,
                "count-out",
                RegisteredFixtures.CountParameters,
                "Not A Slot Name",
                out ResultSlot<long> _));
    }
}
