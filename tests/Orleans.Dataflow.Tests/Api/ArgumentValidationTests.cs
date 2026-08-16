using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The error paths a running program can actually reach.
/// </summary>
/// <remarks>
/// <para>
/// Most mistakes this API exists to prevent are compile errors — a sink of the wrong element type, a
/// dropped result, an unnamed slot — and a compile error cannot be a test in a passing suite. Their
/// evidence is the ADR 0004 compile prototypes with their verbatim diagnostics. What is left, and what is
/// asserted here, is the handful of failures that survive compilation: a null argument, a slot name that is
/// not a valid identifier, and a sink-factory lambda that returns nothing.
/// </para>
/// <para>
/// One error path the definition plane defines is deliberately absent: two result slots sharing a name in
/// one graph. A linear graph is closed by exactly one <c>To</c> and therefore declares at most one slot, so
/// the duplicate-slot violation is unreachable from this API. It becomes reachable when a graph can have
/// more than one sink, and belongs to that milestone's tests rather than to a test here that could only
/// build the document by hand and prove nothing about the authoring surface.
/// </para>
/// </remarks>
public sealed class ArgumentValidationTests
{
    [Fact]
    public void TheFactoriesRejectNullArguments()
    {
        Assert.Throws<ArgumentNullException>("elements", () => { _ = Source.From<int>(null!); });
        Assert.Throws<ArgumentNullException>("folder", () => { _ = Sink.Aggregate<int, long>(0L, null!); });
    }

    [Fact]
    public void TheSourceOperatorsRejectNullArguments()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);

        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = orders.Select<OrderDocument>(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = orders.Where(null!); });
        Assert.Throws<ArgumentNullException>("flow", () => { _ = orders.Via<OrderDocument>(null!); });
        Assert.Throws<ArgumentNullException>("sink", () => { _ = orders.To((Sink<OrderCreated>)null!); });
        Assert.Throws<ArgumentNullException>(
            "sink",
            () => { _ = orders.To((Func<SinkFactory<OrderCreated>, Sink<OrderCreated>>)null!); });
    }

    [Fact]
    public void TheFlowOperatorsRejectNullArguments()
    {
        Flow<OrderCreated, OrderCreated> flow = Flow.For<OrderCreated>();

        Assert.Throws<ArgumentNullException>("selector", () => { _ = flow.Select<OrderDocument>(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = flow.Where(null!); });
        Assert.Throws<ArgumentNullException>("flow", () => { _ = flow.Via<OrderDocument>(null!); });
    }

    [Fact]
    public void TheResultBearingOverloadsRejectANullSink()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);

        Assert.Throws<ArgumentNullException>(
            "sink",
            () => { _ = orders.To((SinkWithResult<OrderCreated, long>)null!, "processed"); });
        Assert.Throws<ArgumentNullException>(
            "sink",
            () => { _ = orders.To((SinkWithResult<OrderCreated, long>)null!, "processed", out ResultSlot<long> _); });
        Assert.Throws<ArgumentNullException>(
            "sink",
            () => { _ = orders.To((Func<SinkFactory<OrderCreated>, SinkWithResult<OrderCreated, long>>)null!, "processed"); });
        Assert.Throws<ArgumentNullException>(
            "sink",
            () =>
            {
                _ = orders.To(
                    (Func<SinkFactory<OrderCreated>, SinkWithResult<OrderCreated, long>>)null!,
                    "processed",
                    out ResultSlot<long> _);
            });
    }

    [Fact]
    public void TheExplicitConversionOfNullIsNull()
    {
        // A conversion converts; it is not the place an argument is checked. Null in, null out, the way
        // every conversion in .NET behaves, and the To overload that receives the result is what rejects
        // it — with the parameter name of the argument the author actually wrote.
        SinkWithResult<OrderCreated, long>? missing = null;

        Assert.Null((Sink<OrderCreated>?)missing);
        Assert.Throws<ArgumentNullException>(
            "sink",
            () => { _ = Source.From(OrderEvents).To(((Sink<OrderCreated>?)missing)!); });
    }

    [Fact]
    public void ASinkFactoryLambdaThatReturnsNothingIsRejected()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "sink",
            () =>
            {
                _ = Source.From(OrderEvents).To(
                    (Func<SinkFactory<OrderCreated>, SinkWithResult<OrderCreated, long>>)(_ => null!),
                    "processed",
                    out ResultSlot<long> _);
            });

        Assert.Contains("returned null", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANullSlotNameIsRejectedAgainstTheSlotNameArgument()
    {
        Assert.Throws<ArgumentNullException>(
            "slotName",
            () =>
            {
                _ = Source.From(OrderEvents).To(
                    s => s.Aggregate(0L, (count, _) => count + 1),
                    null!,
                    out ResultSlot<long> _);
            });
    }

    [Theory]
    [InlineData("")]
    [InlineData("Processed")]
    [InlineData("processed orders")]
    [InlineData("-processed")]
    [InlineData("processed-")]
    [InlineData("pro--cessed")]
    [InlineData("processed_orders")]
    [InlineData("orders/processed")]
    public void AnInvalidSlotNameSurfacesTheResultSlotIdGrammarErrorAgainstTheSlotNameArgument(string candidate)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "slotName",
            () =>
            {
                _ = Source.From(OrderEvents).To(
                    s => s.Aggregate(0L, (count, _) => count + 1),
                    candidate,
                    out ResultSlot<long> _);
            });

        // The grammar and its diagnostic belong to ResultSlotId; only the parameter name is this API's.
        Assert.Contains("is not a valid ResultSlotId", exception.Message, StringComparison.Ordinal);
        Assert.Contains($"'{candidate}'", exception.Message, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(exception.InnerException);
    }

    [Fact]
    public void AnInvalidSlotNameIsRejectedBeforeTheSinkFactoryLambdaRuns()
    {
        // The lambda is the author's own code and may do anything. An argument the call was always going
        // to reject must not cost them a side effect first, so the name is validated before the lambda is
        // invoked and a rejected call leaves the program exactly as it found it.
        int invocations = 0;

        Assert.Throws<ArgumentException>(
            "slotName",
            () => { _ = Source.From(OrderEvents).To(Counting, "Processed"); });

        Assert.Throws<ArgumentException>(
            "slotName",
            () => { _ = Source.From(OrderEvents).To(Counting, "Processed", out ResultSlot<long> _); });

        Assert.Throws<ArgumentNullException>(
            "slotName",
            () => { _ = Source.From(OrderEvents).To(Counting, null!); });

        Assert.Throws<ArgumentNullException>(
            "slotName",
            () => { _ = Source.From(OrderEvents).To(Counting, null!, out ResultSlot<long> _); });

        Assert.Equal(0, invocations);

        // The same lambda does run when the name is accepted, so the count above is a statement about the
        // rejection and not about a lambda that is never called at all.
        _ = Source.From(OrderEvents).To(Counting, "processed", out ResultSlot<long> _);

        Assert.Equal(1, invocations);

        SinkWithResult<OrderCreated, long> Counting(SinkFactory<OrderCreated> factory)
        {
            invocations++;

            return factory.Aggregate(0L, (count, _) => count + 1);
        }
    }

    [Fact]
    public void AnInvalidSlotNameIsRejectedByEveryResultBearingOverload()
    {
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        Source<OrderCreated> orders = Source.From(OrderEvents);

        Assert.Throws<ArgumentException>("slotName", () => { _ = orders.To(counting, "Processed"); });
        Assert.Throws<ArgumentException>(
            "slotName",
            () => { _ = orders.To(counting, "Processed", out ResultSlot<long> _); });
        Assert.Throws<ArgumentException>(
            "slotName",
            () => { _ = orders.To(s => s.Aggregate(0L, (count, _) => count + 1), "Processed"); });
        Assert.Throws<ArgumentException>(
            "slotName",
            () => { _ = orders.To(s => s.Aggregate(0L, (count, _) => count + 1), "Processed", out ResultSlot<long> _); });
    }
}
