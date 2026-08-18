using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Serialization;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// Whether every type that crosses a grain boundary or reaches storage actually survives the crossing.
/// </summary>
/// <remarks>
/// <para>
/// Orleans has two failure modes for a type it cannot serialize and they have very different blast radii:
/// a member without <c>[Id]</c> is a hard, documented failure, and a type without
/// <c>[GenerateSerializer]</c> fails the first time an instance is actually sent — which may be long after
/// the code that introduced it shipped, and only on the path that sends it. A round-trip per type is the
/// only check that turns the second kind into the first.
/// </para>
/// <para>
/// The last test here is the other half of the same fact, stated as a contract rather than as a defect: a
/// result value is the author's own type and Orleans has to be able to serialize it, so a type that
/// cannot be is a documented refusal at first use. Nothing this library can do prevents it; what it can do
/// is say so and be tested saying so.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class WireContractTests(DataflowCluster cluster)
{
    [Fact]
    public void ATicketRoundTripsThroughTheSerializer()
    {
        PipelineRunTicket ticket = new()
        {
            GraphId = "orders",
            RunId = "abcdef0123456789abcdef0123456789",
            Epoch = 42L,
            GraphFingerprint = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            CatalogFingerprint = "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        };

        PipelineRunTicket round = RoundTrip(ticket);

        Assert.Equal(ticket.GraphId, round.GraphId);
        Assert.Equal(ticket.RunId, round.RunId);
        Assert.Equal(ticket.Epoch, round.Epoch);
        Assert.Equal(ticket.GraphFingerprint, round.GraphFingerprint);
        Assert.Equal(ticket.CatalogFingerprint, round.CatalogFingerprint);
    }

    [Fact]
    public void AStatusSnapshotRoundTripsThroughTheSerializer()
    {
        RunStatusSnapshot status = new()
        {
            Phase = RunPhase.Faulted,
            Epoch = 7L,
            FailureType = "System.InvalidOperationException",
            FailureMessage = "something went wrong",
        };

        RunStatusSnapshot round = RoundTrip(status);

        Assert.Equal(status.Phase, round.Phase);
        Assert.Equal(status.Epoch, round.Epoch);
        Assert.Equal(status.FailureType, round.FailureType);
        Assert.Equal(status.FailureMessage, round.FailureMessage);
    }

    [Fact]
    public void AResultEnvelopeRoundTripsThroughTheSerializerWithItsValue()
    {
        ResultEnvelope envelope = new()
        {
            Phase = RunPhase.Completed,
            HasValue = true,
            Value = 20L,
        };

        ResultEnvelope round = RoundTrip(envelope);

        Assert.Equal(RunPhase.Completed, round.Phase);
        Assert.True(round.HasValue);
        Assert.Equal(20L, round.Value);
    }

    [Fact]
    public void ANullResultIsCarriedAsAValueAndNotAsAnAbsence()
    {
        ResultEnvelope round = RoundTrip(new ResultEnvelope { Phase = RunPhase.Completed, HasValue = true });

        Assert.True(round.HasValue);
        Assert.Null(round.Value);
    }

    [Fact]
    public void TheCoordinatorStateRoundTripsItsEpochCounter()
    {
        PipelineCoordinatorState round = RoundTrip(new PipelineCoordinatorState { LastEpoch = 3L });

        Assert.Equal(3L, round.LastEpoch);
    }

    [Fact]
    public void EveryExceptionThatCrossesAGrainBoundaryRoundTripsWithItsOwnMembers()
    {
        PipelineFencingException fencing = RoundTrip(new PipelineFencingException(4L, 2L));

        Assert.Equal(4L, fencing.CurrentEpoch);
        Assert.Equal(2L, fencing.CallerEpoch);

        PipelineRunFailedException failed = RoundTrip(new PipelineRunFailedException("T", "m", "r"));

        Assert.Equal("T", failed.FailureType);
        Assert.Equal("m", failed.FailureMessage);
        Assert.Equal("r", failed.RunId);

        KeyedExecutionFailedException keyed = RoundTrip(
            new KeyedExecutionFailedException("orders/run/priced/key-1", "price", "T", "m"));

        Assert.Equal("orders/run/priced/key-1", keyed.Executor);
        Assert.Equal("price", keyed.Call);
        Assert.Equal("T", keyed.FailureType);
        Assert.Equal("m", keyed.FailureMessage);

        ResultTooLargeException oversized = RoundTrip(new ResultTooLargeException("total", 4096L, 512));

        Assert.Equal("total", oversized.SlotName);
        Assert.Equal(4096L, oversized.Bytes);
        Assert.Equal(512, oversized.MaximumBytes);

        Assert.Contains("refused", RoundTrip(new PipelineRejectedException("refused")).Message, StringComparison.Ordinal);
        Assert.Contains("lost", RoundTrip(new PipelineRunLostException("lost")).Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ARefusalCarryingAnUnserializableInnerExceptionWouldNotSurviveTheHop()
    {
        // The reason every grain-thrown refusal composes its message instead of chaining a cause: Orleans
        // serializes the whole chain, and this inner exception has no codec, so a refusal built this way
        // reaches a caller as a codec error rather than as a diagnosis. Asserted rather than assumed,
        // because the library's rule is only worth stating if the alternative really does break.
        PipelineRejectedException chained = new(
            "refused",
            new Serialization.GraphDocumentFormatException("malformed"));

        _ = Assert.ThrowsAny<Exception>(() => RoundTrip(chained));
    }

    [Fact]
    public void AResultValueOfATypeOrleansCannotSerializeIsRefusedAtFirstUse()
    {
        // The documented requirement, seen from the failing side: a result travels as the author's own
        // type, so it must satisfy Orleans serialization. A type that does not is refused when a value of
        // it is first sent — not when the pipeline was written, which nothing here could arrange.
        ResultEnvelope envelope = new()
        {
            Phase = RunPhase.Completed,
            HasValue = true,
            Value = new UnserializableResult(1),
        };

        _ = Assert.ThrowsAny<Exception>(() => RoundTrip(envelope));
    }

    [Fact]
    public void NoWireContractExposesAnIdentityValueOfTheDefinitionPlane()
    {
        // Identities are authoring-side values with no Orleans annotations, and they are deliberately kept
        // off the wire: a slot travels as a name and a fingerprint as text. This asserts the boundary
        // itself rather than any one signature, so a member added later that quietly re-introduces one is
        // caught here rather than at the first call that sends it.
        Type[] wire =
        [
            typeof(PipelineRunTicket),
            typeof(RunStatusSnapshot),
            typeof(ResultEnvelope),
            typeof(PipelineCoordinatorState),
        ];

        foreach (Type contract in wire)
        {
            foreach (System.Reflection.PropertyInfo member in contract.GetProperties())
            {
                Assert.False(
                    member.PropertyType.Assembly == typeof(GraphId).Assembly,
                    $"{contract.Name}.{member.Name} is a {member.PropertyType.Name} of the definition plane, which carries no Orleans serializer.");
            }
        }

        foreach (System.Reflection.MethodInfo member in typeof(IPipelineCoordinatorGrain)
            .GetMethods()
            .Concat(typeof(IPipelineRunGrain).GetMethods())
            .Concat(typeof(IReminderTriggerGrain).GetMethods())
            .Concat(typeof(IObserverBridgeGrain).GetMethods())
            .Concat(typeof(IKeyedExecutorGrain).GetMethods())
            .Concat(typeof(IDataflowPushReceiver).GetMethods()))
        {
            foreach (System.Reflection.ParameterInfo argument in member.GetParameters())
            {
                Assert.False(
                    argument.ParameterType.Assembly == typeof(GraphId).Assembly,
                    $"{member.DeclaringType!.Name}.{member.Name} takes a {argument.ParameterType.Name} of the definition plane.");
            }
        }
    }

    /// <summary>Sends a value through Orleans' serializer and reads it back.</summary>
    /// <typeparam name="T">The type being checked.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>The value as it arrives on the other side.</returns>
    /// <remarks>
    /// The cluster's own serializer, resolved from the client's services, so what is exercised is the
    /// configuration a real call would use rather than a serializer built for the test.
    /// </remarks>
    private T RoundTrip<T>(T value)
    {
        Serializer serializer = cluster.Cluster.Client.ServiceProvider.GetRequiredService<Serializer>();

        return serializer.Deserialize<T>(serializer.SerializeToArray(value));
    }

    /// <summary>A type with no Orleans serializer, standing in for an unprepared result type.</summary>
    /// <param name="Value">A member nothing reads; the type exists in order to be unserializable.</param>
    private sealed record class UnserializableResult(int Value);
}
