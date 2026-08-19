using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// The registered vocabulary the recovery scenario deploys: a cursored source of numbers and a sink that
/// counts what it is handed.
/// </summary>
/// <remarks>
/// <para>
/// Two stages, because a recovery measurement needs exactly two things and no more: a source that can say
/// where it got to, so that a resumed attempt starts from a position rather than from the beginning, and a
/// terminal whose deliveries the harness can see from outside the cluster.
/// </para>
/// <para>
/// This is written here rather than borrowed from the Orleans test project deliberately. That project's
/// vocabulary is <see langword="internal"/> to it, and reaching into it would make the harness a second
/// consumer of a private surface; everything below is built out of what the library publishes, which also
/// makes the harness a working example of how a provider is registered. Where the two look alike — a
/// counting cursor keyed on <c>index</c>, above all — the resemblance is that they solve the same problem
/// in the only way the published seam offers.
/// </para>
/// </remarks>
internal static class BenchmarkVocabulary
{
    /// <summary>The provider every stage here belongs to.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create("benchmark");

    /// <summary>The source that emits a run of consecutive numbers and reports its position.</summary>
    internal static StageRef Range { get; } = StageRef.Create(Provider, StageId.Create("range"), 1);

    /// <summary>The sink that counts what it is handed and timestamps the deliveries the harness arms for.</summary>
    internal static StageRef Record { get; } = StageRef.Create(Provider, StageId.Create("record"), 1);

    /// <summary>The contract of the numbers these stages carry.</summary>
    internal static ElementContract<long> Number { get; } = ElementContract.For<long>("benchmark-number", 1);

    /// <summary>The contract of the range source's payload.</summary>
    internal static ContractReference RangeParameters { get; } =
        ContractReference.Create(ContractId.Create("benchmark-range-parameters"), 1);

    /// <summary>The contract of the recording sink's payload.</summary>
    internal static ContractReference RecordParameters { get; } =
        ContractReference.Create(ContractId.Create("benchmark-record-parameters"), 1);

    /// <summary>The payload member holding how many numbers the source emits.</summary>
    internal const string CountMember = "count";

    /// <summary>The payload member saying whether the source waits instead of ending.</summary>
    internal const string ParkMember = "park";

    /// <summary>The payload member naming the ledger the sink writes to.</summary>
    internal const string LogMember = "log";

    /// <summary>Builds the catalog a silo registers to run this vocabulary.</summary>
    /// <returns>The catalog.</returns>
    internal static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Source(Range, RangeParameters, Port.Out("out", Number)),
            StageSpecification.Sink(Record, RecordParameters, Port.In("in", Number)),
        ]);

    /// <summary>Writes the range source's payload.</summary>
    /// <param name="count">How many numbers to emit, starting at one.</param>
    /// <param name="park">
    /// <see langword="true"/> to wait to be stopped after the last one instead of ending.
    /// </param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// Canonical form sorts members, and <c>count</c> already precedes <c>park</c>, so what is written here
    /// is what is stored.
    /// </remarks>
    internal static CanonicalJsonValue WriteRange(long count, bool park) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CountMember}\":{count},\"{ParkMember}\":{(park ? "true" : "false")}}}"));

    /// <summary>Writes the recording sink's payload.</summary>
    /// <param name="log">The ledger the sink writes its deliveries to.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue WriteRecord(string log) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{LogMember}\":{JsonSerializer.Serialize(log)}}}"));

    /// <summary>Reads the range source's payload back.</summary>
    /// <param name="parameters">The node's payload.</param>
    /// <returns>How many numbers to emit and whether to wait afterwards.</returns>
    /// <exception cref="InvalidOperationException">The payload is not one this provider wrote.</exception>
    internal static (long Count, bool Park) ReadRange(CanonicalJsonValue parameters)
    {
        JsonElement payload = Payload(parameters);

        if (!payload.TryGetProperty(CountMember, out JsonElement counted) ||
            !counted.TryGetInt64(out long count) ||
            !payload.TryGetProperty(ParkMember, out JsonElement parked) ||
            parked.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
        {
            throw new InvalidOperationException(
                $"The range source carries the payload {parameters}, and this provider writes an object with a 'count' and a 'park'.");
        }

        return (count, parked.ValueKind is JsonValueKind.True);
    }

    /// <summary>Reads the recording sink's payload back.</summary>
    /// <param name="parameters">The node's payload.</param>
    /// <returns>The ledger to write to.</returns>
    /// <exception cref="InvalidOperationException">The payload is not one this provider wrote.</exception>
    internal static string ReadRecord(CanonicalJsonValue parameters)
    {
        JsonElement payload = Payload(parameters);

        if (!payload.TryGetProperty(LogMember, out JsonElement log) ||
            log.ValueKind is not JsonValueKind.String)
        {
            throw new InvalidOperationException(
                $"The recording sink carries the payload {parameters}, and this provider writes an object with a 'log'.");
        }

        return log.GetString()!;
    }

    /// <summary>Opens a payload as the object every stage here declares.</summary>
    /// <param name="parameters">The payload.</param>
    /// <returns>The object.</returns>
    /// <exception cref="InvalidOperationException">The payload is not an object.</exception>
    private static JsonElement Payload(CanonicalJsonValue parameters)
    {
        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            throw new InvalidOperationException(
                $"The payload {parameters} is not a JSON object, and every stage of this provider declares one.");
        }

        return parameters.ToElement();
    }
}
