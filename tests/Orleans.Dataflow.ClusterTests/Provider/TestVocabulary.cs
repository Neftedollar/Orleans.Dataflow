using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.ClusterTests.Provider;

/// <summary>
/// The registered stage vocabulary the cluster tests run: a counting source, a doubling flow, a failing
/// flow, and a summing sink that exposes a result.
/// </summary>
/// <remarks>
/// <para>
/// Small on purpose, and real on purpose. Every one of these is a genuine registered stage — declared in a
/// catalog, resolved by identity from a document, built by a runtime factory — so a pipeline written with
/// them proves the whole path from author to result without any of the local vocabulary's delegates being
/// involved anywhere.
/// </para>
/// <para>
/// The elements are <see cref="long"/> and the result is a <see cref="long"/>, which are Orleans-primitive
/// and therefore serializable without any annotation. That is a deliberate simplification of this
/// vocabulary and not a statement about the runtime: a result of an author's own type must satisfy Orleans
/// serialization, and the phase-1 proof is about the path rather than about the serializer.
/// </para>
/// </remarks>
internal static class TestVocabulary
{
    /// <summary>The provider every stage of this vocabulary belongs to.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create("test");

    /// <summary>The source that emits a run of consecutive numbers.</summary>
    internal static StageRef Range { get; } = StageRef.Create(Provider, StageId.Create("range"), 1);

    /// <summary>The flow that doubles every element.</summary>
    internal static StageRef Double { get; } = StageRef.Create(Provider, StageId.Create("double"), 1);

    /// <summary>The flow that throws when it sees a declared element.</summary>
    internal static StageRef Fail { get; } = StageRef.Create(Provider, StageId.Create("fail"), 1);

    /// <summary>The sink that sums every element into a result.</summary>
    internal static StageRef Sum { get; } = StageRef.Create(Provider, StageId.Create("sum"), 1);

    /// <summary>The source of <see cref="Range"/> with the opaque element contract on its output.</summary>
    /// <remarks>
    /// <para>
    /// The same numbers under a different declaration, and it exists for one reason: local plumbing declares
    /// <c>local-opaque@v1</c> on every port, a registered stage declares whatever its provider registered,
    /// and the graph compiler's element rule compares the two for equality. So a document with a buffer
    /// between two stages carrying <see cref="Number"/> does not validate anywhere, and a deployable
    /// document with plumbing in it can only be written today by a provider that types its elements in the
    /// CLR rather than in the document — which is what declaring the opaque contract says.
    /// </para>
    /// <para>
    /// That limit is ADR 0009's rather than this fixture's, and
    /// <c>DeployablePlumbingTests.PlumbingBetweenTwoStagesThatTypeTheirElementsIsStillRefusedByTheElementRule</c>
    /// measures it directly. This pair is what lets the cluster prove the rest of the ADR meanwhile.
    /// </para>
    /// </remarks>
    internal static StageRef OpaqueRange { get; } =
        StageRef.Create(Provider, StageId.Create("opaque-range"), 1);

    /// <summary>The sink of <see cref="Sum"/> with the opaque element contract on its input.</summary>
    internal static StageRef OpaqueSum { get; } =
        StageRef.Create(Provider, StageId.Create("opaque-sum"), 1);

    /// <summary>The flow that doubles every element through an awaited callback.</summary>
    /// <remarks>
    /// The asynchronous shape of the seam, which is a different code path from the synchronous one: it
    /// heads its own segment, elements reach it through a bounded channel, and its concurrency bound is
    /// what limits how many callbacks are in flight.
    /// </remarks>
    internal static StageRef DoubleAsync { get; } = StageRef.Create(Provider, StageId.Create("double-async"), 1);

    /// <summary>The sink that collects every element and sums the collection at the end.</summary>
    /// <remarks>
    /// The terminal shape with a projection and a mutable per-run seed, which the summing sink does not
    /// exercise: its state is a value two runs could share without noticing, and a list is not.
    /// </remarks>
    internal static StageRef Collected { get; } = StageRef.Create(Provider, StageId.Create("collected"), 1);

    /// <summary>The flow whose factory returns a source, which cannot stand where the node does.</summary>
    internal static StageRef Misplaced { get; } = StageRef.Create(Provider, StageId.Create("misplaced"), 1);

    /// <summary>The flow whose factory refuses to build it.</summary>
    internal static StageRef Explode { get; } = StageRef.Create(Provider, StageId.Create("explode"), 1);

    /// <summary>The junction that delivers every element to both of its legs.</summary>
    /// <remarks>
    /// A registered junction, which is what M4.5 made possible: its ports carry this vocabulary's own
    /// element contract, so a branching pipeline built out of these stages declares no capability token and
    /// deploys. Its legs are named for what they are rather than <c>out-0</c> and <c>out-1</c>, because a
    /// provider names its own ports.
    /// </remarks>
    internal static StageRef Split { get; } = StageRef.Create(Provider, StageId.Create("split"), 1);

    /// <summary>The sink whose result is a block of bytes of a declared size.</summary>
    /// <remarks>
    /// The stage the result-size cap is measured against. Its result is a <c>byte[]</c> rather than a
    /// number because the cap is about what crosses the wire, and a block of bytes is the one result whose
    /// serialized size a test can state exactly.
    /// </remarks>
    internal static StageRef Bulk { get; } = StageRef.Create(Provider, StageId.Create("bulk"), 1);

    /// <summary>The sink that writes down every element it is handed, in order.</summary>
    /// <remarks>
    /// The crash suite's measuring instrument. A duplicate window is a claim about which elements were
    /// delivered twice, so the sink that proves it has to record a sequence rather than accumulate a number;
    /// the log it writes to is named by the document and lives in the test process, which is what lets it
    /// outlive the silo whose death the window is measured across.
    /// </remarks>
    internal static StageRef Record { get; } = StageRef.Create(Provider, StageId.Create("record"), 1);

    /// <summary>The contract of the numbers this vocabulary's stages carry.</summary>
    internal static ElementContract<long> Number { get; } = ElementContract.For<long>("test-number", 1);

    /// <summary>The contract of the total a summing sink yields.</summary>
    internal static ResultContract<long> Total { get; } = ResultContract.For<long>("test-total", 1);

    /// <summary>The contract every local port declares, borrowed by the two opaque stages.</summary>
    /// <remarks>
    /// The identity is the local vocabulary's own, and reusing it is the point rather than a shortcut: a
    /// provider that declares it is saying "my elements are typed by the CLR and not by this document",
    /// which is exactly what a stage carrying boxed <see cref="long"/> values through this seam is doing.
    /// </remarks>
    internal static ElementContract<long> Opaque { get; } = ElementContract.For<long>("local-opaque", 1);

    /// <summary>The contract of the block of bytes the bulk sink yields.</summary>
    internal static ResultContract<byte[]> Block { get; } = ResultContract.For<byte[]>("test-block", 1);

    /// <summary>The contract of a payload with no members.</summary>
    internal static ContractReference NoParameters { get; } =
        ContractReference.Create(ContractId.Create("test-no-parameters"), 1);

    /// <summary>The contract of the range source's payload.</summary>
    internal static ContractReference RangeParameters { get; } =
        ContractReference.Create(ContractId.Create("test-range-parameters"), 1);

    /// <summary>The contract of the failing flow's payload.</summary>
    internal static ContractReference FailParameters { get; } =
        ContractReference.Create(ContractId.Create("test-fail-parameters"), 1);

    /// <summary>The contract of the bulk sink's payload.</summary>
    internal static ContractReference BulkParameters { get; } =
        ContractReference.Create(ContractId.Create("test-bulk-parameters"), 1);

    /// <summary>The contract of the recording sink's payload.</summary>
    internal static ContractReference RecordParameters { get; } =
        ContractReference.Create(ContractId.Create("test-record-parameters"), 1);

    /// <summary>The empty parameter payload every unparameterized stage of this vocabulary carries.</summary>
    internal static CanonicalJsonValue Empty { get; } = CanonicalJsonValue.Parse("{}");

    /// <summary>Gets the catalog a silo registers to run this vocabulary.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// A fresh catalog per call rather than a shared instance, because a test that registers two silos
    /// with two catalogs is a test about catalogs and would be quietly answered by one shared object.
    /// </remarks>
    internal static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Source(
                Range,
                RangeParameters,
                Port.Out("out", Number),
                TestRangeParameters.Validator),
            StageSpecification.Flow(Double, NoParameters, Port.In("in", Number), Port.Out("out", Number)),
            StageSpecification.Flow(Fail, FailParameters, Port.In("in", Number), Port.Out("out", Number)),
            StageSpecification.Sink(Sum, NoParameters, Port.In("in", Number), Port.Result("total", Total)),
            StageSpecification.Source(
                OpaqueRange,
                RangeParameters,
                Port.Out("out", Opaque),
                TestRangeParameters.Validator),
            StageSpecification.Sink(
                OpaqueSum,
                NoParameters,
                Port.In("in", Opaque),
                Port.Result("total", Total)),
            StageSpecification.Flow(DoubleAsync, NoParameters, Port.In("in", Number), Port.Out("out", Number)),
            StageSpecification.Sink(Collected, NoParameters, Port.In("in", Number), Port.Result("total", Total)),
            StageSpecification.Flow(Misplaced, NoParameters, Port.In("in", Number), Port.Out("out", Number)),
            StageSpecification.Flow(Explode, NoParameters, Port.In("in", Number), Port.Out("out", Number)),
            StageSpecification.FanOut(
                Split,
                NoParameters,
                Port.In("in", Number),
                [Port.Out("left", Number), Port.Out("right", Number)]),
            StageSpecification.Sink(Bulk, BulkParameters, Port.In("in", Number), Port.Result("payload", Block)),
            StageSpecification.Sink(
                Record,
                RecordParameters,
                Port.In("in", Number),
                TestRecordParameters.Validator),
        ]);
}
