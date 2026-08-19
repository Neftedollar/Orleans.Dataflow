using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;

namespace Orleans.Dataflow.Tests.Conformance;

/// <summary>
/// The conformance kit measured against providers that are wrong on purpose.
/// </summary>
/// <remarks>
/// <para>
/// Both vocabularies this repository ships pass every check, and a green suite that measured nothing would
/// look exactly the same. So each check is pointed at one provider broken in the one way that check is
/// about, and required to say so — which is what makes the green suites next door mean something.
/// </para>
/// <para>
/// <b>Two checks have no negative test here, and it is a property of what they assert rather than an
/// omission.</b> The canonical-order and unique-name clauses of the port check re-derive invariants
/// <c>StageSpecification.Create</c> enforces, so a specification breaking them cannot be constructed
/// through the public factory at all — what is falsifiable is the clause that factory does *not* enforce, a
/// stage with no port, and that one has a test. And
/// <c>NoCoreOptionTypeNamesAnythingOfThisProvider</c> fails only when a type shipped in the core package
/// names a type of the provider's, which no test assembly can arrange: it guards a future change to
/// <c>Orleans.Dataflow</c> rather than reporting a present state, and it is written down here so that its
/// green is read for what it is.
/// </para>
/// </remarks>
public sealed class ProviderConformanceTests
{
    [Fact]
    public void AStageWithNoPortAtAllIsCaught()
    {
        // The one port rule a StageSpecification does not enforce for itself: a stage may legally declare no
        // input, no output, and no result, and nothing in a graph could ever reach it.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(ConformanceProbe.Portless()).Check("EveryPortCarriesADeclaredContractInCanonicalOrder"));

        Assert.Contains("declares no port at all", Assert.Single(failed.Failures), StringComparison.Ordinal);
    }

    [Fact]
    public void AStageWhosePayloadNothingReadsIsCaught()
    {
        // A stage that declares a parameter contract and no validator accepts every payload that names the
        // contract, including one written for another stage entirely.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(ConformanceProbe.Source())
                .Check("EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare"));

        Assert.Contains(
            "no parameter validator",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AReaderThatLetsThroughAMemberItDoesNotDeclareIsCaught()
    {
        // The rule every payload reader in this repository keeps and the one a new provider is likeliest to
        // miss, because ignoring an unknown member is what a hand-written reader does by default.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(ConformanceProbe.Source(new ProbeReader(ignoresUnknownMembers: true)))
                .Check("EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains("conformance-unknown-member", StringComparison.Ordinal));
    }

    [Fact]
    public void AReaderWhoseRefusalsNameNothingIsCaught()
    {
        // A reader may refuse everything it should and still leave an author with a document and no idea
        // which line of it to change, because the compiler embeds the fragment in a diagnostic that names
        // the node and nothing else.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(ConformanceProbe.Source(new ProbeReader(namesNothing: true)))
                .Check("EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains("without naming 'name' in single quotes", StringComparison.Ordinal));
    }

    [Fact]
    public void AReaderThatAcceptsAPayloadMissingARequiredMemberIsCaught()
    {
        // The other half of "refuses what it does not declare": a reader that shrugs at an absent member
        // hands the factory a declaration with a hole in it, which becomes a default somewhere.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(ConformanceProbe.Source(new ProbeReader(ignoresMissingSize: true)))
                .Check("EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains(
                "accepts a payload that is missing the member 'size'",
                StringComparison.Ordinal));
    }

    [Fact]
    public void AMemberDeclaredOptionalThatTheReaderRequiresIsCaught()
    {
        // "Optional" is a claim the sample makes, and the kit checks it in the direction that can be wrong:
        // a member named optional whose absence the reader refuses is a provider documenting one contract
        // and shipping another.
        ProviderConformance kit = ProviderConformance.Create(
            ConformanceProbe.Provider,
            ConformanceProbe.Catalog(ConformanceProbe.Source(new ProbeReader())),
            ProbeFactory.Correct(),
            [ProviderStageSample.Create(ConformanceProbe.Stage("source"), ConformanceProbe.Payload, ["size"])]);

        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => kit.Check("EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare"));

        Assert.Contains(
            "refuses the payload without the optional member 'size'",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void ASampleTheProvidersOwnReaderRejectsIsCaught()
    {
        // The sample is the provider's own claim about what its stage accepts, so a sample the reader
        // refuses means the kit is about to mutate a payload that was never valid — and every later failure
        // would be a consequence of that rather than of the mutation.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Named("""{"name":"probe"}""")
                .Check("EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains("refuses the sample payload", StringComparison.Ordinal));
    }

    [Fact]
    public void ACatalogThatPublishesADifferentVocabularyOnEveryReadIsCaught()
    {
        // A catalog fingerprint is what two silos exchange instead of a catalog, so a provider whose
        // IStageCatalog composes its specifications per call publishes two vocabularies under one name, and
        // every symptom of that appears somewhere else.
        ProviderConformance kit = ProviderConformance.Create(
            ConformanceProbe.Provider,
            new DriftingCatalog(),
            ProbeFactory.Correct(),
            [ProviderStageSample.Create(ConformanceProbe.Stage("source"), ConformanceProbe.Payload)]);

        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => kit.Check("TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages"));

        Assert.Contains(
            "one vocabulary but one per read",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AFactoryThatDoesNotImplementAStageItsOwnCatalogDeclaresIsCaught()
    {
        // One registration per vocabulary is the seam's shape, and this is what it costs: a deployment that
        // registered a catalog of ten stages and a factory implementing nine finds the tenth at the first
        // document that names it.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Flow(),
                    new ProbeFactory(static request => request.Node.Stage.Stage.Value is "flow"
                        ? throw new NotSupportedException("not this build")
                        : ProbeFactory.Build(request)))
                .Check("TheFactoryAnswersForEveryStageTheCatalogDeclares"));

        Assert.Contains(
            "refused the stage 'conformance-probe/flow@v1', which its own catalog declares",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AFactoryThatBuildsAStrangerIsCaught()
    {
        // A factory with no final refusal builds whatever it is handed, so a document naming a stage this
        // build does not implement runs something else instead of being refused.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Source(new ProbeReader()),
                    new ProbeFactory(static _ => DataflowStageRuntime.Element(static element => element)))
                .Check("TheFactoryRefusesAStageTheCatalogDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains("instead of refusing it", StringComparison.Ordinal));
    }

    [Fact]
    public void AFactoryThatFailsByAccidentOnAStrangerIsCaught()
    {
        // The accident every factory in this repository writes an explicit lookup to avoid: a dictionary
        // indexer over the stages this build implements reports a configuration problem as a defect
        // somewhere else entirely.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Source(new ProbeReader()),
                    new ProbeFactory(static request => request.Node.Stage == ConformanceProbe.Stage("source")
                        ? ProbeFactory.Build(request)
                        : throw new KeyNotFoundException()))
                .Check("TheFactoryRefusesAStageTheCatalogDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains(
                "with KeyNotFoundException, which is an accident rather than a refusal",
                StringComparison.Ordinal));
    }

    [Fact]
    public void AFactoryThatRefusesAStrangerWithoutNamingItIsCaught()
    {
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Source(new ProbeReader()),
                    new ProbeFactory(static request => request.Node.Stage == ConformanceProbe.Stage("source")
                        ? ProbeFactory.Build(request)
                        : throw new InvalidOperationException("this build implements something else")))
                .Check("TheFactoryRefusesAStageTheCatalogDoesNotDeclare"));

        Assert.Contains(
            failed.Failures,
            failure => failure.Contains("without naming it", StringComparison.Ordinal));
    }

    [Fact]
    public void AFactoryThatBuildsAJunctionWhereItsCatalogDeclaresAChainIsCaught()
    {
        // The M4.5a miscast, generalized: a catalog describes ports and says nothing about what a factory
        // will build, so this disagreement is invisible until a run is planned — and until now it was
        // invisible until a graph existed to plan.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Flow(),
                    new ProbeFactory(static _ => DataflowStageRuntime.Broadcast()))
                .Check("EveryRuntimeHasTheShapeItsSpecificationDeclares"));

        Assert.Contains(
            "declared as an element stage and its factory built a FanOut runtime",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void ATerminalThatProducesNoResultForAStageDeclaringOneIsCaught()
    {
        // The quietest disagreement of the lot: the run succeeds, the sink counts, and the slot the document
        // declared over it resolves nothing.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.CountingSink(),
                    new ProbeFactory(static _ => DataflowStageRuntime.Terminal(
                        static () => 0L,
                        static (state, _) => state,
                        finish: null,
                        producesResult: false)))
                .Check("EveryRuntimeHasTheShapeItsSpecificationDeclares"));

        Assert.Contains(
            "built a terminal that produces no result",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AFanOutThatSplitsARowIntoFewerPartsThanItHasLegsIsCaught()
    {
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Wide(),
                    new ProbeFactory(static _ => DataflowStageRuntime.Unzip(
                        [static element => element, static element => element])))
                .Check("EveryRuntimeHasTheShapeItsSpecificationDeclares"));

        Assert.Contains(
            "declares 3 output ports and its factory splits a row into 2 parts",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionNoTypedHandleCanAuthorIsCaught()
    {
        // Three legs carrying two contracts falls between the two fan-out handles: the like-legged one needs
        // every leg to carry one contract and the unlike-legged one takes exactly two. A provider that
        // registers such a stage has published something an author cannot write.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(ConformanceProbe.Wide()).Check("EveryStageHasATypedHandleThatRefusesTheWrongShape"));

        Assert.Contains(
            "no typed handle can author it",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void APayloadThatNamesAClrTypeThisProcessCanResolveIsCaught()
    {
        // ADR 0001's rule, broken where it is easiest to break: a payload is the one part of a document a
        // provider writes freely, and a CLR name in one makes reading the document load code.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Named("""{"name":"System.String","size":3}""").Check("NoParameterPayloadNamesAClrType"));

        Assert.Contains(
            "which this process resolves to the CLR type System.String",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void APayloadThatNamesAClrTypeInAnAssemblyThisProcessHasNotGotIsCaught()
    {
        // The half the runtime's own answer cannot give, and the dangerous half: a name this process cannot
        // resolve is a name written to be resolved somewhere else, which is exactly what a durable document
        // must never carry.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Named(
                    """{"name":"Contoso.Orders.Order, Contoso.Orders, Version=2.0.0.0, Culture=neutral, PublicKeyToken=null","size":3}""")
                .Check("NoParameterPayloadNamesAClrType"));

        Assert.Contains(
            "which is an assembly-qualified CLR type name",
            Assert.Single(failed.Failures),
            StringComparison.Ordinal);
    }

    [Fact]
    public void ACorrectProbeProviderPassesEveryCheckAtOnce()
    {
        // The control the negatives need: the same vocabulary with none of the defects in it passes, so each
        // failure above is caused by the one thing that test changed and not by the fixture.
        ProviderConformance.Create(
            ConformanceProbe.Provider,
            StageCatalog.Create(
            [
                ConformanceProbe.Source(new ProbeReader()),
                ConformanceProbe.Flow(),
                ConformanceProbe.CountingSink(),
            ]),
            ProbeFactory.Correct(),
            [
                ProviderStageSample.Create(ConformanceProbe.Stage("source"), ConformanceProbe.Payload),
                ProviderStageSample.Create(ConformanceProbe.Stage("flow"), ConformanceProbe.Payload),
                ProviderStageSample.Create(ConformanceProbe.Stage("counting-sink"), ConformanceProbe.Payload),
            ]).CheckAll();
    }

    [Fact]
    public void CheckAllReportsTheFailuresOfEveryCheckAtOnce()
    {
        // The house rule for reports, applied to the kit itself: a provider fixing one check per run learns
        // the contract one check at a time, so every check's failures arrive together, each tagged with the
        // check that found it.
        ProviderConformanceException failed = Assert.Throws<ProviderConformanceException>(
            () => Kit(
                    ConformanceProbe.Flow(),
                    new ProbeFactory(static _ => DataflowStageRuntime.Broadcast()))
                .CheckAll());

        Assert.Contains(
            failed.Failures,
            failure => failure.StartsWith(
                "[EveryRuntimeHasTheShapeItsSpecificationDeclares]",
                StringComparison.Ordinal));
        Assert.Contains(
            failed.Failures,
            failure => failure.StartsWith(
                "[TheFactoryRefusesAStageTheCatalogDoesNotDeclare]",
                StringComparison.Ordinal));
    }

    [Fact]
    public void AnUnknownCheckNameIsRefusedNamingTheOnesThereAre()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Kit(ConformanceProbe.Flow()).Check("EverythingIsFine"));

        Assert.Equal("check", refused.ParamName);
        Assert.Contains("'NoParameterPayloadNamesAClrType'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ASampleNamingAMemberThePayloadDoesNotCarryIsRefused()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => ProviderStageSample.Create(
                ConformanceProbe.Stage("source"),
                ConformanceProbe.Payload,
                ["absent"]));

        Assert.Contains("which the payload does not carry", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Points the kit at the probe source carrying a payload of the test's own choosing.</summary>
    /// <param name="payload">The payload text.</param>
    /// <returns>The kit.</returns>
    private static ProviderConformance Named(string payload) =>
        ProviderConformance.Create(
            ConformanceProbe.Provider,
            ConformanceProbe.Catalog(ConformanceProbe.Source(new ProbeReader())),
            ProbeFactory.Correct(),
            [ProviderStageSample.Create(ConformanceProbe.Stage("source"), CanonicalJsonValue.Parse(payload))]);

    /// <summary>Points the kit at one probe stage with the factory that answers for it correctly.</summary>
    /// <param name="specification">The stage.</param>
    /// <returns>The kit.</returns>
    private static ProviderConformance Kit(StageSpecification specification) =>
        Kit(specification, ProbeFactory.Correct());

    /// <summary>Points the kit at one probe stage and one factory.</summary>
    /// <param name="specification">The stage.</param>
    /// <param name="factory">The factory.</param>
    /// <returns>The kit.</returns>
    private static ProviderConformance Kit(StageSpecification specification, IDataflowStageFactory factory) =>
        ProviderConformance.Create(
            ConformanceProbe.Provider,
            ConformanceProbe.Catalog(specification),
            factory,
            [ProviderStageSample.Create(specification.Stage, ConformanceProbe.Payload)]);

    /// <summary>A catalog that composes its specifications afresh, and differently, on every read.</summary>
    /// <remarks>
    /// The shape of a provider that builds its catalog lazily from something that moves. It is legal, it
    /// compiles, and every consequence of it — a fingerprint two silos disagree about, a document accepted
    /// here and refused there — shows up far from the cause.
    /// </remarks>
    private sealed class DriftingCatalog : IStageCatalog
    {
        private int _reads;

        /// <inheritdoc/>
        public IReadOnlyList<StageSpecification> Specifications =>
            [
                Interlocked.Increment(ref _reads) is 1
                    ? ConformanceProbe.Source(new ProbeReader())
                    : StageSpecification.Source(
                        ConformanceProbe.Stage("source"),
                        ContractReference.Create(ContractId.Create("probe-parameters"), 1),
                        Port.Out("out", ContractReference.Create(ContractId.Create("probe-other"), 1)),
                        new ProbeReader()),
            ];

        /// <inheritdoc/>
        public bool TryGetSpecification(StageRef stageRef, out StageSpecification specification)
        {
            foreach (StageSpecification declared in Specifications)
            {
                if (declared.Stage == stageRef)
                {
                    specification = declared;

                    return true;
                }
            }

            specification = null!;

            return false;
        }
    }
}
