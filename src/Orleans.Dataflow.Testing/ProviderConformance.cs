using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The rules a stage provider has to keep, checked against the provider's own catalog and factory.
/// </summary>
/// <remarks>
/// <para>
/// <b>What it is.</b> A provider ships two halves — a <see cref="IStageCatalog"/> saying which stages exist
/// and an <see cref="IDataflowStageFactory"/> saying what they do — and everything that can go wrong
/// between them goes wrong quietly: a port whose contract is the default value, a payload reader that
/// ignores a member it does not understand, a factory that answers with a junction where its own catalog
/// declared a chain. None of that is caught by a compiler and all of it is caught here, mechanically, from
/// the two halves plus one valid payload per stage.
/// </para>
/// <para>
/// <b>Where the rules came from.</b> Every check is a generalization of something this repository already
/// proved by hand for its own providers: the canonical port order the registered junction handles read, the
/// unknown-member refusal every adapter payload performs, the catalog fingerprint the cluster negotiates on,
/// the once-per-node factory contract, the planner's refusal of a stage whose runtime shape disagrees with
/// its document, and the handle-creation validation that turns a catalog mismatch into an
/// <see cref="ArgumentException"/> at the call an author wrote. What was a test per provider is a check for
/// every provider.
/// </para>
/// <para>
/// <b>How it is run.</b> <see cref="Checks"/> names them and <see cref="Check(string)"/> runs one, so a
/// provider's own suite is one theory over the names and gains a test whenever this kit gains a check:
/// </para>
/// <code>
/// public static TheoryData&lt;string&gt; Checks =&gt; [.. ProviderConformance.Checks];
///
/// [Theory]
/// [MemberData(nameof(Checks))]
/// public void TheProviderConforms(string check) =&gt; Kit().Check(check);
/// </code>
/// <para>
/// Nothing here names a test framework. A failure is a <see cref="ProviderConformanceException"/> carrying
/// every violation the check found, which is what every test framework already reports well.
/// </para>
/// <para>
/// <b>What it does not check.</b> Semantics. Whether a source really ends its sequence on a stop token,
/// whether a terminal's fold is associative, whether an adapter's acknowledgement boundary is where its
/// documentation says it is — none of that is derivable from a catalog and a factory, and a kit that
/// pretended otherwise would be worse than one that says so. Those are the provider's own tests, and the
/// answers belong in the delivery table the provider publishes beside its adapters.
/// </para>
/// </remarks>
public sealed class ProviderConformance
{
    /// <summary>The member a mutated payload carries to see whether the reader refuses what it cannot read.</summary>
    private const string UnknownMember = "conformance-unknown-member";

    /// <summary>The stage identifier the factory is asked about to see whether it refuses a stranger.</summary>
    private const string UnknownStage = "conformance-no-such-stage";

    /// <summary>The contract reference no port of a conforming provider declares.</summary>
    private const string AbsentContract = "conformance-absent-contract";

    private readonly ProviderId _provider;
    private readonly IStageCatalog _catalog;
    private readonly IDataflowStageFactory _factory;
    private readonly IReadOnlyList<StageSpecification> _specifications;
    private readonly Dictionary<StageRef, ProviderStageSample> _samples;

    /// <summary>Initializes a new instance of the <see cref="ProviderConformance"/> class.</summary>
    /// <param name="provider">The provider under test.</param>
    /// <param name="catalog">The catalog the provider publishes.</param>
    /// <param name="factory">The factory the provider registers.</param>
    /// <param name="specifications">The catalog's specifications belonging to the provider.</param>
    /// <param name="samples">One sample per specification, keyed by stage reference.</param>
    private ProviderConformance(
        ProviderId provider,
        IStageCatalog catalog,
        IDataflowStageFactory factory,
        IReadOnlyList<StageSpecification> specifications,
        Dictionary<StageRef, ProviderStageSample> samples)
    {
        _provider = provider;
        _catalog = catalog;
        _factory = factory;
        _specifications = specifications;
        _samples = samples;
    }

    /// <summary>Gets the names of every check this kit runs, in the order it runs them.</summary>
    /// <value>
    /// Nine names, each a sentence stating what the check asserts, suitable as a test theory's own data:
    /// a failure then reads as the sentence that stopped being true.
    /// </value>
    public static IReadOnlyList<string> Checks { get; } =
    [
        EveryPortCarriesADeclaredContractInCanonicalOrder,
        EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare,
        TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages,
        TheFactoryAnswersForEveryStageTheCatalogDeclares,
        TheFactoryRefusesAStageTheCatalogDoesNotDeclare,
        EveryRuntimeHasTheShapeItsSpecificationDeclares,
        EveryStageHasATypedHandleThatRefusesTheWrongShape,
        NoParameterPayloadNamesAClrType,
        NoCoreOptionTypeNamesAnythingOfThisProvider,
    ];

    /// <summary>The name of the check over port declarations and their order.</summary>
    private const string EveryPortCarriesADeclaredContractInCanonicalOrder =
        nameof(EveryPortCarriesADeclaredContractInCanonicalOrder);

    /// <summary>The name of the check over payload readers.</summary>
    private const string EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare =
        nameof(EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare);

    /// <summary>The name of the check over catalog fingerprint determinism.</summary>
    private const string TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages =
        nameof(TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages);

    /// <summary>The name of the check over factory coverage.</summary>
    private const string TheFactoryAnswersForEveryStageTheCatalogDeclares =
        nameof(TheFactoryAnswersForEveryStageTheCatalogDeclares);

    /// <summary>The name of the check over what a factory does with a stage it does not implement.</summary>
    private const string TheFactoryRefusesAStageTheCatalogDoesNotDeclare =
        nameof(TheFactoryRefusesAStageTheCatalogDoesNotDeclare);

    /// <summary>The name of the check comparing a built runtime with its specification.</summary>
    private const string EveryRuntimeHasTheShapeItsSpecificationDeclares =
        nameof(EveryRuntimeHasTheShapeItsSpecificationDeclares);

    /// <summary>The name of the check over typed authoring handles.</summary>
    private const string EveryStageHasATypedHandleThatRefusesTheWrongShape =
        nameof(EveryStageHasATypedHandleThatRefusesTheWrongShape);

    /// <summary>The name of the check that keeps CLR names out of documents.</summary>
    private const string NoParameterPayloadNamesAClrType = nameof(NoParameterPayloadNamesAClrType);

    /// <summary>The name of the check that keeps a provider's configuration out of the core.</summary>
    private const string NoCoreOptionTypeNamesAnythingOfThisProvider =
        nameof(NoCoreOptionTypeNamesAnythingOfThisProvider);

    /// <summary>Points the kit at one provider's two halves.</summary>
    /// <param name="provider">The provider whose stages are checked.</param>
    /// <param name="catalog">The catalog the provider publishes, which may declare other providers' stages too.</param>
    /// <param name="factory">The factory the provider registers for those stages.</param>
    /// <param name="samples">One sample payload per stage the catalog declares for this provider.</param>
    /// <returns>The kit.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="catalog"/>, <paramref name="factory"/>, or <paramref name="samples"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="provider"/> is the default value, the catalog declares no stage of it, or the samples
    /// and the catalog do not describe the same set of stages. The message is a numbered list of every
    /// violation found.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A catalog declaring no stage of this provider is refused rather than passing every check vacuously,
    /// which is the one way a conformance kit can lie: a green suite that measured nothing reads exactly
    /// like a green suite that measured everything.
    /// </para>
    /// <para>
    /// Every declared stage needs a sample and every sample needs a declared stage, both for the same
    /// reason. A stage without one would be skipped silently, and a sample without one is a stage the
    /// author believes they registered and did not.
    /// </para>
    /// </remarks>
    public static ProviderConformance Create(
        ProviderId provider,
        IStageCatalog catalog,
        IDataflowStageFactory factory,
        IEnumerable<ProviderStageSample> samples)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(samples);

        ProviderStageSample[] declared = [.. samples];
        List<string> violations = [];

        if (provider.IsDefault)
        {
            violations.Add($"the provider is the default {nameof(ProviderId)}, which names no provider");

            throw new ArgumentException(FormatViolations("conformance kit", violations), nameof(provider));
        }

        List<StageSpecification> specifications =
            [.. catalog.Specifications.Where(specification => specification.Stage.Provider == provider)];

        if (specifications.Count == 0)
        {
            violations.Add(
                $"the catalog declares no stage of the provider '{provider}', so every check would pass without measuring anything");
        }

        Dictionary<StageRef, ProviderStageSample> byStage = [];

        for (int index = 0; index < declared.Length; index++)
        {
            ProviderStageSample sample = declared[index];

            if (sample is null)
            {
                violations.Add(string.Create(CultureInfo.InvariantCulture, $"samples[{index}] is null"));
            }
            else if (!byStage.TryAdd(sample.Stage, sample))
            {
                violations.Add(
                    $"samples names the stage '{sample.Stage}' more than once, and one stage has one sample");
            }
        }

        foreach (StageSpecification specification in specifications)
        {
            if (!byStage.ContainsKey(specification.Stage))
            {
                violations.Add(
                    $"the catalog declares the stage '{specification.Stage}' and the samples carry no payload for it, so nothing would be checked of it");
            }
        }

        foreach (StageRef stage in byStage.Keys)
        {
            if (!specifications.Exists(specification => specification.Stage == stage))
            {
                violations.Add(
                    $"the samples carry a payload for the stage '{stage}', which this catalog does not declare under the provider '{provider}'");
            }
        }

        if (violations.Count > 0)
        {
            throw new ArgumentException(FormatViolations("conformance kit", violations));
        }

        return new ProviderConformance(provider, catalog, factory, specifications, byStage);
    }

    /// <summary>Points the kit at a vocabulary that carries both of its halves.</summary>
    /// <param name="provider">The vocabulary under test.</param>
    /// <param name="samples">One sample payload per stage it declares.</param>
    /// <returns>The kit.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="provider"/> or <paramref name="samples"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The vocabulary declares no stage, or the samples and the vocabulary do not describe the same set of
    /// stages. The message is a numbered list of every violation found.
    /// </exception>
    /// <remarks>
    /// The same kit, given the three arguments a <see cref="StageProvider"/> already holds: its provider, its
    /// catalog, and itself as the factory. A vocabulary written in one place is checked in one call, and the
    /// four-argument overload stays for a provider whose halves are genuinely separate values — which is the
    /// case this kit was written for and still covers.
    /// </remarks>
    public static ProviderConformance Create(
        StageProvider provider,
        IEnumerable<ProviderStageSample> samples)
    {
        ArgumentNullException.ThrowIfNull(provider);

        return Create(provider.Provider, provider.Catalog, provider, samples);
    }

    /// <summary>Runs one check.</summary>
    /// <param name="check">One of the names <see cref="Checks"/> lists.</param>
    /// <exception cref="ArgumentNullException"><paramref name="check"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="check"/> is not one of the names.</exception>
    /// <exception cref="ProviderConformanceException">
    /// The provider breaks at least one of the rules the check states. The message lists every one.
    /// </exception>
    public void Check(string check)
    {
        ArgumentNullException.ThrowIfNull(check);

        List<string> failures = [];

        switch (check)
        {
            case EveryPortCarriesADeclaredContractInCanonicalOrder:
                CheckPorts(failures);
                break;
            case EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare:
                CheckReaders(failures);
                break;
            case TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages:
                CheckFingerprint(failures);
                break;
            case TheFactoryAnswersForEveryStageTheCatalogDeclares:
                CheckFactoryCoverage(failures);
                break;
            case TheFactoryRefusesAStageTheCatalogDoesNotDeclare:
                CheckFactoryRefusal(failures);
                break;
            case EveryRuntimeHasTheShapeItsSpecificationDeclares:
                CheckShapes(failures);
                break;
            case EveryStageHasATypedHandleThatRefusesTheWrongShape:
                CheckHandles(failures);
                break;
            case NoParameterPayloadNamesAClrType:
                CheckPayloadsNameNoType(failures);
                break;
            case NoCoreOptionTypeNamesAnythingOfThisProvider:
                CheckCoreOptions(failures);
                break;
            default:
                throw new ArgumentException(
                    $"'{check}' is not a conformance check. The checks are {string.Join(", ", Checks.Select(static one => $"'{one}'"))}.",
                    nameof(check));
        }

        if (failures.Count > 0)
        {
            throw new ProviderConformanceException(_provider.Value, failures);
        }
    }

    /// <summary>Runs every check and reports every failure of all of them at once.</summary>
    /// <exception cref="ProviderConformanceException">
    /// The provider breaks at least one rule of at least one check.
    /// </exception>
    /// <remarks>
    /// The whole kit as one call, for a provider author who wants one test rather than a theory. It reports
    /// the failures of every check together rather than stopping at the first check that found one, because
    /// a provider fixing one check per run learns the contract one check at a time.
    /// </remarks>
    public void CheckAll()
    {
        List<string> failures = [];

        foreach (string check in Checks)
        {
            try
            {
                Check(check);
            }
            catch (ProviderConformanceException failed)
            {
                failures.AddRange(failed.Failures.Select(failure => $"[{check}] {failure}"));
            }
        }

        if (failures.Count > 0)
        {
            throw new ProviderConformanceException(_provider.Value, failures);
        }
    }

    /// <summary>Returns a one-line diagnostic summary of this kit.</summary>
    /// <returns>Text of the form <c>conformance of 'orleans' (10 stages, 9 checks)</c>.</returns>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"conformance of '{_provider}' ({_specifications.Count} stage{(_specifications.Count == 1 ? string.Empty : "s")}, {Checks.Count} checks)");

    /// <summary>Renders collected violations as one numbered list.</summary>
    /// <param name="subject">What was being built or checked, read after "The".</param>
    /// <param name="violations">The violations, in the order they were found.</param>
    /// <returns>A message whose first line states the count and whose remaining lines are numbered.</returns>
    internal static string FormatViolations(string subject, IReadOnlyList<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The {subject} breaks {violations.Count} ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
    }

    /// <summary>Checks that every port declares a contract and that the port lists are canonical.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// The order is load-bearing rather than cosmetic: a junction's legs are wired, planned, and routed by
    /// the position a port has in this list, so three places read one statement and a specification whose
    /// ports were not sorted would make them read three different ones. It is guaranteed by
    /// <see cref="StageSpecification"/>'s own factory and checked here anyway, because a catalog reaching a
    /// host need not have come from that factory in the same process.
    /// </remarks>
    private void CheckPorts(List<string> failures)
    {
        foreach (StageSpecification specification in _specifications)
        {
            StageRef stage = specification.Stage;

            if (specification.InputPorts.Count == 0 &&
                specification.OutputPorts.Count == 0 &&
                specification.ResultPorts.Count == 0)
            {
                failures.Add($"the stage '{stage}' declares no port at all, so nothing in a graph could reach it");
            }

            if (specification.ParameterContract.IsDefault)
            {
                failures.Add($"the stage '{stage}' declares no parameter contract");
            }

            HashSet<string> names = [];

            foreach (InputPortSpecification port in specification.InputPorts)
            {
                Port(failures, stage, "input", port.IsDefault, port.Id, port.ElementContract, names);
            }

            foreach (OutputPortSpecification port in specification.OutputPorts)
            {
                Port(failures, stage, "output", port.IsDefault, port.Id, port.ElementContract, names);
            }

            foreach (ResultPortSpecification port in specification.ResultPorts)
            {
                Port(failures, stage, "result", port.IsDefault, port.Id, port.ResultContract, names);
            }

            Ordered(failures, stage, "input", [.. specification.InputPorts.Select(port => port.Id.Value)]);
            Ordered(failures, stage, "output", [.. specification.OutputPorts.Select(port => port.Id.Value)]);
            Ordered(failures, stage, "result", [.. specification.ResultPorts.Select(port => port.Id.Value)]);
        }
    }

    /// <summary>Checks one port of one stage.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="stage">The stage the port belongs to.</param>
    /// <param name="direction">Which of the three port lists it came from.</param>
    /// <param name="isDefault">Whether the port specification is the default value.</param>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The contract the port declares.</param>
    /// <param name="names">The names already taken across the whole stage.</param>
    private static void Port(
        List<string> failures,
        StageRef stage,
        string direction,
        bool isDefault,
        PortId port,
        ContractReference contract,
        HashSet<string> names)
    {
        if (isDefault)
        {
            failures.Add($"the stage '{stage}' declares a default {direction} port, which declares no port");

            return;
        }

        if (contract.IsDefault)
        {
            failures.Add(
                $"the {direction} port '{port}' of the stage '{stage}' declares the default {nameof(ContractReference)}, and a port carries a contract a document can name");
        }

        if (!names.Add(port.Value))
        {
            failures.Add(
                $"the stage '{stage}' declares the port '{port}' more than once, and port names are unique across a stage's inputs, outputs, and result ports together");
        }
    }

    /// <summary>Checks that one port list is in the canonical ordinal order of its names.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="stage">The stage the ports belong to.</param>
    /// <param name="direction">Which of the three port lists it is.</param>
    /// <param name="names">The port names, in the order the specification holds them.</param>
    private static void Ordered(List<string> failures, StageRef stage, string direction, string[] names)
    {
        for (int index = 1; index < names.Length; index++)
        {
            if (string.CompareOrdinal(names[index - 1], names[index]) >= 0)
            {
                failures.Add(
                    $"the {direction} ports of the stage '{stage}' are not in canonical order: '{names[index - 1]}' stands before '{names[index]}', and a port's position is what wires, plans, and routes a junction's legs");

                return;
            }
        }
    }

    /// <summary>Checks that every stage's payload has a reader and that the reader refuses what it must.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// The mutations are generated from the sample rather than described, so a payload that grows a member
    /// grows its checks with it. Six families: the sample itself has to be accepted, an added member has to
    /// be refused, a removed required member has to be refused, a removed optional member has to be
    /// accepted, a retyped member has to be refused, and a payload that is not an object at all has to be
    /// refused.
    /// </para>
    /// <para>
    /// <b>Why from a sample and not from the parameter contract.</b> A <see cref="ContractReference"/> is an
    /// identifier and a major version and carries no schema — that is the definition plane's own decision,
    /// and it is what keeps a document from describing the code that reads it. So there is nothing in the
    /// contract to derive a mutation from, and one accepted example is the smallest honest input a
    /// provider can supply.
    /// </para>
    /// <para>
    /// A refusal has to name the member, in the single quotes
    /// <see cref="IStageParameterValidator"/> documents, because the graph compiler embeds the fragment in a
    /// diagnostic that names the node and nothing else: a reader that says only "the payload is wrong"
    /// leaves an author with a document and no idea which line of it to change.
    /// </para>
    /// </remarks>
    private void CheckReaders(List<string> failures)
    {
        foreach (StageSpecification specification in _specifications)
        {
            StageRef stage = specification.Stage;
            ProviderStageSample sample = _samples[stage];

            if (specification.ParameterValidator is not { } reader)
            {
                failures.Add(
                    $"the stage '{stage}' declares the parameter contract '{specification.ParameterContract}' and no parameter validator, so nothing reads the payload its occurrences carry and a member this stage never heard of reaches its factory");

                continue;
            }

            Accepts(failures, reader, stage, sample.Parameters, "the sample payload");

            JsonElement payload = sample.Parameters.ToElement();

            Refuses(
                failures,
                reader,
                stage,
                Rewrite(payload, UnknownMember, "true"),
                UnknownMember,
                $"carries the member '{UnknownMember}', which the stage does not declare");

            foreach (JsonProperty member in payload.EnumerateObject())
            {
                bool optional = sample.OptionalMembers.Contains(member.Name, StringComparer.Ordinal);
                CanonicalJsonValue without = Rewrite(payload, member.Name, null);

                if (optional)
                {
                    Accepts(
                        failures,
                        reader,
                        stage,
                        without,
                        $"the payload without the optional member '{member.Name}'");
                }
                else
                {
                    Refuses(
                        failures,
                        reader,
                        stage,
                        without,
                        member.Name,
                        $"is missing the member '{member.Name}'");
                }

                Refuses(
                    failures,
                    reader,
                    stage,
                    Rewrite(payload, member.Name, Retyped(member.Value)),
                    member.Name,
                    $"carries the member '{member.Name}' as {Kind(Retyped(member.Value))} rather than as {Kind(member.Value.GetRawText())}");
            }

            string[] others = ["[]", "0", "\"conformance\"", "true"];

            foreach (string other in others)
            {
                Refuses(
                    failures,
                    reader,
                    stage,
                    CanonicalJsonValue.Parse(other),
                    member: null,
                    $"is {Kind(other)} rather than an object");
            }
        }
    }

    /// <summary>Requires a reader to accept a payload and to answer with a well-formed report.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="reader">The stage's parameter validator.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="parameters">The payload.</param>
    /// <param name="what">What the payload is, read after "and".</param>
    private static void Accepts(
        List<string> failures,
        IStageParameterValidator reader,
        StageRef stage,
        CanonicalJsonValue parameters,
        string what)
    {
        IReadOnlyList<string> violations = Read(failures, reader, stage, parameters);

        if (violations.Count > 0)
        {
            failures.Add(
                $"the reader of the stage '{stage}' refuses {what}: {string.Join("; ", violations)}");
        }
    }

    /// <summary>Requires a reader to refuse a payload, naming the member that is wrong.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="reader">The stage's parameter validator.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="parameters">The mutated payload.</param>
    /// <param name="member">The member the refusal has to name, or <see langword="null"/> when none.</param>
    /// <param name="what">What is wrong with the payload, read after "a payload that".</param>
    private static void Refuses(
        List<string> failures,
        IStageParameterValidator reader,
        StageRef stage,
        CanonicalJsonValue parameters,
        string? member,
        string what)
    {
        IReadOnlyList<string> violations = Read(failures, reader, stage, parameters);

        if (violations.Count == 0)
        {
            failures.Add($"the reader of the stage '{stage}' accepts a payload that {what}");

            return;
        }

        if (member is not null &&
            !violations.Any(violation => violation.Contains($"'{member}'", StringComparison.Ordinal)))
        {
            failures.Add(
                $"the reader of the stage '{stage}' refuses a payload that {what} without naming '{member}' in single quotes, and it said: {string.Join("; ", violations)}");
        }
    }

    /// <summary>Runs a reader and checks that its report keeps the validator contract.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="reader">The stage's parameter validator.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="parameters">The payload.</param>
    /// <returns>The violations the reader answered with, or an empty list when it threw.</returns>
    /// <remarks>
    /// A validator that throws has broken its own contract — an invalid payload is the expected outcome of
    /// validating an untrusted document — and one that answers with a null, empty, or whitespace fragment
    /// has produced a diagnostic the graph compiler would embed as nothing at all.
    /// </remarks>
    private static IReadOnlyList<string> Read(
        List<string> failures,
        IStageParameterValidator reader,
        StageRef stage,
        CanonicalJsonValue parameters)
    {
        IReadOnlyList<string>? violations;

        try
        {
            violations = reader.Validate(parameters);
        }
        catch (Exception thrown)
        {
            failures.Add(
                $"the reader of the stage '{stage}' threw {thrown.GetType().Name} for the payload {parameters}, and a reader returns violations rather than throwing");

            return [];
        }

        if (violations is null)
        {
            failures.Add(
                $"the reader of the stage '{stage}' answered with null for the payload {parameters}, and a report is a list that may be empty");

            return [];
        }

        foreach (string violation in violations)
        {
            if (string.IsNullOrWhiteSpace(violation))
            {
                failures.Add(
                    $"the reader of the stage '{stage}' answered with an empty violation fragment for the payload {parameters}");

                break;
            }
        }

        return violations;
    }

    /// <summary>Checks that the same registration produces the same catalog identity.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// A catalog fingerprint is what two silos exchange instead of a catalog, so it has to be a function of
    /// the declared shapes and of nothing else — not of the order a provider listed its stages in, and not
    /// of which read of the catalog produced it. The second clause is the one with something to catch: a
    /// provider whose <see cref="IStageCatalog"/> composes its specifications per call rather than once can
    /// publish two vocabularies under one name, and every symptom of that appears somewhere else.
    /// </para>
    /// <para>
    /// The last clause is a control rather than a rule about the provider: a fingerprint that ignored a
    /// changed parameter contract would pass the first two and mean nothing.
    /// </para>
    /// </remarks>
    private void CheckFingerprint(List<string> failures)
    {
        StageCatalog first = StageCatalog.Create(_specifications);
        StageCatalog second = StageCatalog.Create(_specifications.Reverse());
        CatalogFingerprint fingerprint = StageCatalogSerializer.Fingerprint(first);

        if (fingerprint != StageCatalogSerializer.Fingerprint(second))
        {
            failures.Add(
                $"registering the stages of '{_provider}' in another order produces another catalog fingerprint, so two processes with one vocabulary would disagree about running it");
        }

        StageCatalog reread = StageCatalog.Create(
            _catalog.Specifications.Where(specification => specification.Stage.Provider == _provider));

        if (fingerprint != StageCatalogSerializer.Fingerprint(reread))
        {
            failures.Add(
                $"reading the catalog of '{_provider}' twice produces two catalog fingerprints, so what this provider publishes is not one vocabulary but one per read");
        }

        StageSpecification head = _specifications[0];
        StageCatalog altered = StageCatalog.Create(
        [
            .. _specifications.Skip(1),
            StageSpecification.Create(
                head.Stage,
                ContractReference.Create(ContractId.Create(AbsentContract), head.ParameterContract.MajorVersion),
                head.InputPorts,
                head.OutputPorts,
                head.ResultPorts,
                head.RequiredCapabilities),
        ]);

        if (fingerprint == StageCatalogSerializer.Fingerprint(altered))
        {
            failures.Add(
                $"changing the parameter contract of the stage '{head.Stage}' leaves the catalog fingerprint unchanged, so the fingerprint is not measuring this catalog");
        }
    }

    /// <summary>Checks that the factory builds every stage the catalog declares.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// One registration per vocabulary is the seam's own shape, and its cost is exactly this: a deployment
    /// that registered a catalog of ten stages and a factory that implements nine discovers the tenth at the
    /// first document that names it.
    /// </remarks>
    private void CheckFactoryCoverage(List<string> failures)
    {
        foreach (StageSpecification specification in _specifications)
        {
            _ = Build(failures, specification, _samples[specification.Stage].Parameters);
        }
    }

    /// <summary>Checks that the factory refuses a stage of its provider that it does not implement.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// Two strangers: a stage identifier no catalog registers, and a major version of a registered stage
    /// that no catalog registers. Both reach a factory the same way — a document naming them is refused by
    /// the graph compiler first, so what is under test is what happens when the compiler is not the thing
    /// that ran — and both must be refused by throwing, naming the stage.
    /// </para>
    /// <para>
    /// A <see cref="NullReferenceException"/>, an <see cref="IndexOutOfRangeException"/>, a
    /// <see cref="KeyNotFoundException"/>, or an <see cref="InvalidCastException"/> is not a refusal. It is
    /// the same accident every one of this repository's own factories writes an explicit lookup to avoid,
    /// and it reports a configuration problem as a defect somewhere else entirely.
    /// </para>
    /// </remarks>
    private void CheckFactoryRefusal(List<string> failures)
    {
        StageSpecification head = _specifications[0];
        int beyond = _specifications
            .Where(specification => specification.Stage.Stage == head.Stage.Stage)
            .Max(specification => specification.Stage.MajorVersion) + 1;

        Stranger(failures, head, Absent(), "a stage identifier the catalog does not register");
        Stranger(
            failures,
            head,
            StageRef.Create(_provider, head.Stage.Stage, beyond),
            "a major version of a registered stage that the catalog does not register");
    }

    /// <summary>Asks the factory about one stage it should not be able to build.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="shape">The specification whose ports and parameter contract the stranger borrows.</param>
    /// <param name="stranger">The stage reference the catalog does not declare.</param>
    /// <param name="what">What kind of stranger it is.</param>
    private void Stranger(
        List<string> failures,
        StageSpecification shape,
        StageRef stranger,
        string what)
    {
        if (_catalog.TryGetSpecification(stranger, out StageSpecification? _))
        {
            failures.Add(
                $"the catalog declares the stage '{stranger}', which this check needed to be {what}");

            return;
        }

        StageSpecification specification = StageSpecification.Create(
            stranger,
            shape.ParameterContract,
            shape.InputPorts,
            shape.OutputPorts,
            shape.ResultPorts,
            shape.RequiredCapabilities);
        StageNode node = StageNode.Create(
            NodeId.Create("conformance"),
            stranger,
            shape.ParameterContract,
            _samples[shape.Stage].Parameters);

        try
        {
            DataflowStageRuntime? built = _factory.Create(new DataflowStageRequest(node, specification));

            failures.Add(built is null
                ? $"the factory answered with nothing for '{stranger}', {what}, and a null runtime says neither that the stage was built nor why it could not be"
                : $"the factory built '{stranger}', {what}, instead of refusing it");
        }
        catch (Exception thrown) when (
            thrown is NullReferenceException or
                IndexOutOfRangeException or
                ArgumentOutOfRangeException or
                KeyNotFoundException or
                InvalidCastException)
        {
            failures.Add(
                $"the factory answered '{stranger}', {what}, with {thrown.GetType().Name}, which is an accident rather than a refusal");
        }
        catch (Exception thrown)
        {
            if (!thrown.Message.Contains(stranger.ToString(), StringComparison.Ordinal))
            {
                failures.Add(
                    $"the factory refused '{stranger}', {what}, without naming it: {thrown.Message}");
            }
        }
    }

    /// <summary>Checks that every runtime the factory builds has the shape its specification declares.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// A catalog cannot catch this and is not supposed to: a specification describes ports and says nothing
    /// about what a factory will build. The planner refuses the disagreement when a run is materialized,
    /// naming the node; this asks the same question of one stage at a time, so a provider learns it from its
    /// own test suite rather than from a graph somebody wrote.
    /// </remarks>
    private void CheckShapes(List<string> failures)
    {
        foreach (StageSpecification specification in _specifications)
        {
            StageRef stage = specification.Stage;

            if (Classify(specification) is not { } expected)
            {
                failures.Add(
                    $"the stage '{stage}' declares {specification.InputPorts.Count} inputs, {specification.OutputPorts.Count} outputs, and {specification.ResultPorts.Count} result ports, which is not a shape this engine runs");

                continue;
            }

            if (Build(failures, specification, _samples[stage].Parameters) is not { } built)
            {
                continue;
            }

            StageRuntime runtime = built.Runtime;

            switch (expected)
            {
                case ConformanceShape.Source when runtime.Shape is not StageRuntimeShape.Source:
                case ConformanceShape.Element when runtime.Shape
                    is not StageRuntimeShape.Element and not StageRuntimeShape.ElementAsync:
                case ConformanceShape.Terminal when runtime.Shape is not StageRuntimeShape.Terminal:
                case ConformanceShape.FanOut when runtime.Shape is not StageRuntimeShape.FanOut:
                case ConformanceShape.FanIn when runtime.Shape is not StageRuntimeShape.FanIn:
                    failures.Add(
                        $"the stage '{stage}' is declared as {Describe(expected)} and its factory built a {runtime.Shape} runtime, which the planner would refuse at the first run of a document naming it");

                    continue;
            }

            if (expected is ConformanceShape.Terminal &&
                runtime.ProducesResult != (specification.ResultPorts.Count == 1))
            {
                failures.Add(specification.ResultPorts.Count == 1
                    ? $"the stage '{stage}' declares the result port '{specification.ResultPorts[0].Id}' and its factory built a terminal that produces no result, so a document declaring a slot over it would never resolve one"
                    : $"the stage '{stage}' declares no result port and its factory built a terminal that produces a result, which nothing in a document could read");
            }

            if (expected is ConformanceShape.FanOut &&
                runtime.Splitting?.Halves is { } halves &&
                halves.Count != specification.OutputPorts.Count)
            {
                failures.Add(
                    $"the stage '{stage}' declares {specification.OutputPorts.Count} output ports and its factory splits a row into {halves.Count} parts");
            }
        }
    }

    /// <summary>Checks that every stage can be authored through a typed handle and only through the right one.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// The handles are the authoring half of the same statement the ports make: a stage becomes a value an
    /// author can write by pairing its specification with element contracts, and construction validates
    /// against the catalog immediately. What is checked is that the pairing the specification itself implies
    /// is accepted, that a handle of another shape is refused, and that a contract no port declares is
    /// refused — so an author's mistake is an <see cref="ArgumentException"/> at the line they wrote rather
    /// than a diagnostic when the graph closes.
    /// </para>
    /// <para>
    /// The handles are created over <see cref="object"/> because the kit does not know the provider's own
    /// CLR types and does not need to: a handle validates contract references, and the CLR type is the
    /// deployment's assertion about what carries one.
    /// </para>
    /// </remarks>
    private void CheckHandles(List<string> failures)
    {
        foreach (StageSpecification specification in _specifications)
        {
            StageRef stage = specification.Stage;

            if (Classify(specification) is not { } shape)
            {
                continue;
            }

            ContractReference absent = ContractReference.Create(ContractId.Create(AbsentContract), 1);

            switch (shape)
            {
                case ConformanceShape.Source:
                    Accepted(failures, stage, () => RegisteredStage.Source(_catalog, stage, Out(specification, 0)));
                    Refused(failures, stage, "a sink", () => RegisteredStage.Sink(_catalog, stage, Out(specification, 0)));
                    Refused(failures, stage, "a source over a contract it does not declare", () =>
                        RegisteredStage.Source(_catalog, stage, Element(absent)));
                    break;

                case ConformanceShape.Element:
                    Accepted(failures, stage, () =>
                        RegisteredStage.Flow(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    Refused(failures, stage, "a source", () =>
                        RegisteredStage.Source(_catalog, stage, Out(specification, 0)));
                    Refused(failures, stage, "a flow over a contract it does not declare", () =>
                        RegisteredStage.Flow(_catalog, stage, In(specification, 0), Element(absent)));
                    break;

                case ConformanceShape.Terminal when specification.ResultPorts.Count == 1:
                    Accepted(failures, stage, () => RegisteredStage.SinkWithResult(
                        _catalog,
                        stage,
                        In(specification, 0),
                        Result(specification)));
                    Refused(failures, stage, "a sink declaring no result", () =>
                        RegisteredStage.Sink(_catalog, stage, In(specification, 0)));
                    Refused(failures, stage, "a sink over a result contract it does not declare", () =>
                        RegisteredStage.SinkWithResult(
                            _catalog,
                            stage,
                            In(specification, 0),
                            ResultContract.For<object>(AbsentContract, 1)));
                    break;

                case ConformanceShape.Terminal:
                    Accepted(failures, stage, () => RegisteredStage.Sink(_catalog, stage, In(specification, 0)));
                    Refused(failures, stage, "a flow", () =>
                        RegisteredStage.Flow(_catalog, stage, In(specification, 0), In(specification, 0)));
                    Refused(failures, stage, "a sink over a contract it does not declare", () =>
                        RegisteredStage.Sink(_catalog, stage, Element(absent)));
                    break;

                case ConformanceShape.FanOut when Alike(specification.OutputPorts.Select(port => port.ElementContract)):
                    Accepted(failures, stage, () =>
                        RegisteredStage.FanOut(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    Refused(failures, stage, "a flow", () =>
                        RegisteredStage.Flow(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    break;

                case ConformanceShape.FanOut when specification.OutputPorts.Count == 2:
                    Accepted(failures, stage, () => RegisteredStage.FanOut(
                        _catalog,
                        stage,
                        In(specification, 0),
                        Out(specification, 0),
                        Out(specification, 1)));
                    Refused(failures, stage, "a fan-out whose legs carry one contract", () =>
                        RegisteredStage.FanOut(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    break;

                case ConformanceShape.FanOut:
                    failures.Add(
                        $"the stage '{stage}' routes to {specification.OutputPorts.Count} legs carrying more than one contract and fewer than one handle can declare, so no typed handle can author it");
                    break;

                case ConformanceShape.FanIn when Alike(specification.InputPorts.Select(port => port.ElementContract)):
                    Accepted(failures, stage, () =>
                        RegisteredStage.FanIn(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    Refused(failures, stage, "a flow", () =>
                        RegisteredStage.Flow(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    break;

                case ConformanceShape.FanIn when specification.InputPorts.Count == 2:
                    Accepted(failures, stage, () => RegisteredStage.FanIn(
                        _catalog,
                        stage,
                        In(specification, 0),
                        In(specification, 1),
                        Out(specification, 0)));
                    Refused(failures, stage, "a fan-in whose inputs carry one contract", () =>
                        RegisteredStage.FanIn(_catalog, stage, In(specification, 0), Out(specification, 0)));
                    break;

                default:
                    failures.Add(
                        $"the stage '{stage}' joins {specification.InputPorts.Count} inputs carrying more than one contract and fewer than one handle can declare, so no typed handle can author it");
                    break;
            }
        }
    }

    /// <summary>Requires a handle to be created.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="create">The creation.</param>
    private static void Accepted(List<string> failures, StageRef stage, Func<object> create)
    {
        try
        {
            _ = create();
        }
        catch (ArgumentException refused)
        {
            failures.Add(
                $"the stage '{stage}' has no typed handle: pairing it with its own specification's contracts is refused with {refused.Message}");
        }
    }

    /// <summary>Requires a handle of the wrong kind to be refused.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="what">The handle that should not be creatable, read after "as".</param>
    /// <param name="create">The creation.</param>
    private static void Refused(List<string> failures, StageRef stage, string what, Func<object> create)
    {
        try
        {
            _ = create();

            failures.Add(
                $"the stage '{stage}' can be authored as {what}, and handle creation is where a catalog mismatch is supposed to become an exception");
        }
        catch (ArgumentException)
        {
            // The refusal this check is looking for.
        }
    }

    /// <summary>Checks that no parameter payload carries something the CLR would resolve to a type.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// ADR 0001's rule, checked where it is easiest to break: a payload is the one part of a document a
    /// provider writes freely, and a CLR name in one is a document that causes code loading in whatever
    /// process reads it.
    /// </para>
    /// <para>
    /// Two clauses, because neither covers the other. The first is the CLR's own answer — a string the
    /// runtime turns into a <see cref="Type"/> is a type name whatever it looks like — and it sees only
    /// what this process can already resolve, which is the runtime library, this assembly, and anything
    /// assembly-qualified and loadable. The second is the shape of an assembly-qualified name, which
    /// catches the ones naming an assembly this process has not got: those are the dangerous half, because
    /// a document carrying one is a document written to be resolved somewhere else.
    /// </para>
    /// </remarks>
    private void CheckPayloadsNameNoType(List<string> failures)
    {
        string[] qualifiers = [", Version=", ", Culture=", ", PublicKeyToken="];

        foreach (StageSpecification specification in _specifications)
        {
            foreach (string text in Strings(_samples[specification.Stage].Parameters.ToElement()))
            {
                if (Type.GetType(text, throwOnError: false) is { } resolved)
                {
                    failures.Add(
                        $"the payload of the stage '{specification.Stage}' carries '{text}', which this process resolves to the CLR type {resolved.FullName}, and a document names no CLR type");
                }
                else if (Array.Exists(
                    qualifiers,
                    qualifier => text.Contains(qualifier, StringComparison.Ordinal)))
                {
                    failures.Add(
                        $"the payload of the stage '{specification.Stage}' carries '{text}', which is an assembly-qualified CLR type name, and a document names no CLR type");
                }
            }
        }
    }

    /// <summary>Checks that the core packages' option types name nothing of this provider.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// The M4 exit criterion, read mechanically: a provider configures its stages through the payloads its
    /// occurrences carry and the bindings a deployment registers, and never by adding a member to a type the
    /// core package ships. The direction matters and only one direction is checked — a provider is free to
    /// declare a core option type in its own API, which is how every adapter here states an ingress bound
    /// once rather than inventing a second spelling of "drop the oldest".
    /// </para>
    /// <para>
    /// <b>What this cannot see.</b> A provider that ships inside one of the core assemblies is compared
    /// against its own assembly, so the assembly half of the check is vacuous for it and only the namespace
    /// half says anything. That is stated rather than hidden: the check is sharp for a provider in a package
    /// of its own, which is the case the criterion is about.
    /// </para>
    /// <para>
    /// <b>Where the provider's code is looked for.</b> Ordinarily the factory's own CLR type, because a
    /// provider that writes an <see cref="IDataflowStageFactory"/> writes it in its own assembly. A
    /// <see cref="StageProvider"/> is the exception and has to be treated as one: its factory type is this
    /// library's, so measuring against it would compare the core packages with themselves and report every
    /// core option naming an <c>Orleans.Dataflow.Hosting</c> type as the provider's. The code such a
    /// vocabulary actually holds is the delegates it was declared with, so those are what is measured — one
    /// scope per declaring type, and the check is the same check applied to each.
    /// </para>
    /// </remarks>
    private void CheckCoreOptions(List<string> failures)
    {
        foreach (Assembly core in (Assembly[])
            [typeof(LocalDataflowHost).Assembly, typeof(StageSpecification).Assembly])
        {
            foreach (Type option in core.GetExportedTypes()
                .Where(static type => type.Name.EndsWith("Options", StringComparison.Ordinal)))
            {
                foreach (Type named in Named(option))
                {
                    foreach (Type implementation in Implementations())
                    {
                        Assembly provider = implementation.Assembly;
                        string? space = implementation.Namespace;

                        if (named.Assembly == provider &&
                            (provider != core ||
                                (space is not null &&
                                    string.Equals(named.Namespace, space, StringComparison.Ordinal))))
                        {
                            failures.Add(
                                $"the core option type {option.FullName} names {named.FullName}, which belongs to the provider '{_provider}', so a deployment configures this provider by setting a core option instead of by writing a payload");
                        }
                    }
                }
            }
        }
    }

    /// <summary>Lists the types that hold the provider's code.</summary>
    /// <returns>
    /// The declaring types of a <see cref="StageProvider"/>'s build delegates, or the factory's own type for
    /// every other factory.
    /// </returns>
    /// <remarks>
    /// A vocabulary that carries both halves has no CLR type of its own, so its factory type says nothing
    /// about the provider and its delegates say everything. A vocabulary that declares no stage at all yields
    /// nothing here and is caught long before this check, by the refusal that a catalog declaring no stage of
    /// the provider is not a subject.
    /// </remarks>
    private IEnumerable<Type> Implementations() =>
        _factory is StageProvider vocabulary ? vocabulary.ImplementationTypes : [_factory.GetType()];

    /// <summary>Lists every type one option type names on its public surface.</summary>
    /// <param name="option">The option type.</param>
    /// <returns>The named types, with generic arguments flattened.</returns>
    private static IEnumerable<Type> Named(Type option)
    {
        foreach (PropertyInfo property in option.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
        {
            foreach (Type named in Flatten(property.PropertyType))
            {
                yield return named;
            }
        }

        foreach (FieldInfo field in option.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
        {
            foreach (Type named in Flatten(field.FieldType))
            {
                yield return named;
            }
        }

        foreach (MethodBase member in option
            .GetConstructors()
            .Concat<MethodBase>(option.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)))
        {
            foreach (ParameterInfo parameter in member.GetParameters())
            {
                foreach (Type named in Flatten(parameter.ParameterType))
                {
                    yield return named;
                }
            }
        }
    }

    /// <summary>Lists a type and every type argument it carries.</summary>
    /// <param name="type">The type.</param>
    /// <returns>The type and its flattened generic arguments.</returns>
    private static IEnumerable<Type> Flatten(Type type)
    {
        yield return type;

        foreach (Type argument in type.GetGenericArguments())
        {
            foreach (Type named in Flatten(argument))
            {
                yield return named;
            }
        }
    }

    /// <summary>Asks the factory to build one node of one stage.</summary>
    /// <param name="failures">The report under construction.</param>
    /// <param name="specification">The specification.</param>
    /// <param name="parameters">The sample payload.</param>
    /// <returns>The runtime, or <see langword="null"/> when the factory did not build one.</returns>
    private DataflowStageRuntime? Build(
        List<string> failures,
        StageSpecification specification,
        CanonicalJsonValue parameters)
    {
        StageNode node = StageNode.Create(
            NodeId.Create("conformance"),
            specification.Stage,
            specification.ParameterContract,
            parameters);

        try
        {
            DataflowStageRuntime? built = _factory.Create(new DataflowStageRequest(node, specification));

            if (built is null)
            {
                failures.Add(
                    $"the factory answered with nothing for the stage '{specification.Stage}', which its catalog declares, and a null runtime says neither that the stage was built nor why it could not be");
            }

            return built;
        }
        catch (Exception thrown)
        {
            failures.Add(
                $"the factory refused the stage '{specification.Stage}', which its own catalog declares, with {thrown.GetType().Name}: {thrown.Message}");

            return null;
        }
    }

    /// <summary>Reads the executable shape a specification's ports imply.</summary>
    /// <param name="specification">The specification.</param>
    /// <returns>The shape, or <see langword="null"/> when the ports describe none the engine runs.</returns>
    private static ConformanceShape? Classify(StageSpecification specification)
    {
        int inputs = specification.InputPorts.Count;
        int outputs = specification.OutputPorts.Count;
        int results = specification.ResultPorts.Count;

        return (inputs, outputs, results) switch
        {
            (0, 1, 0) => ConformanceShape.Source,
            (1, 1, 0) => ConformanceShape.Element,
            (1, 0, 0) or (1, 0, 1) => ConformanceShape.Terminal,
            (1, >= 2, 0) => ConformanceShape.FanOut,
            (>= 2, 1, 0) => ConformanceShape.FanIn,
            _ => null,
        };
    }

    /// <summary>Names one shape for a diagnostic.</summary>
    /// <param name="shape">The shape.</param>
    /// <returns>The article and the noun.</returns>
    private static string Describe(ConformanceShape shape) => shape switch
    {
        ConformanceShape.Source => "a source",
        ConformanceShape.Element => "an element stage",
        ConformanceShape.Terminal => "a terminal",
        ConformanceShape.FanOut => "a fan-out",
        _ => "a fan-in",
    };

    /// <summary>Builds a stage reference of this provider that the catalog does not declare.</summary>
    /// <returns>The reference.</returns>
    private StageRef Absent()
    {
        string stage = UnknownStage;

        while (_catalog.TryGetSpecification(
            StageRef.Create(_provider, StageId.Create(stage), StageRef.FirstMajorVersion),
            out StageSpecification? _))
        {
            stage += "-x";
        }

        return StageRef.Create(_provider, StageId.Create(stage), StageRef.FirstMajorVersion);
    }

    /// <summary>Declares one input port's contract as a typed element contract over <see cref="object"/>.</summary>
    /// <param name="specification">The specification.</param>
    /// <param name="index">The port's position in the canonical order.</param>
    /// <returns>The declaration.</returns>
    private static ElementContract<object> In(StageSpecification specification, int index) =>
        Element(specification.InputPorts[index].ElementContract);

    /// <summary>Declares one output port's contract as a typed element contract over <see cref="object"/>.</summary>
    /// <param name="specification">The specification.</param>
    /// <param name="index">The port's position in the canonical order.</param>
    /// <returns>The declaration.</returns>
    private static ElementContract<object> Out(StageSpecification specification, int index) =>
        Element(specification.OutputPorts[index].ElementContract);

    /// <summary>Declares the single result port's contract as a typed result contract.</summary>
    /// <param name="specification">The specification.</param>
    /// <returns>The declaration.</returns>
    private static ResultContract<object> Result(StageSpecification specification) =>
        ResultContract.For<object>(
            specification.ResultPorts[0].ResultContract.Contract.Value,
            specification.ResultPorts[0].ResultContract.MajorVersion);

    /// <summary>Declares one contract reference as a typed element contract over <see cref="object"/>.</summary>
    /// <param name="contract">The reference.</param>
    /// <returns>The declaration.</returns>
    private static ElementContract<object> Element(ContractReference contract) =>
        ElementContract.For<object>(contract.Contract.Value, contract.MajorVersion);

    /// <summary>Determines whether every contract in a sequence is the same one.</summary>
    /// <param name="contracts">The contracts.</param>
    /// <returns><see langword="true"/> when they are all equal.</returns>
    private static bool Alike(IEnumerable<ContractReference> contracts)
    {
        ContractReference[] all = [.. contracts];

        return Array.TrueForAll(all, contract => contract == all[0]);
    }

    /// <summary>Rewrites one member of a payload object.</summary>
    /// <param name="payload">The payload.</param>
    /// <param name="member">The member to add, replace, or remove.</param>
    /// <param name="raw">The raw JSON of the new value, or <see langword="null"/> to remove the member.</param>
    /// <returns>The rewritten payload, in canonical form.</returns>
    private static CanonicalJsonValue Rewrite(JsonElement payload, string member, string? raw)
    {
        StringBuilder json = new("{");
        bool first = true;
        bool found = false;

        foreach (JsonProperty property in payload.EnumerateObject())
        {
            bool target = string.Equals(property.Name, member, StringComparison.Ordinal);
            found |= target;

            if (target && raw is null)
            {
                continue;
            }

            if (!first)
            {
                _ = json.Append(',');
            }

            _ = json.Append(JsonText.Quote(property.Name))
                .Append(':')
                .Append(target ? raw : property.Value.GetRawText());
            first = false;
        }

        if (!found && raw is not null)
        {
            if (!first)
            {
                _ = json.Append(',');
            }

            _ = json.Append(JsonText.Quote(member)).Append(':').Append(raw);
        }

        return CanonicalJsonValue.Parse(json.Append('}').ToString());
    }

    /// <summary>Answers a raw JSON value of a different kind from the one given.</summary>
    /// <param name="value">The value.</param>
    /// <returns>Raw JSON of another kind.</returns>
    /// <remarks>
    /// A number becomes text and everything else becomes a number, which is the smallest rule that always
    /// changes the kind: a reader that checks the kind of its member complains about either.
    /// </remarks>
    private static string Retyped(JsonElement value) =>
        value.ValueKind is JsonValueKind.Number ? "\"conformance\"" : "0";

    /// <summary>Renders the kind of a raw JSON value for a diagnostic.</summary>
    /// <param name="raw">The raw JSON.</param>
    /// <returns>An article and a noun.</returns>
    private static string Kind(string raw) => raw.Length == 0 ? "nothing" : raw[0] switch
    {
        '"' => "a string",
        '{' => "an object",
        '[' => "an array",
        't' or 'f' => "a boolean",
        'n' => "null",
        _ => "a number",
    };

    /// <summary>Lists every string a JSON value carries, at any depth.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The strings, including object member values but not member names.</returns>
    private static IEnumerable<string> Strings(JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.String:
                yield return value.GetString()!;
                break;

            case JsonValueKind.Object:
                foreach (JsonProperty member in value.EnumerateObject())
                {
                    foreach (string text in Strings(member.Value))
                    {
                        yield return text;
                    }
                }

                break;

            case JsonValueKind.Array:
                foreach (JsonElement element in value.EnumerateArray())
                {
                    foreach (string text in Strings(element))
                    {
                        yield return text;
                    }
                }

                break;

            default:
                break;
        }
    }

    /// <summary>The executable shape a specification's ports imply.</summary>
    private enum ConformanceShape
    {
        /// <summary>No inputs and one output.</summary>
        Source,

        /// <summary>One input and one output.</summary>
        Element,

        /// <summary>One input, no output, and at most one result port.</summary>
        Terminal,

        /// <summary>One input and several outputs.</summary>
        FanOut,

        /// <summary>Several inputs and one output.</summary>
        FanIn,
    }
}
