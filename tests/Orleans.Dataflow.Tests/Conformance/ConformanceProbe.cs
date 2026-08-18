using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Conformance;

/// <summary>
/// A provider whose every part can be broken on purpose, so that each conformance check can be shown to
/// have something to catch.
/// </summary>
/// <remarks>
/// <para>
/// The kit passes against both vocabularies this repository ships, which is the result it was built for and
/// is also exactly what a kit that measured nothing would produce. So the kit is checked the way any
/// instrument is: by pointing it at something known to be wrong in one specific way and requiring it to say
/// so, one way per check.
/// </para>
/// <para>
/// Everything here is built through the ordinary public factories, so a defect that cannot be constructed
/// is a defect the definition model already forbids — which is worth knowing, and is why the checks that
/// have no negative test in the sibling suite are listed there by name rather than left to be inferred.
/// </para>
/// </remarks>
internal static class ConformanceProbe
{
    /// <summary>The provider every probe stage belongs to.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create("conformance-probe");

    /// <summary>Gets the payload every probe stage's reader accepts.</summary>
    internal static CanonicalJsonValue Payload { get; } =
        CanonicalJsonValue.Parse("""{"name":"probe","size":3}""");

    /// <summary>Builds a probe stage reference.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The reference at major version 1.</returns>
    internal static StageRef Stage(string stage) =>
        StageRef.Create(Provider, StageId.Create(stage), StageRef.FirstMajorVersion);

    /// <summary>Builds a one-stage catalog.</summary>
    /// <param name="specification">The specification.</param>
    /// <returns>The catalog.</returns>
    internal static StageCatalog Catalog(StageSpecification specification) =>
        StageCatalog.Create([specification]);

    /// <summary>Declares a source: no input, one output.</summary>
    /// <param name="reader">The parameter validator, or <see langword="null"/> for a stage with none.</param>
    /// <returns>The specification.</returns>
    internal static StageSpecification Source(IStageParameterValidator? reader = null) =>
        Declare(Stage("source"), [], [Output("out")], [], reader);

    /// <summary>Declares a flow: one input, one output.</summary>
    /// <returns>The specification.</returns>
    internal static StageSpecification Flow() =>
        Declare(Stage("flow"), [Input("in")], [Output("out")], [], new ProbeReader());

    /// <summary>Declares a terminal that yields a result.</summary>
    /// <returns>The specification.</returns>
    internal static StageSpecification CountingSink() =>
        Declare(Stage("counting-sink"), [Input("in")], [], [Result("total")], new ProbeReader());

    /// <summary>Declares a stage with no port at all.</summary>
    /// <returns>The specification.</returns>
    internal static StageSpecification Portless() =>
        Declare(Stage("portless"), [], [], [], new ProbeReader());

    /// <summary>Declares a fan-out with three legs carrying two contracts.</summary>
    /// <returns>The specification.</returns>
    /// <remarks>
    /// The one junction shape no typed handle can author: the like-legged factory needs every leg to carry
    /// one contract and the unlike-legged one takes exactly two legs, so three legs of two contracts falls
    /// between them. It is a real hole in the authoring surface rather than an invented defect, which is
    /// what makes it the honest thing to point the handle check at.
    /// </remarks>
    internal static StageSpecification Wide() =>
        Declare(
            Stage("wide"),
            [Input("in")],
            [Output("leg-a"), Output("leg-b"), Output("leg-c", "probe-other")],
            [],
            new ProbeReader());

    /// <summary>Builds a specification.</summary>
    /// <param name="stage">The stage reference.</param>
    /// <param name="inputs">The input ports.</param>
    /// <param name="outputs">The output ports.</param>
    /// <param name="results">The result ports.</param>
    /// <param name="reader">The parameter validator, or <see langword="null"/>.</param>
    /// <returns>The specification.</returns>
    private static StageSpecification Declare(
        StageRef stage,
        IEnumerable<InputPortSpecification> inputs,
        IEnumerable<OutputPortSpecification> outputs,
        IEnumerable<ResultPortSpecification> results,
        IStageParameterValidator? reader) =>
        reader is null
            ? StageSpecification.Create(stage, inputs, outputs, results, Parameters, [])
            : StageSpecification.Create(stage, inputs, outputs, results, Parameters, [], reader);

    /// <summary>Gets the parameter contract every probe stage declares.</summary>
    private static ContractReference Parameters { get; } =
        ContractReference.Create(ContractId.Create("probe-parameters"), 1);

    /// <summary>Builds an input port.</summary>
    /// <param name="port">The port name.</param>
    /// <returns>The port specification.</returns>
    private static InputPortSpecification Input(string port) =>
        InputPortSpecification.Create(PortId.Create(port), Contract("probe-element"));

    /// <summary>Builds an output port.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The element contract identifier text.</param>
    /// <returns>The port specification.</returns>
    private static OutputPortSpecification Output(string port, string contract = "probe-element") =>
        OutputPortSpecification.Create(PortId.Create(port), Contract(contract));

    /// <summary>Builds a result port.</summary>
    /// <param name="port">The port name.</param>
    /// <returns>The port specification.</returns>
    private static ResultPortSpecification Result(string port) =>
        ResultPortSpecification.Create(PortId.Create(port), Contract("probe-count"));

    /// <summary>Builds a contract reference at major version 1.</summary>
    /// <param name="contract">The contract identifier text.</param>
    /// <returns>The reference.</returns>
    private static ContractReference Contract(string contract) =>
        ContractReference.Create(ContractId.Create(contract), 1);
}

/// <summary>
/// The probe vocabulary's parameter reader, in a correct form and in two broken ones.
/// </summary>
/// <param name="ignoresUnknownMembers">Whether it lets through a member the stage does not declare.</param>
/// <param name="namesNothing">Whether its refusals name the member that is wrong.</param>
/// <param name="ignoresMissingSize">Whether it accepts a payload with no <c>size</c> member at all.</param>
internal sealed class ProbeReader(
    bool ignoresUnknownMembers = false,
    bool namesNothing = false,
    bool ignoresMissingSize = false)
    : IStageParameterValidator
{
    /// <inheritdoc/>
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
    {
        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            return ["the payload is not a JSON object"];
        }

        JsonElement payload = parameters.ToElement();
        List<string> violations = [];

        Member(payload, "name", JsonValueKind.String, violations);

        if (!ignoresMissingSize || payload.TryGetProperty("size", out JsonElement _))
        {
            Member(payload, "size", JsonValueKind.Number, violations);
        }

        if (!ignoresUnknownMembers)
        {
            foreach (JsonProperty member in payload.EnumerateObject())
            {
                if (member.Name is not "name" and not "size")
                {
                    violations.Add(namesNothing
                        ? "the payload carries a member this stage does not declare"
                        : $"the member '{member.Name}' is not one this stage declares");
                }
            }
        }

        return violations;
    }

    /// <summary>Checks one member's presence and kind.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="kind">The kind the member has to be.</param>
    /// <param name="violations">The report under construction.</param>
    private void Member(JsonElement payload, string member, JsonValueKind kind, List<string> violations)
    {
        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(namesNothing ? "a member is missing" : $"the member '{member}' is missing");
        }
        else if (declared.ValueKind != kind)
        {
            violations.Add(namesNothing
                ? "a member is of the wrong kind"
                : $"the member '{member}' is of the wrong kind, and it is {kind}");
        }
    }
}

/// <summary>
/// A stage factory whose whole behaviour is the function it was constructed with.
/// </summary>
/// <param name="build">What to answer for one request.</param>
/// <remarks>
/// The falsification suite needs a factory that is wrong in one specific way per test, and a delegate is the
/// smallest thing that expresses "wrong in exactly this way" without a class per defect.
/// </remarks>
internal sealed class ProbeFactory(Func<DataflowStageRequest, DataflowStageRuntime> build)
    : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request) => build(request);

    /// <summary>Builds a factory that answers correctly for every probe stage.</summary>
    /// <returns>The factory.</returns>
    internal static ProbeFactory Correct() => new(Build);

    /// <summary>Answers one request the way the probe catalog declares it.</summary>
    /// <param name="request">The request.</param>
    /// <returns>The runtime.</returns>
    /// <remarks>
    /// The dispatch is on the whole stage reference rather than on the stage identifier, which is what both
    /// shipped vocabularies do and what the kit's stranger check requires: a major version is compatibility
    /// identity, so a factory matching on the identifier alone would build version one's behaviour for a
    /// document written against version two.
    /// </remarks>
    internal static DataflowStageRuntime Build(DataflowStageRequest request)
    {
        StageRef stage = request.Node.Stage;

        if (stage == ConformanceProbe.Stage("source"))
        {
            return DataflowStageRuntime.Source(static _ => Nothing());
        }

        if (stage == ConformanceProbe.Stage("flow") || stage == ConformanceProbe.Stage("portless"))
        {
            return DataflowStageRuntime.Element(static element => element);
        }

        if (stage == ConformanceProbe.Stage("counting-sink"))
        {
            return DataflowStageRuntime.Terminal(
                static () => 0L,
                static (state, _) => (long)state! + 1L,
                finish: null,
                producesResult: true);
        }

        return stage == ConformanceProbe.Stage("wide")
            ? DataflowStageRuntime.Broadcast()
            : throw new NotSupportedException(
                $"The probe provider does not implement the stage '{stage}'.");
    }

    /// <summary>Opens a sequence with nothing in it.</summary>
    /// <returns>The empty sequence.</returns>
#pragma warning disable CS1998 // An async iterator with no await is the shortest empty IAsyncEnumerable.
    private static async IAsyncEnumerable<object?> Nothing()
#pragma warning restore CS1998
    {
        yield break;
    }
}
