using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="StageSpecification"/>.
/// </summary>
public sealed class StageSpecificationTests
{
    private static readonly StageRef SampleStage =
        StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 2);

    private static readonly ContractReference SampleParameterContract =
        ContractReference.Create(ContractId.Create("map-parameters"), 3);

    private static readonly ContractReference ElementContract =
        ContractReference.Create(ContractId.Create("order"), 1);

    private static readonly ContractReference ResultContract =
        ContractReference.Create(ContractId.Create("counter-result"), 1);

    [Fact]
    public void CreateRoundTripsARepresentativeSpecification()
    {
        StageSpecification specification = Representative();

        Assert.Equal(SampleStage, specification.Stage);
        Assert.Equal(SampleParameterContract, specification.ParameterContract);
        Assert.Equal(["in", "side"], specification.InputPorts.Select(port => port.Id.Value));
        Assert.Equal(["out", "trace"], specification.OutputPorts.Select(port => port.Id.Value));
        Assert.Equal(["count"], specification.ResultPorts.Select(port => port.Id.Value));
        Assert.Equal(["nondeployable"], specification.RequiredCapabilities.Select(token => token.Value));
        Assert.True(specification.InputPorts[1].IsOptional);
        Assert.True(specification.OutputPorts[1].IsIgnorable);
        Assert.Equal(ElementContract, specification.InputPorts[0].ElementContract);
        Assert.Equal(ResultContract, specification.ResultPorts[0].ResultContract);
        Assert.Null(specification.ParameterValidator);
    }

    [Fact]
    public void CreateAcceptsAStageThatDeclaresNothingAtAll()
    {
        StageSpecification specification = StageSpecification.Create(
            SampleStage,
            [],
            [],
            [],
            SampleParameterContract,
            []);

        Assert.Empty(specification.InputPorts);
        Assert.Empty(specification.OutputPorts);
        Assert.Empty(specification.ResultPorts);
        Assert.Empty(specification.RequiredCapabilities);
    }

    [Fact]
    public void CreateOrdersEveryCollectionCanonically()
    {
        StageSpecification specification = StageSpecification.Create(
            SampleStage,
            [Input("side"), Input("in"), Input("aux")],
            [Output("trace"), Output("out")],
            [Result("total"), Result("count")],
            SampleParameterContract,
            [Capability("zeta"), Capability("alpha"), Capability("mid")]);

        Assert.Equal(["aux", "in", "side"], specification.InputPorts.Select(port => port.Id.Value));
        Assert.Equal(["out", "trace"], specification.OutputPorts.Select(port => port.Id.Value));
        Assert.Equal(["count", "total"], specification.ResultPorts.Select(port => port.Id.Value));
        Assert.Equal(["alpha", "mid", "zeta"], specification.RequiredCapabilities.Select(token => token.Value));
    }

    [Fact]
    public void PermutedInputsProduceEqualSpecificationsWithIdenticalElementOrder()
    {
        StageSpecification first = Representative();
        StageSpecification second = StageSpecification.Create(
            SampleStage,
            [Input("side", isOptional: true), Input("in")],
            [Output("trace", isIgnorable: true), Output("out")],
            [Result("count")],
            SampleParameterContract,
            [CapabilityToken.Nondeployable]);

        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.Equal(first.InputPorts, second.InputPorts);
        Assert.Equal(first.OutputPorts, second.OutputPorts);
        Assert.Equal(first.ResultPorts, second.ResultPorts);
        Assert.Equal(first.RequiredCapabilities, second.RequiredCapabilities);
    }

    [Fact]
    public void SpecificationsDifferingInAnyDeclaredMemberAreNotEqual()
    {
        StageSpecification specification = Representative();

        Assert.NotEqual(
            specification,
            StageSpecification.Create(
                StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 3),
                [Input("in"), Input("side", isOptional: true)],
                [Output("out"), Output("trace", isIgnorable: true)],
                [Result("count")],
                SampleParameterContract,
                [CapabilityToken.Nondeployable]));

        Assert.NotEqual(
            specification,
            StageSpecification.Create(
                SampleStage,
                [Input("in"), Input("side")],
                [Output("out"), Output("trace", isIgnorable: true)],
                [Result("count")],
                SampleParameterContract,
                [CapabilityToken.Nondeployable]));

        Assert.NotEqual(
            specification,
            StageSpecification.Create(
                SampleStage,
                [Input("in"), Input("side", isOptional: true)],
                [Output("out"), Output("trace", isIgnorable: true)],
                [Result("count")],
                ContractReference.Create(ContractId.Create("map-parameters"), 4),
                [CapabilityToken.Nondeployable]));

        Assert.NotEqual(
            specification,
            StageSpecification.Create(
                SampleStage,
                [Input("in"), Input("side", isOptional: true)],
                [Output("out"), Output("trace", isIgnorable: true)],
                [Result("count")],
                SampleParameterContract,
                []));
    }

    [Fact]
    public void ValidatorIsCarriedButTakesNoPartInEqualityOrHashing()
    {
        StageSpecification withoutValidator = Minimal();
        StageSpecification withOneValidator = MinimalWith(new AcceptingValidator());
        StageSpecification withAnotherValidator = MinimalWith(new RejectingValidator());

        Assert.NotNull(withOneValidator.ParameterValidator);
        Assert.NotSame(withOneValidator.ParameterValidator, withAnotherValidator.ParameterValidator);

        // Behavior is not shape. The three specifications declare one stage contract, serialize to one
        // byte string, and therefore share one identity; making equality depend on a validator instance
        // would split values that agree on every byte of that identity.
        Assert.Equal(withoutValidator, withOneValidator);
        Assert.Equal(withOneValidator, withAnotherValidator);
        Assert.Equal(withoutValidator.GetHashCode(), withAnotherValidator.GetHashCode());
    }

    [Fact]
    public void SpecificationsCarryingOneValidatorStillDifferWhenTheirShapesDiffer()
    {
        AcceptingValidator validator = new();

        Assert.NotEqual(MinimalWith(validator), Representative());
    }

    [Fact]
    public void ToStringSummarizesTheStageAndItsPortCounts() =>
        Assert.Equal("orleans-core/map-async@v2 (2 in, 2 out, 1 result)", Representative().ToString());

    [Theory]
    [InlineData("inputPorts")]
    [InlineData("outputPorts")]
    [InlineData("resultPorts")]
    [InlineData("requiredCapabilities")]
    public void CreateRejectsANullSequence(string parameterName)
    {
        Assert.Throws<ArgumentNullException>(
            parameterName,
            () =>
            {
                _ = StageSpecification.Create(
                    SampleStage,
                    parameterName == "inputPorts" ? null! : [],
                    parameterName == "outputPorts" ? null! : [],
                    parameterName == "resultPorts" ? null! : [],
                    SampleParameterContract,
                    parameterName == "requiredCapabilities" ? null! : []);
            });
    }

    [Fact]
    public void CreateRejectsANullValidator() =>
        Assert.Throws<ArgumentNullException>(
            "parameterValidator",
            () =>
            {
                _ = StageSpecification.Create(
                    SampleStage,
                    [],
                    [],
                    [],
                    SampleParameterContract,
                    [],
                    null!);
            });

    [Fact]
    public void CreateRejectsADefaultStageReference()
    {
        string message = Rejection(default, [], [], [], SampleParameterContract, []);

        Assert.Contains("the stage reference is the default StageRef", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultParameterContract()
    {
        string message = Rejection(SampleStage, [], [], [], default, []);

        Assert.Contains(
            "the parameter contract is the default ContractReference",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultInputPort()
    {
        string message = Rejection(inputPorts: [Input("in"), default]);

        Assert.Contains("inputPorts[1] is the default InputPortSpecification", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultOutputPort()
    {
        string message = Rejection(outputPorts: [default]);

        Assert.Contains("outputPorts[0] is the default OutputPortSpecification", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultResultPort()
    {
        string message = Rejection(resultPorts: [default]);

        Assert.Contains("resultPorts[0] is the default ResultPortSpecification", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultCapabilityToken()
    {
        string message = Rejection(requiredCapabilities: [Capability("alpha"), default]);

        Assert.Contains(
            "requiredCapabilities[1] is the default CapabilityToken",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateCapabilityToken()
    {
        string message = Rejection(requiredCapabilities: [Capability("alpha"), Capability("alpha")]);

        Assert.Contains(
            "requiredCapabilities[1] repeats the capability token 'alpha'",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsARepeatedInputPortName()
    {
        string message = Rejection(inputPorts: [Input("in"), Input("in")]);

        Assert.Contains("inputPorts[1] repeats the port id 'in'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnOutputPortNameAnInputPortAlreadyTook()
    {
        string message = Rejection(inputPorts: [Input("shared")], outputPorts: [Output("shared")]);

        Assert.Contains("outputPorts[0] repeats the port id 'shared'", message, StringComparison.Ordinal);
        Assert.Contains(
            "port ids are unique across the whole stage specification, inputs, outputs, and result ports together",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAResultPortNameAnInputPortAlreadyTook()
    {
        string message = Rejection(inputPorts: [Input("shared")], resultPorts: [Result("shared")]);

        Assert.Contains("resultPorts[0] repeats the port id 'shared'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAResultPortNameAnOutputPortAlreadyTook()
    {
        string message = Rejection(outputPorts: [Output("shared")], resultPorts: [Result("shared")]);

        Assert.Contains("resultPorts[0] repeats the port id 'shared'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsEveryViolationAtOnce()
    {
        string message = Rejection(
            default,
            [Input("shared"), default],
            [Output("shared")],
            [Result("shared")],
            default,
            [Capability("alpha"), Capability("alpha")]);

        Assert.Contains("The stage specification breaks 6 invariants:", message, StringComparison.Ordinal);
        Assert.Contains("1. the stage reference is the default StageRef", message, StringComparison.Ordinal);
        Assert.Contains("2. inputPorts[1] is the default InputPortSpecification", message, StringComparison.Ordinal);
        Assert.Contains("3. outputPorts[0] repeats the port id 'shared'", message, StringComparison.Ordinal);
        Assert.Contains("4. resultPorts[0] repeats the port id 'shared'", message, StringComparison.Ordinal);
        Assert.Contains("5. the parameter contract is the default ContractReference", message, StringComparison.Ordinal);
        Assert.Contains(
            "6. requiredCapabilities[1] repeats the capability token 'alpha'",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateNumbersASingleViolationInTheSingularForm()
    {
        string message = Rejection(default, [], [], [], SampleParameterContract, []);

        Assert.Contains("The stage specification breaks 1 invariant:", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectionsAreReadOnlyAndAreNotTheUnderlyingArrays()
    {
        StageSpecification specification = Representative();

        Assert.IsNotType<InputPortSpecification[]>(specification.InputPorts);
        Assert.IsNotType<OutputPortSpecification[]>(specification.OutputPorts);
        Assert.IsNotType<ResultPortSpecification[]>(specification.ResultPorts);
        Assert.IsNotType<CapabilityToken[]>(specification.RequiredCapabilities);

        IList<InputPortSpecification> inputs =
            Assert.IsAssignableFrom<IList<InputPortSpecification>>(specification.InputPorts);

        Assert.True(inputs.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => inputs.Add(Input("intruder")));
    }

    [Fact]
    public void CreateCopiesEverySequenceOnce()
    {
        List<InputPortSpecification> inputs = [Input("in")];
        StageSpecification specification = StageSpecification.Create(
            SampleStage,
            inputs,
            [],
            [],
            SampleParameterContract,
            []);

        inputs.Add(Input("added-afterwards"));

        Assert.Equal(["in"], specification.InputPorts.Select(port => port.Id.Value));
    }

    /// <summary>Builds the representative specification used by several tests.</summary>
    /// <returns>A stage with all three port kinds and one required capability.</returns>
    private static StageSpecification Representative() =>
        StageSpecification.Create(
            SampleStage,
            [Input("in"), Input("side", isOptional: true)],
            [Output("out"), Output("trace", isIgnorable: true)],
            [Result("count")],
            SampleParameterContract,
            [CapabilityToken.Nondeployable]);

    /// <summary>Builds a specification that declares nothing but its stage and parameter contract.</summary>
    /// <returns>The minimal specification.</returns>
    private static StageSpecification Minimal() =>
        StageSpecification.Create(SampleStage, [], [], [], SampleParameterContract, []);

    /// <summary>Builds the minimal specification with a validator attached.</summary>
    /// <param name="validator">The validator to attach.</param>
    /// <returns>The minimal specification, which differs from <see cref="Minimal"/> only in behavior.</returns>
    private static StageSpecification MinimalWith(IStageParameterValidator validator) =>
        StageSpecification.Create(SampleStage, [], [], [], SampleParameterContract, [], validator);

    /// <summary>Builds an input port on the sample element contract.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="isOptional">Whether the port may be left unconnected.</param>
    /// <returns>The port specification.</returns>
    private static InputPortSpecification Input(string port, bool isOptional = false) =>
        InputPortSpecification.Create(PortId.Create(port), ElementContract, isOptional);

    /// <summary>Builds an output port on the sample element contract.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="isIgnorable">Whether the port may be left unconnected.</param>
    /// <returns>The port specification.</returns>
    private static OutputPortSpecification Output(string port, bool isIgnorable = false) =>
        OutputPortSpecification.Create(PortId.Create(port), ElementContract, isIgnorable);

    /// <summary>Builds a result port on the sample result contract.</summary>
    /// <param name="port">The port name.</param>
    /// <returns>The port specification.</returns>
    private static ResultPortSpecification Result(string port) =>
        ResultPortSpecification.Create(PortId.Create(port), ResultContract);

    /// <summary>Builds a capability token from its text.</summary>
    /// <param name="value">The token text.</param>
    /// <returns>The token.</returns>
    private static CapabilityToken Capability(string value) => CapabilityToken.Create(value);

    /// <summary>
    /// Asserts that a candidate specification with the sample stage and contract is rejected.
    /// </summary>
    /// <param name="inputPorts">The candidate input ports.</param>
    /// <param name="outputPorts">The candidate output ports.</param>
    /// <param name="resultPorts">The candidate result ports.</param>
    /// <param name="requiredCapabilities">The candidate required capabilities.</param>
    /// <returns>The message of the thrown <see cref="ArgumentException"/>.</returns>
    private static string Rejection(
        IEnumerable<InputPortSpecification>? inputPorts = null,
        IEnumerable<OutputPortSpecification>? outputPorts = null,
        IEnumerable<ResultPortSpecification>? resultPorts = null,
        IEnumerable<CapabilityToken>? requiredCapabilities = null) =>
        Rejection(
            SampleStage,
            inputPorts ?? [],
            outputPorts ?? [],
            resultPorts ?? [],
            SampleParameterContract,
            requiredCapabilities ?? []);

    /// <summary>
    /// Asserts that a candidate specification is rejected and returns the rejection message.
    /// </summary>
    /// <param name="stage">The candidate stage reference.</param>
    /// <param name="inputPorts">The candidate input ports.</param>
    /// <param name="outputPorts">The candidate output ports.</param>
    /// <param name="resultPorts">The candidate result ports.</param>
    /// <param name="parameterContract">The candidate parameter contract.</param>
    /// <param name="requiredCapabilities">The candidate required capabilities.</param>
    /// <returns>The message of the thrown <see cref="ArgumentException"/>.</returns>
    /// <remarks>
    /// The exception carries no parameter name, because the invariants are relations between arguments
    /// rather than properties of one of them. That is asserted here so every rejection test states it.
    /// </remarks>
    private static string Rejection(
        StageRef stage,
        IEnumerable<InputPortSpecification> inputPorts,
        IEnumerable<OutputPortSpecification> outputPorts,
        IEnumerable<ResultPortSpecification> resultPorts,
        ContractReference parameterContract,
        IEnumerable<CapabilityToken> requiredCapabilities)
    {
        ArgumentException exception = Assert.ThrowsAny<ArgumentException>(
            () =>
            {
                _ = StageSpecification.Create(
                    stage,
                    inputPorts,
                    outputPorts,
                    resultPorts,
                    parameterContract,
                    requiredCapabilities);
            });

        Assert.IsType<ArgumentException>(exception);
        Assert.Null(exception.ParamName);

        return exception.Message;
    }

    /// <summary>A validator that accepts every payload.</summary>
    private sealed class AcceptingValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) => [];
    }

    /// <summary>A validator that rejects every payload.</summary>
    private sealed class RejectingValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) => ["the payload is refused"];
    }
}
