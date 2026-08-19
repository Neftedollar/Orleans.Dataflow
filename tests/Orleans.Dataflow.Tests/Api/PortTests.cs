using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What <see cref="Port"/> declares, and what it refuses.
/// </summary>
/// <remarks>
/// <para>
/// These factories are shorthand and nothing else: every one of them must produce exactly what the port
/// specification's own <c>Create</c> produces from the same name and contract. That equivalence is what the
/// first tests fix, because a shorthand that quietly declared something else would be a second meaning
/// rather than a second spelling.
/// </para>
/// <para>
/// The rest are about the diagnostics. An author here writes a port name as text and a contract as a typed
/// declaration, so a bad name has to be reported against <c>name</c> rather than against the
/// <see cref="PortId"/> parameter the author never wrote, and a default declaration has to be an argument
/// failure rather than the <see cref="InvalidOperationException"/> that reading its reference would raise.
/// </para>
/// </remarks>
public sealed class PortTests
{
    private static readonly ElementContract<OrderCreated> Element =
        ElementContract.For<OrderCreated>("order-created", 1);

    private static readonly ResultContract<long> Result = ResultContract.For<long>("order-count", 1);

    [Fact]
    public void ATypedPortDeclaresWhatTheSpecificationFactoryDeclares()
    {
        Assert.Equal(
            InputPortSpecification.Create(PortId.Create("in"), Element.Reference),
            Port.In("in", Element));
        Assert.Equal(
            OutputPortSpecification.Create(PortId.Create("out"), Element.Reference),
            Port.Out("out", Element));
        Assert.Equal(
            ResultPortSpecification.Create(PortId.Create("total"), Result.Reference),
            Port.Result("total", Result));
    }

    [Fact]
    public void AReferencePortDeclaresWhatTheTypedOneDeclares()
    {
        Assert.Equal(Port.In("in", Element), Port.In("in", Element.Reference));
        Assert.Equal(Port.Out("out", Element), Port.Out("out", Element.Reference));
        Assert.Equal(Port.Result("total", Result), Port.Result("total", Result.Reference));
    }

    [Fact]
    public void APortIsRequiredAndConsumedUnlessItSaysOtherwise()
    {
        Assert.False(Port.In("in", Element).IsOptional);
        Assert.True(Port.In("side", Element, isOptional: true).IsOptional);
        Assert.False(Port.In("side", Element.Reference, isOptional: false).IsOptional);

        Assert.False(Port.Out("out", Element).IsIgnorable);
        Assert.True(Port.Out("trace", Element, isIgnorable: true).IsIgnorable);
        Assert.False(Port.Out("trace", Element.Reference, isIgnorable: false).IsIgnorable);
    }

    [Theory]
    [InlineData("In")]
    [InlineData("out ")]
    [InlineData("")]
    [InlineData("-out")]
    public void APortNameThatBreaksTheGrammarIsReportedAgainstTheNameItWasWrittenAs(string name)
    {
        ArgumentException failure =
            Assert.Throws<ArgumentException>(nameof(name), () => Port.Out(name, Element));

        // PortId owns the grammar and its diagnostic, so the sentence is the one it writes; only the
        // parameter is corrected to the one the author actually filled in.
        Assert.Contains(nameof(PortId), failure.Message, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(failure.InnerException);
    }

    [Fact]
    public void ANullPortNameIsReportedAgainstTheNameItWasWrittenAs() =>
        Assert.Throws<ArgumentNullException>("name", () => Port.In(null!, Element));

    [Fact]
    public void ADefaultContractDeclarationIsAnArgumentFailureRatherThanAReadOfNothing()
    {
        Assert.Throws<ArgumentException>("contract", () => Port.In("in", default(ElementContract<OrderCreated>)));
        Assert.Throws<ArgumentException>("contract", () => Port.Out("out", default(ElementContract<OrderCreated>)));
        Assert.Throws<ArgumentException>("contract", () => Port.Result("total", default(ResultContract<long>)));
    }

    [Fact]
    public void ADefaultContractReferenceIsRefusedByThePortSpecificationItself()
    {
        Assert.Throws<ArgumentException>("elementContract", () => Port.In("in", default(ContractReference)));
        Assert.Throws<ArgumentException>("elementContract", () => Port.Out("out", default(ContractReference)));
        Assert.Throws<ArgumentException>("resultContract", () => Port.Result("total", default(ContractReference)));
    }
}
