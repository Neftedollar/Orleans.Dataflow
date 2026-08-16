using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What an <see cref="ElementContract{T}"/> and a <see cref="ResultContract{TResult}"/> assert, and what
/// makes two of them the same assertion.
/// </summary>
/// <remarks>
/// The definition plane forbids CLR type names as contract identity, so a document stores only the
/// reference. These declarations are the process-local other half: "in this process, this contract is that
/// type". The tests below fix both halves — the reference the document gets, and the type binding that
/// never leaves the process but is part of what makes two declarations equal.
/// </remarks>
public sealed class RegisteredContractTests
{
    [Fact]
    public void ADeclarationCarriesTheReferenceItWasSpelledFrom()
    {
        ElementContract<OrderCreated> declared = ElementContract.For<OrderCreated>("order-created", 2);

        Assert.Equal(
            ContractReference.Create(ContractId.Create("order-created"), 2),
            declared.Reference);
        Assert.Equal(typeof(OrderCreated), declared.ElementType);
        Assert.False(declared.IsDefault);
    }

    [Fact]
    public void TwoDeclarationsOfOneContractOverOneTypeAreEqual()
    {
        ElementContract<OrderCreated> first = ElementContract.For<OrderCreated>("order-created", 1);
        ElementContract<OrderCreated> second = ElementContract.For<OrderCreated>("order-created", 1);

        Assert.Equal(first, second);
        Assert.True(first == second);
        Assert.False(first != second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

    [Fact]
    public void TwoDeclarationsOfOneContractOverDifferentTypesAreNotEqual()
    {
        // The whole point of the type argument. Two processes agreeing on 'order-created@v1' while binding
        // different CLR types is a deployment error the definition plane cannot see; within one process the
        // authoring layer refuses to conflate them, so a handle built for one is not a handle for the
        // other. The comparison has to be spelled through object, because the two are different types and
        // the equality operator does not even exist between them.
        object created = ElementContract.For<OrderCreated>("order-created", 1);
        object document = ElementContract.For<OrderDocument>("order-created", 1);

        Assert.NotEqual(created, document);
        Assert.False(created.Equals(document));
        Assert.False(document.Equals(created));
    }

    [Fact]
    public void TwoDeclarationsOfDifferentVersionsOfOneContractAreNotEqual()
    {
        Assert.NotEqual(
            ElementContract.For<OrderCreated>("order-created", 1),
            ElementContract.For<OrderCreated>("order-created", 2));
    }

    [Fact]
    public void AnElementDeclarationAndAResultDeclarationOverOneReferenceAreNotEqual()
    {
        // Element ports and result ports are different port kinds checked by different compiler rules, so
        // an element declaration is never accepted where a result declaration is required. Nothing in the
        // type system would stop the two from comparing equal if they were one type; they are not.
        object element = ElementContract.For<long>("order-count", 1);
        object result = ResultContract.For<long>("order-count", 1);

        Assert.NotEqual(element, result);
        Assert.False(element.Equals(result));
    }

    [Fact]
    public void ADefaultElementDeclarationNamesNothingAndSaysSo()
    {
        ElementContract<OrderCreated> declared = default;

        Assert.True(declared.IsDefault);
        Assert.Equal("(default ElementContract)", declared.ToString());
        Assert.Equal(typeof(OrderCreated), declared.ElementType);
        Assert.Throws<InvalidOperationException>(() => { _ = declared.Reference; });
    }

    [Fact]
    public void ADefaultResultDeclarationNamesNothingAndSaysSo()
    {
        ResultContract<long> declared = default;

        Assert.True(declared.IsDefault);
        Assert.Equal("(default ResultContract)", declared.ToString());
        Assert.Equal(typeof(long), declared.ResultType);
        Assert.Throws<InvalidOperationException>(() => { _ = declared.Reference; });
    }

    [Fact]
    public void ADeclarationRendersBothHalvesOfTheAssertion()
    {
        Assert.Equal(
            "order-created@v1 as OrderCreated",
            ElementContract.For<OrderCreated>("order-created", 1).ToString());
        Assert.Equal(
            "order-count@v1 as Int64",
            ResultContract.For<long>("order-count", 1).ToString());
    }

    [Fact]
    public void ANameThatIsNotAnIdentifierSegmentIsRejectedUnderTheParameterTheAuthorWrote()
    {
        ArgumentException rejected =
            Assert.Throws<ArgumentException>(() => ElementContract.For<OrderCreated>("Order Created", 1));

        Assert.Equal("contractId", rejected.ParamName);
        Assert.Contains("Order Created", rejected.Message, StringComparison.Ordinal);

        Assert.Equal(
            "contractId",
            Assert.Throws<ArgumentException>(() => ResultContract.For<long>("Order Count", 1)).ParamName);
    }

    [Fact]
    public void ANullNameIsRejected()
    {
        Assert.Equal(
            "contractId",
            Assert.Throws<ArgumentNullException>(() => ElementContract.For<OrderCreated>(null!, 1)).ParamName);
        Assert.Equal(
            "contractId",
            Assert.Throws<ArgumentNullException>(() => ResultContract.For<long>(null!, 1)).ParamName);
    }

    [Fact]
    public void AMajorVersionBelowOneIsRejected()
    {
        Assert.Equal(
            "majorVersion",
            Assert.Throws<ArgumentOutOfRangeException>(
                () => ElementContract.For<OrderCreated>("order-created", 0)).ParamName);
        Assert.Equal(
            "majorVersion",
            Assert.Throws<ArgumentOutOfRangeException>(
                () => ResultContract.For<long>("order-count", -1)).ParamName);
    }
}
