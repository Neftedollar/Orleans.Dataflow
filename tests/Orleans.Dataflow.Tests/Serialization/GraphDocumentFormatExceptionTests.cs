using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests for the shape of <see cref="GraphDocumentFormatException"/>.
/// </summary>
public sealed class GraphDocumentFormatExceptionTests
{
    [Fact]
    public void TheTypeIsSealedAndPublic()
    {
        Type type = typeof(GraphDocumentFormatException);

        Assert.True(type.IsSealed);
        Assert.True(type.IsPublic);
        Assert.Equal(typeof(Exception), type.BaseType);
    }

    [Fact]
    public void TheParameterlessConstructorProducesADefaultMessage()
    {
        GraphDocumentFormatException exception = new();

        Assert.False(string.IsNullOrEmpty(exception.Message));
        Assert.Null(exception.InnerException);
    }

    [Fact]
    public void TheMessageConstructorKeepsItsMessage()
    {
        GraphDocumentFormatException exception = new("$.nodes[0]: the rule was broken.");

        Assert.Equal("$.nodes[0]: the rule was broken.", exception.Message);
        Assert.Null(exception.InnerException);
    }

    [Fact]
    public void TheInnerExceptionConstructorKeepsBoth()
    {
        ArgumentException inner = new("the document model said so");
        GraphDocumentFormatException exception = new("$: the rule was broken.", inner);

        Assert.Equal("$: the rule was broken.", exception.Message);
        Assert.Same(inner, exception.InnerException);
    }
}
