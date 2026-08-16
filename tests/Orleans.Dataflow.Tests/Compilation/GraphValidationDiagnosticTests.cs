using Orleans.Dataflow.Compilation;
using Xunit;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// Tests for <see cref="GraphValidationDiagnostic"/>.
/// </summary>
public sealed class GraphValidationDiagnosticTests
{
    [Fact]
    public void CreateRoundTripsARuleAndAMessageWithoutASubject()
    {
        GraphValidationDiagnostic diagnostic =
            GraphValidationDiagnostic.Create("unknown-stage", "the node 'reader' references nothing");

        Assert.Equal("unknown-stage", diagnostic.Rule);
        Assert.Equal("the node 'reader' references nothing", diagnostic.Message);
        Assert.Null(diagnostic.Subject);
    }

    [Fact]
    public void CreateRoundTripsASubject()
    {
        GraphValidationDiagnostic diagnostic = GraphValidationDiagnostic.Create(
            "unconnected-input-port",
            "the input port 'writer#in' has no edge",
            "writer#in");

        Assert.Equal("writer#in", diagnostic.Subject);
    }

    [Fact]
    public void ToStringJoinsTheRuleAndTheMessage() =>
        Assert.Equal(
            "unknown-stage: the node 'reader' references nothing",
            GraphValidationDiagnostic
                .Create("unknown-stage", "the node 'reader' references nothing", "reader")
                .ToString());

    [Fact]
    public void CreateAcceptsARuleIdentifierThisVersionDoesNotDefine()
    {
        // The rule vocabulary is open: a later provider-supplied check reports an identifier of its own,
        // so the factory checks that a rule is present rather than that it is one of a fixed list.
        GraphValidationDiagnostic diagnostic =
            GraphValidationDiagnostic.Create("provider-specific-rule", "something a later check found");

        Assert.Equal("provider-specific-rule", diagnostic.Rule);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void CreateRejectsABlankRule(string blank)
    {
        Assert.Throws<ArgumentException>("rule", () => GraphValidationDiagnostic.Create(blank, "a message"));
        Assert.Throws<ArgumentException>(
            "rule",
            () => GraphValidationDiagnostic.Create(blank, "a message", "a subject"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void CreateRejectsABlankMessage(string blank)
    {
        Assert.Throws<ArgumentException>("message", () => GraphValidationDiagnostic.Create("a-rule", blank));
        Assert.Throws<ArgumentException>(
            "message",
            () => GraphValidationDiagnostic.Create("a-rule", blank, "a subject"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void CreateRejectsABlankSubject(string blank) =>
        Assert.Throws<ArgumentException>(
            "subject",
            () => GraphValidationDiagnostic.Create("a-rule", "a message", blank));

    [Fact]
    public void CreateRejectsNullArguments()
    {
        Assert.Throws<ArgumentNullException>("rule", () => GraphValidationDiagnostic.Create(null!, "a message"));
        Assert.Throws<ArgumentNullException>("message", () => GraphValidationDiagnostic.Create("a-rule", null!));
        Assert.Throws<ArgumentNullException>(
            "subject",
            () => GraphValidationDiagnostic.Create("a-rule", "a message", null!));
    }

    [Fact]
    public void EqualDiagnosticsAreEqualAndShareHashCode()
    {
        GraphValidationDiagnostic left =
            GraphValidationDiagnostic.Create("unknown-stage", "the node 'reader' references nothing", "reader");
        GraphValidationDiagnostic right =
            GraphValidationDiagnostic.Create("unknown-stage", "the node 'reader' references nothing", "reader");

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
    }

    [Fact]
    public void DiagnosticsDifferingInAnyMemberAreNotEqual()
    {
        GraphValidationDiagnostic diagnostic =
            GraphValidationDiagnostic.Create("unknown-stage", "a message", "reader");

        Assert.NotEqual(diagnostic, GraphValidationDiagnostic.Create("unknown-input-port", "a message", "reader"));
        Assert.NotEqual(diagnostic, GraphValidationDiagnostic.Create("unknown-stage", "another message", "reader"));
        Assert.NotEqual(diagnostic, GraphValidationDiagnostic.Create("unknown-stage", "a message", "writer"));
        Assert.NotEqual(diagnostic, GraphValidationDiagnostic.Create("unknown-stage", "a message"));
    }
}
