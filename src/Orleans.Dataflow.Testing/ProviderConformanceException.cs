using System.Globalization;
using System.Text;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// Thrown by <see cref="ProviderConformance"/> when a provider breaks at least one of the rules a check
/// states.
/// </summary>
/// <remarks>
/// <para>
/// The message is a numbered list of every failure the check found rather than the first one, for the same
/// reason a stage specification and a graph validation report list every violation: a provider author who
/// learns the contract one rejection per run learns it very slowly.
/// </para>
/// <para>
/// It is an exception rather than a return value because a conformance check is run from a test, and a test
/// framework's own idea of failure is an exception. That is also the whole of the coupling: nothing here
/// names a test framework, so the kit runs under xunit, NUnit, MSTest, or a console program.
/// </para>
/// </remarks>
public sealed class ProviderConformanceException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="ProviderConformanceException"/> class.</summary>
    public ProviderConformanceException()
        : this("A provider failed a conformance check.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ProviderConformanceException"/> class.</summary>
    /// <param name="message">The message.</param>
    public ProviderConformanceException(string message)
        : base(message) => Failures = [];

    /// <summary>Initializes a new instance of the <see cref="ProviderConformanceException"/> class.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public ProviderConformanceException(string message, Exception innerException)
        : base(message, innerException) => Failures = [];

    /// <summary>Initializes a new instance of the <see cref="ProviderConformanceException"/> class.</summary>
    /// <param name="provider">The provider under test.</param>
    /// <param name="failures">One lower-case sentence fragment per failure, in the order they were found.</param>
    internal ProviderConformanceException(string provider, IReadOnlyList<string> failures)
        : base(Describe(provider, failures)) => Failures = failures;

    /// <summary>Gets the failures this check found.</summary>
    /// <value>
    /// One lower-case sentence fragment per failure, in the order the check found them; empty for an
    /// instance built through one of the ordinary constructors.
    /// </value>
    public IReadOnlyList<string> Failures { get; }

    /// <summary>Renders the failures as one numbered list.</summary>
    /// <param name="provider">The provider under test.</param>
    /// <param name="failures">The failures.</param>
    /// <returns>A message whose first line states the count and whose remaining lines are numbered.</returns>
    private static string Describe(string provider, IReadOnlyList<string> failures)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The provider '{provider}' breaks {failures.Count} ");
        message.Append(failures.Count == 1 ? "conformance rule:" : "conformance rules:");

        for (int index = 0; index < failures.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {failures[index]}.");
        }

        return message.ToString();
    }
}
