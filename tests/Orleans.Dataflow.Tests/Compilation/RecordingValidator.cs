using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// A parameter validator that returns fixed fragments and records how it was called.
/// </summary>
/// <param name="fragments">The fragments this validator returns for every payload.</param>
/// <remarks>
/// Recording the calls is what lets a test prove that a gated rule was never evaluated at all rather than
/// evaluated and silent. Asserting an empty diagnostic list cannot tell those two apart.
/// </remarks>
internal sealed class RecordingValidator(params string[] fragments) : IStageParameterValidator
{
    /// <summary>Gets the number of times <see cref="Validate"/> was called.</summary>
    internal int CallCount { get; private set; }

    /// <summary>Gets the payload of the last call, or the default value when there was none.</summary>
    internal CanonicalJsonValue LastParameters { get; private set; }

    /// <inheritdoc/>
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
    {
        CallCount++;
        LastParameters = parameters;

        return fragments;
    }
}
