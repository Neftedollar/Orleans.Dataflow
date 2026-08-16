using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// A parameter validator that breaks its own contract in a chosen way.
/// </summary>
/// <remarks>
/// A validator is third-party code registered by deployment. This double stands in for one that was
/// written wrong, so that the compiler's answer to that case is a tested fact rather than whatever the
/// runtime happens to do first.
/// </remarks>
internal sealed class ContractBreakingValidator : IStageParameterValidator
{
    private readonly bool _returnsNull;
    private readonly string? _fragment;

    /// <summary>
    /// Initializes a new instance of the <see cref="ContractBreakingValidator"/> class.
    /// </summary>
    /// <param name="returnsNull">
    /// <see langword="true"/> to return no list at all; <see langword="false"/> to return a list holding
    /// <paramref name="fragment"/>.
    /// </param>
    /// <param name="fragment">The single fragment to return when a list is returned.</param>
    internal ContractBreakingValidator(bool returnsNull, string? fragment = null)
    {
        _returnsNull = returnsNull;
        _fragment = fragment;
    }

    /// <inheritdoc/>
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
    {
        if (_returnsNull)
        {
            return null!;
        }

        return [_fragment!];
    }
}
