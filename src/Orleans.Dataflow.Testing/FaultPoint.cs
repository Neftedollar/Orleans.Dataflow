using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The facade over one run's fault point, and the one place the public arming vocabulary meets the local
/// vocabulary's own.
/// </summary>
/// <remarks>
/// The counting, the arming, and the throw belong to the runtime, because they happen on a segment's own
/// thread while elements are moving. What is here is the boundary this package exists to draw: a mode an
/// author writes is a mode the shipping package never publishes, so the mapping between the two lives with
/// the public spelling and refuses a value neither of them declares.
/// </remarks>
/// <param name="point">The run's own fault point.</param>
internal sealed class FaultPoint(LocalFaultPoint point) : IFaultPoint
{
    /// <inheritdoc/>
    public long ElementsSeen => point.ElementsSeen;

    /// <inheritdoc/>
    public long FaultsThrown => point.FaultsThrown;

    /// <inheritdoc/>
    public void Arm(FaultPointMode mode, long firstFailure = 1)
    {
        if (firstFailure < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(firstFailure),
                firstFailure,
                "A fault point is armed from the next arrival onwards, so the first failing arrival is 1 or more.");
        }

        point.Arm(Local(mode, nameof(mode)), firstFailure);
    }

    /// <inheritdoc/>
    public void Disarm() => point.Arm(LocalFaultMode.Never, firstFailure: 1);

    /// <summary>Maps one public mode onto the mode the local vocabulary writes into a document.</summary>
    /// <param name="mode">The public mode, which may be a value no member declares.</param>
    /// <param name="parameterName">The name of the parameter it arrived in.</param>
    /// <returns>The local mode.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// The mapping is exhaustive and refuses rather than defaulting, so a cast from an arbitrary integer is
    /// a diagnostic at the call site instead of a fault point that silently never throws.
    /// </remarks>
    internal static LocalFaultMode Local(FaultPointMode mode, string parameterName) => mode switch
    {
        FaultPointMode.Never => LocalFaultMode.Never,
        FaultPointMode.Once => LocalFaultMode.Once,
        FaultPointMode.Always => LocalFaultMode.Always,
        _ => throw new ArgumentOutOfRangeException(
            parameterName,
            mode,
            $"The value is not a declared {nameof(FaultPointMode)}, so there is no arming it names. The declared modes are {nameof(FaultPointMode.Never)}, {nameof(FaultPointMode.Once)}, and {nameof(FaultPointMode.Always)}."),
    };
}
