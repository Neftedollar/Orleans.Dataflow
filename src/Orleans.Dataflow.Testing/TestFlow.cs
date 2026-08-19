using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The flows a test composes into a graph to make it fail where the test says it should.
/// </summary>
/// <remarks>
/// <para>
/// The failure-injection seam, and it follows the probes exactly: an injected fault is an
/// <b>ordinary stage a document names</b> rather than a hook reaching into the engine, so a graph under test
/// is a graph — it validates against the stage catalog, it fingerprints, and it composes anywhere a flow
/// stage stands. What it is not is a shipping stage: the arming vocabulary and every spelling that reaches
/// this stage live in this package.
/// </para>
/// <para>
/// <b>Everything about a fault point is deterministic and none of it is random.</b> It counts the arrivals
/// it has been handed and throws at the ones its arming names, so two runs of one graph fail at the same
/// elements. That is what makes it usable for proving a supervision policy, where "it failed sometimes" is
/// not evidence of anything.
/// </para>
/// <para>
/// <b>Two spellings, and the difference is a name.</b> The one taking a control name declares a per-run
/// <see cref="IFaultPoint"/> a test resolves and re-arms, and it stands at a node of its own. The one
/// without declares nothing to resolve and may therefore stand inside a supervision scope, whose stages are
/// not nodes and have nothing for a slot to name; its declared arming is the whole of what it does, which is
/// all a scope's own tests need.
/// </para>
/// <para>
/// The factories live on a non-generic companion class beside <see cref="Flow"/>, so that the type argument
/// is written only where it cannot be inferred.
/// </para>
/// </remarks>
public static class TestFlow
{
    /// <summary>Creates a flow that throws where its declared arming says to, exposing no control.</summary>
    /// <typeparam name="T">The element type, which the stage passes through untouched.</typeparam>
    /// <param name="mode">When the fault point throws.</param>
    /// <param name="firstFailure">
    /// The one-based arrival the mode first throws at, counting from the first element of the run.
    /// </param>
    /// <returns>The flow, ready to be composed anywhere an element stage stands.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration, or
    /// <paramref name="firstFailure"/> is below one.
    /// </exception>
    /// <remarks>
    /// The spelling for a fault point inside a supervision scope, and for any test that wants nothing from
    /// the point beyond making the graph fail where it said. It throws a
    /// <see cref="FaultInjectedException"/> carrying the arrival; the overload taking a factory is for a
    /// test that wants its own type.
    /// </remarks>
    public static Flow<T, T> FaultPoint<T>(FaultPointMode mode, int firstFailure) =>
        FaultPoint<T>(mode, firstFailure, static arrival => new FaultInjectedException(arrival));

    /// <summary>Creates a flow that throws a declared failure where its arming says to.</summary>
    /// <typeparam name="T">The element type, which the stage passes through untouched.</typeparam>
    /// <param name="mode">When the fault point throws.</param>
    /// <param name="firstFailure">The one-based arrival the mode first throws at.</param>
    /// <param name="fault">
    /// What to throw, over the one-based position of the arrival that is throwing; it is called once per
    /// throw, on the segment's own thread.
    /// </param>
    /// <returns>The flow.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="fault"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration, or
    /// <paramref name="firstFailure"/> is below one.
    /// </exception>
    /// <remarks>
    /// What a fault point throws is a value of a type no document names, so it is bound rather than
    /// declared — the split every stage of the local vocabulary makes between its numbers and its delegates.
    /// The exception travels unwrapped, exactly as an author's own would.
    /// </remarks>
    public static Flow<T, T> FaultPoint<T>(
        FaultPointMode mode,
        int firstFailure,
        Func<long, Exception> fault)
    {
        ArgumentNullException.ThrowIfNull(fault);

        return new Flow<T, T>(LocalStageChain.Of(LocalStageDescriptor.FaultPoint(
            Orleans.Dataflow.Testing.FaultPoint.Local(mode, nameof(mode)),
            LocalOptionGuard.FaultPosition(firstFailure, nameof(firstFailure)),
            Bind(fault, facade: null),
            controlSlot: null,
            controlType: null)));
    }

    /// <summary>Creates a flow that throws where its arming says to and exposes a control by name.</summary>
    /// <typeparam name="T">The element type, which the stage passes through untouched.</typeparam>
    /// <param name="controlName">The author-stable name to expose the fault point under.</param>
    /// <param name="mode">When the fault point throws.</param>
    /// <param name="firstFailure">The one-based arrival the mode first throws at.</param>
    /// <returns>The flow.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="controlName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="controlName"/> is not a valid result slot identifier.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration, or
    /// <paramref name="firstFailure"/> is below one.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The control is a per-run object reached by name, exactly as a probe is: closing the graph declares a
    /// result slot under <paramref name="controlName"/>, <see cref="RunnableGraph.Control{TControl}"/> turns
    /// that name back into a typed <see cref="ResultSlot{TResult}"/> of <see cref="IFaultPoint"/>, and
    /// <see cref="RunHandle.GetValueAsync{TResult}"/> resolves it against one run. Two runs of one graph
    /// have two fault points with two counters.
    /// </para>
    /// <para>
    /// The declared arming is what a test relies on for a graph that starts producing immediately; the
    /// control is for re-arming a run whose elements the test is pacing, and for reading how many arrivals
    /// the point saw and how many of them it threw at.
    /// </para>
    /// </remarks>
    public static Flow<T, T> FaultPoint<T>(
        string controlName,
        FaultPointMode mode,
        int firstFailure) =>
        FaultPoint<T>(controlName, mode, firstFailure, static arrival => new FaultInjectedException(arrival));

    /// <summary>Creates a flow that throws a declared failure and exposes a control by name.</summary>
    /// <typeparam name="T">The element type, which the stage passes through untouched.</typeparam>
    /// <param name="controlName">The author-stable name to expose the fault point under.</param>
    /// <param name="mode">When the fault point throws.</param>
    /// <param name="firstFailure">The one-based arrival the mode first throws at.</param>
    /// <param name="fault">What to throw, over the one-based position of the arrival that is throwing.</param>
    /// <returns>The flow.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="controlName"/> or <paramref name="fault"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="controlName"/> is not a valid result slot identifier.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="mode"/> is not a declared member of its enumeration, or
    /// <paramref name="firstFailure"/> is below one.
    /// </exception>
    public static Flow<T, T> FaultPoint<T>(
        string controlName,
        FaultPointMode mode,
        int firstFailure,
        Func<long, Exception> fault)
    {
        ArgumentNullException.ThrowIfNull(fault);

        ResultSlotId control = LocalOptionGuard.SlotName(controlName, nameof(controlName));
        Func<LocalFaultPoint, object> facade = static point => new FaultPoint(point);

        return new Flow<T, T>(LocalStageChain.Of(LocalStageDescriptor.FaultPoint(
            Orleans.Dataflow.Testing.FaultPoint.Local(mode, nameof(mode)),
            LocalOptionGuard.FaultPosition(firstFailure, nameof(firstFailure)),
            Bind(fault, facade),
            control,
            typeof(IFaultPoint))));
    }

    /// <summary>Pairs the two things a fault point binds, in the order the runtime reads them.</summary>
    /// <param name="fault">What to throw.</param>
    /// <param name="facade">The factory of the typed control, or <see langword="null"/> for no control.</param>
    /// <returns>The binding.</returns>
    private static object?[] Bind(Func<long, Exception> fault, Func<LocalFaultPoint, object>? facade) =>
        [fault, facade];
}
