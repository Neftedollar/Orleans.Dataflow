namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One element-to-element stage that reads the run's clock, and the lifetime that gives it one.
/// </summary>
/// <remarks>
/// <para>
/// Every other stage of this runtime is a pure function of its element and can be built when the plan is
/// built. These cannot: a clock belongs to the run, a wait has to report itself to the run's pause gate,
/// and two of them act when no element arrives at all. So such a stage is built with its numbers when the
/// plan is built — its durations, its rate, its own valve — and handed the run's
/// <see cref="LocalStageAttachment"/> when the run starts, which is once per stage: a plan is compiled once
/// per materialization and a stage instance belongs to exactly one segment of exactly one run.
/// </para>
/// <para>
/// <see cref="Arm"/> is where a stage that has to act on silence starts its timer, and
/// <see cref="Detach"/> is where it stops. The first happens as the run is launched and the second on the
/// owning segment's own thread as it ends, so a timer never fires before the run has begun and never
/// outlives it.
/// </para>
/// </remarks>
internal abstract class LocalAttachedStage : LocalElementStage
{
    private LocalStageAttachment? _attachment;

    /// <summary>Gets the run this stage belongs to: its clock, its waits, and its stop hooks.</summary>
    /// <value>What <see cref="Attach"/> was given.</value>
    /// <exception cref="InvalidOperationException">This stage was applied before the run attached one.</exception>
    /// <remarks>
    /// The refusal is a defect report rather than a contract: a run attaches every timed stage of every
    /// segment before it launches any of them, so a stage without one is a plan that was executed by
    /// something other than this runtime's own loop.
    /// </remarks>
    private protected LocalStageAttachment Run =>
        _attachment ??
        throw new InvalidOperationException(
            "A stage that reads the run's clock was applied before its segment attached one. Every segment attaches the timed stages it holds before its first element.");

    /// <summary>Gives this stage the run it is part of, and starts whatever it does on silence.</summary>
    /// <param name="attachment">The run's clock, waits, and stop hooks.</param>
    internal void Attach(LocalStageAttachment attachment)
    {
        _attachment = attachment;

        Arm();
    }

    /// <summary>Releases whatever this stage started, because its segment has stopped.</summary>
    /// <remarks>
    /// Called on every terminal path of the segment, including the ones where the stage itself is what went
    /// wrong: a timer that outlived its run would fail or complete a run that had already ended, and a
    /// controlled clock would keep holding it.
    /// </remarks>
    internal virtual void Detach()
    {
    }

    /// <summary>Starts whatever this stage does when no element arrives.</summary>
    /// <remarks>Nothing, for the stages whose whole behavior is a function of the elements they receive.</remarks>
    private protected virtual void Arm()
    {
    }
}
