namespace Orleans.Dataflow.FSharp

open System.Threading
open System.Threading.Tasks

// Orleans.Dataflow itself is deliberately not opened: see the note in Source.fs.

/// <summary>Reads a run that is already running, in the direction a pipeline reads.</summary>
/// <remarks>
/// <para>
/// A run handle is public runtime surface and reads perfectly well from F# as it is: <c>run.Completion</c>,
/// <c>run.WatchTermination</c>, <c>run.Snapshot()</c>, <c>run.PauseAsync ct</c>, <c>run.ResumeAsync()</c>,
/// <c>run.ShutdownAsync()</c>, and <c>run.DisposeAsync()</c> are members with no receiver-threading to smooth
/// over and no <c>out</c> parameter, and a module function per member would be a second name in a completion
/// list for the identical call. So this module is deliberately one function, and it is the one worth
/// spelling as a pipeline: resolving a slot is what an author does <em>to</em> a run, in the middle of
/// composing something else, and it is the only member whose argument order is worth reversing.
/// </para>
/// <para>
/// The token is a required argument rather than an omitted optional one. Every other asynchronous surface in
/// this package takes the run's own token explicitly, and a wrapper whose one convenience was hiding a
/// <see cref="P:System.Threading.CancellationToken.None"/> would make the unbounded wait the shortest thing
/// to write.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Run =

    /// <summary>Resolves the value of one slot of a run.</summary>
    /// <param name="slot">The slot, as the closing call or the graph's control lookup handed it over.</param>
    /// <param name="cancellationToken">A token that cancels this wait; the run itself keeps going.</param>
    /// <param name="run">The run being read, which is unchanged.</param>
    /// <returns>
    /// A task that resolves with the value when it becomes available, faults with the exception the run
    /// failed with, or cancels when the run cancels or <paramref name="cancellationToken"/> fires.
    /// </returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slot"/> is the default value, was declared by a different graph, or names no result of
    /// this run's graph.
    /// </exception>
    /// <remarks>
    /// Callable before, during, and after the run, and asking twice gives the same answer twice. A result —
    /// a fold's state, a first or last element, a collected list — becomes available when the stream has
    /// ended, so its task carries the run's outcome; a control — an ingress queue, a valve — exists as soon as
    /// the run does and is already resolved when the handle is handed over.
    /// </remarks>
    let value
        (slot: Orleans.Dataflow.ResultSlot<'T>)
        (cancellationToken: CancellationToken)
        (run: Orleans.Dataflow.RunHandle)
        : Task<'T> =
        run.GetValueAsync(slot, cancellationToken)
