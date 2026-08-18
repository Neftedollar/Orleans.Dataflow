namespace Orleans.Dataflow.FSharp

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.Identity

// Orleans.Dataflow itself is deliberately not opened here either: see the note in Source.fs.

/// <summary>The conversions and checks this package makes more than once, written down once.</summary>
/// <remarks>
/// <para>
/// Everything else in this package is a wrapping so thin it is the operator: an F# function becomes the
/// <see cref="T:System.Func`2"/> the descriptor stores and nothing happens in between. The two conversions
/// are the exceptions, and both are exceptions for the same reason — the runtime's delegate adapter names a
/// shape F# has no direct spelling of, so a conversion has to exist somewhere. Writing them here rather than
/// at every use site is what keeps the answer one answer, which is why the one check two different closing
/// calls both make lives here too.
/// </para>
/// <para>
/// This module is internal. Neither conversion is a concept an author composes with, and exposing them
/// would invite exactly the extension-method-over-the-facade shape the binding rule refuses.
/// </para>
/// </remarks>
module internal Bindings =

    /// <summary>Checks the name a result is to be declared under.</summary>
    /// <param name="parameterName">The name of the closing call's own parameter the name arrived in.</param>
    /// <param name="slotName">The name the author supplied.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slotName"/> is not a valid single-segment identifier.
    /// </exception>
    /// <remarks>
    /// Two calls declare a result — the one that closes a graph and the one that ends a branch — and they
    /// are in two files, so the check is here rather than duplicated in both. It delegates to the very
    /// guard the C# facade calls, because <c>ResultSlotId</c> owns the segment grammar and the diagnostic
    /// for breaking it: this function once restated the message and the two frontends drifted into
    /// different sentences for one refusal, which is exactly what the delegation exists to prevent. The
    /// caller's parameter name is passed rather than inferred: inferring it would name this function's own
    /// parameter, and the author wrote the closing call's.
    /// </remarks>
    let slotId (parameterName: string) (slotName: string) : ResultSlotId =
        Orleans.Dataflow.Authoring.LocalOptionGuard.SlotName(slotName, parameterName)

    /// <summary>Starts an asynchronous computation as the task an asynchronous stage awaits.</summary>
    /// <param name="computation">The author's computation.</param>
    /// <param name="cancellationToken">The run's own token, as the stage hands it over.</param>
    /// <returns>The running task.</returns>
    /// <remarks>
    /// <para>
    /// The run's token starts the computation rather than being ignored beside it, so
    /// <c>Async.CancellationToken</c> inside the author's workflow is the run's token and a cancelled run
    /// actually reaches the work. That is the requirement F-SHARP-API.md states as "cancellation must reach
    /// the returned computation", and it is a requirement precisely because the obvious spelling —
    /// starting the computation and passing the token nowhere — compiles and silently fails it.
    /// </para>
    /// <para>
    /// The immediate start is chosen over the queued one because it is what the stage already promises: an
    /// asynchronous callback runs on the segment's own thread up to its first suspension, exactly as a C#
    /// <c>async</c> lambda does. Queueing would add a thread-pool hop the C# spelling of the same graph does
    /// not have, and the two frontends run one runtime.
    /// </para>
    /// </remarks>
    let asTask (computation: Async<'T>) (cancellationToken: CancellationToken) : Task<'T> =
        Async.StartImmediateAsTask(computation, cancellationToken)

    /// <summary>Builds the projection a batching stage turns one group of boxed elements into a list with.</summary>
    /// <returns>The projection, closed over the element type.</returns>
    /// <remarks>
    /// <para>
    /// A run accumulates elements as <see cref="T:System.Object"/>, because a local graph's element types
    /// live in the CLR type system and never in a document; the author declared a list of their own type.
    /// The array is copied out per group, so nothing a batch emits shares storage with the buffer the stage
    /// reuses.
    /// </para>
    /// <para>
    /// This is the one place where the C# package's own spelling had to be re-stated rather than called: the
    /// projection is a private static of <c>Source</c>, of <c>Flow</c>, and of <c>Sink</c> — three copies
    /// there and a fourth here. It is binding rather than payload, so no document can disagree because of
    /// it, and every batching operator's behavior test asserts the elements of the list it produces, which
    /// is the only way a drift here could ever show.
    /// </para>
    /// </remarks>
    let groupOf<'T> () : Func<objnull, objnull> =
        Func<objnull, objnull>(fun group ->
            let collected = unbox<List<objnull>> group
            let elements = Array.zeroCreate<'T> collected.Count

            for index in 0 .. elements.Length - 1 do
                elements[index] <- unbox<'T> collected[index]

            box elements)
