namespace Orleans.Dataflow;

/// <summary>
/// What became of one element offered to a run's bounded ingress queue.
/// </summary>
/// <remarks>
/// <para>
/// Offering is the one place in this library where a refusal is a value rather than an exception. A
/// producer offering into a bounded queue meets a full queue, a completed queue, and an ended run in the
/// ordinary course of its work; none of the three is exceptional, and a producer that had to catch them
/// would be writing control flow in <c>catch</c> blocks. <see cref="IIngressQueue{T}.OfferAsync"/>
/// therefore never throws for the state of the queue — the only exception it can raise is the
/// <see cref="OperationCanceledException"/> of the caller's own token.
/// </para>
/// <para>
/// Every member is about <em>the element that was offered</em> and about nothing else. A policy that makes
/// room by discarding elements already in the queue accepts the offered one, so it answers
/// <see cref="Accepted"/>; how many elements a run has discarded is a property of the run rather than of
/// one offer.
/// </para>
/// </remarks>
public enum QueueOfferOutcome
{
    /// <summary>The element is in the queue and will be delivered unless the run ends first.</summary>
    /// <remarks>
    /// Acceptance is not processing and is not persistence: the element is queued, and the run may still
    /// fail, be cancelled, or end from downstream before it reaches a sink. This is the acceptance-versus-
    /// consumption distinction every bounded ingress adapter has to make explicit.
    /// </remarks>
    Accepted,

    /// <summary>The queue was full and its policy discarded this element rather than waiting.</summary>
    /// <remarks>
    /// Only <see cref="OverflowPolicy.DropNewest"/> answers this, because it is the only policy that
    /// discards the arriving element. <see cref="OverflowPolicy.DropOldest"/> and
    /// <see cref="OverflowPolicy.DropBuffer"/> discard elements that were already queued and accept this
    /// one.
    /// </remarks>
    Dropped,

    /// <summary>The queue no longer accepts elements because it was completed or its run has ended.</summary>
    /// <remarks>
    /// The two causes are deliberately one outcome: from the producer's side both mean "this queue will
    /// never take another element", and which of them happened is answered by
    /// <see cref="RunHandle.Completion"/> rather than by an offer.
    /// </remarks>
    Closed,

    /// <summary>The queue was failed, so it accepts nothing and the run is faulting.</summary>
    /// <remarks>
    /// Reached two ways, and both mean the same thing to a producer: the queue was failed explicitly with
    /// <see cref="IIngressQueue{T}.Fail"/>, or it was full under <see cref="OverflowPolicy.Fail"/>, which
    /// makes overflow a run failure by the author's own choice.
    /// </remarks>
    Failed,
}
