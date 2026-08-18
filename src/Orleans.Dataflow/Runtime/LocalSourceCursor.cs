using System.Collections;
using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A source that knows where it is, and can be opened there again.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0007's cursor, as a runtime seam rather than a promise about sources in general. A source
/// <em>declares</em> a cursor by being planned through one of these; every other source contributes nothing
/// to a checkpoint and <b>resumes from now</b>, which is stated per source in the adapter table rather than
/// generalized. One is built per materialization, and the seam has three moving parts and no more: open at
/// where the checkpoint said, advance when an element has been delivered, and answer where you are.
/// </para>
/// <para>
/// <b>The position is a canonical value and that is the seam's requirement.</b> An index, a sequence token,
/// an offset — whatever an adapter's position is, it enters the checkpoint as canonical JSON with no CLR
/// type name anywhere in it, because a checkpoint is read by a process that does not have the adapter's
/// types loaded. An adapter that cannot say where it is in that plane declares no cursor at all.
/// </para>
/// <para>
/// <b>The position is read at a safe point and means "handed over", not "committed".</b> The capture holds
/// the run quiescent before it asks, so the number is not a moving target; what it counts is elements this
/// source handed the run and that travelled all the way through the segment they entered. Whether they
/// reached a sink is the commit mark's question and not this one, which is exactly why a checkpoint carries
/// both.
/// </para>
/// <para>
/// <b>Advancing is the run's call and not the sequence's.</b> A sequence only ever learns that its element
/// was wanted when it is asked for the next one, and the moment between those two — the element delivered,
/// the next not yet asked for — is exactly where a pause lands. A cursor that counted pulls would therefore
/// be one behind at every capture; the run instead advances the cursor when an element has travelled all
/// the way through the segment it entered, which is a fact only the run knows. That makes a capture's
/// position exact rather than approximately right in the safe direction.
/// </para>
/// </remarks>
internal abstract class LocalSourceCursor
{
    /// <summary>Gets where this source has reached.</summary>
    /// <value>The position, as a canonical value only this adapter's opener has to understand.</value>
    internal abstract CanonicalJsonValue Position { get; }

    /// <summary>Records that the element just pulled has been delivered through its segment.</summary>
    internal abstract void Delivered();

    /// <summary>Takes back a position this cursor reported earlier.</summary>
    /// <param name="position">The position, as a checkpoint carried it.</param>
    /// <exception cref="InvalidOperationException">The value is not a position this cursor understands.</exception>
    /// <remarks>
    /// Called before the run starts, on the thread that materializes it. A cursor restored mid-run would be
    /// a source told to be somewhere else while it was reading.
    /// </remarks>
    internal abstract void RestoreTo(CanonicalJsonValue position);
}

/// <summary>
/// The cursor of a source a provider built, seen by the engine.
/// </summary>
/// <param name="declared">The cursor the provider's stage runtime carries.</param>
/// <remarks>
/// <para>
/// A pass-through and deliberately nothing more: what a position means, where a restored one is read, and
/// how a reopened sequence uses it are the adapter's, and this type exists only because the engine's cursor
/// seam is internal and the provider's is public. The three members line up one for one, so a provider's
/// cursor is the engine's cursor with a different address.
/// </para>
/// <para>
/// <b>It does not open anything</b>, and that is the difference from <see cref="LocalIndexCursor"/>. A
/// registered source is opened by the opener its factory handed over, which closed over this very cursor
/// instance; a local one has no opener of its own, so its cursor is also its sequence. Both are cursors and
/// only one of them is a source.
/// </para>
/// </remarks>
internal sealed class LocalProvidedCursor(Hosting.DataflowSourceCursor declared) : LocalSourceCursor
{
    /// <inheritdoc/>
    internal override CanonicalJsonValue Position => declared.Position;

    /// <inheritdoc/>
    internal override void Delivered() => declared.Delivered();

    /// <inheritdoc/>
    internal override void RestoreTo(CanonicalJsonValue position) => declared.RestoreTo(position);
}

/// <summary>
/// The cursor of a source that counts: how many of its elements have been delivered, and re-reading from
/// there.
/// </summary>
/// <param name="elements">The author's sequence, which is re-enumerated to reopen at a position.</param>
/// <remarks>
/// <para>
/// The proof vehicle for ADR 0007's cursor model, and the simplest position a source can have: an index.
/// <c>from-enumerable</c> declares one, so a graph over an in-memory sequence resumes where it left off
/// rather than from the top.
/// </para>
/// <para>
/// <b>What this cursor requires of the author is stated rather than assumed.</b> Reopening at a position
/// re-enumerates the very sequence the author handed over and skips that many elements, so a sequence that
/// enumerates differently the second time resumes into different elements, and a sequence shorter than the
/// stored position fails the resume by name. That is the adapter's declared requirement — a re-enumerable,
/// stable sequence — and not a promise the engine makes on its behalf. A source over an iterator that reads
/// a file, a socket, or a random generator has no business declaring this cursor, and one that reads a list
/// has every business declaring it.
/// </para>
/// <para>
/// The skipped elements are pulled and discarded rather than jumped over, because an
/// <see cref="IEnumerable"/> has no seek. That is linear in the stored position and is the honest cost of
/// this particular adapter's cursor; a source with a real seek declares a cursor that uses it.
/// </para>
/// </remarks>
internal sealed class LocalIndexCursor(IEnumerable elements) : LocalSourceCursor
{
    /// <summary>The member of this cursor's position holding how many elements have been delivered.</summary>
    internal const string IndexMember = "index";

    private long _delivered;
    private long _from;

    /// <summary>Opens the author's sequence at the position this cursor was restored to.</summary>
    /// <param name="context">The tokens of the run being opened.</param>
    /// <returns>The sequence, which the run enumerates exactly once.</returns>
    /// <remarks>
    /// On this cursor and not on the seam, because a local source has no opener of its own: its sequence
    /// <em>is</em> what the cursor knows how to reopen. A registered source is opened by the opener its
    /// factory handed over, which is why <see cref="LocalProvidedCursor"/> has no such member.
    /// </remarks>
    internal IEnumerable Open(LocalRunContext context) => Enumerate(_from);

    /// <inheritdoc/>
    /// <remarks>
    /// Read from the capture loop's thread while the run is quiescent, and written from the segment's own.
    /// The interlocked read costs nothing worth measuring and makes the reading a fact rather than a race
    /// the quiescence happens to have closed.
    /// </remarks>
    internal override CanonicalJsonValue Position =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{IndexMember}\":{Interlocked.Read(ref _delivered)}}}"));

    /// <inheritdoc/>
    internal override void Delivered() => Interlocked.Increment(ref _delivered);

    /// <inheritdoc/>
    internal override void RestoreTo(CanonicalJsonValue position)
    {
        JsonElement declared = position.IsDefault
            ? throw Unreadable(position)
            : position.ToElement();

        if (declared.ValueKind is not JsonValueKind.Object ||
            !declared.TryGetProperty(IndexMember, out JsonElement index) ||
            index.ValueKind is not JsonValueKind.Number ||
            !index.TryGetInt64(out long from) ||
            from < 0)
        {
            throw Unreadable(position);
        }

        _from = from;
        _delivered = from;
    }

    /// <summary>Builds the failure a position this cursor cannot read produces.</summary>
    /// <param name="position">The position as the checkpoint carried it.</param>
    /// <returns>The exception.</returns>
    private static InvalidOperationException Unreadable(CanonicalJsonValue position) =>
        new($"The checkpoint carries the position {position} for a sequence source, and such a source's position is an object with an '{IndexMember}' member holding a count of zero or more delivered elements. The checkpoint was written by a different graph or by hand.");

    /// <summary>Enumerates the author's sequence from a stored position.</summary>
    /// <param name="from">How many elements to skip before the first one this run delivers.</param>
    /// <returns>The sequence.</returns>
    /// <exception cref="InvalidOperationException">
    /// The sequence produced no enumerator, or it holds fewer elements than the stored position.
    /// </exception>
    private IEnumerable Enumerate(long from)
    {
        IEnumerator inner = elements.GetEnumerator() ??
            throw new InvalidOperationException(
                "The source sequence produced no enumerator. A sequence a graph is bound to has to be enumerable more than in name.");

        try
        {
            for (long skipped = 0; skipped < from; skipped++)
            {
                if (!inner.MoveNext())
                {
                    throw new InvalidOperationException(string.Create(
                        CultureInfo.InvariantCulture,
                        $"A resume asked this sequence source to reopen at element {from} and the sequence holds only {skipped}. A sequence source's cursor re-enumerates the very sequence the author handed over, so resuming into one that has shrunk, been consumed, or enumerates differently the second time is refused rather than silently started from somewhere else."));
                }
            }

            while (inner.MoveNext())
            {
                yield return inner.Current;
            }
        }
        finally
        {
            (inner as IDisposable)?.Dispose();
        }
    }
}
