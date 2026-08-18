using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Serialization;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// Where one run's stream subscription has reached, and the token it reopens at.
/// </summary>
/// <param name="serializer">The silo's serializer, which is what makes a token a value a store can hold.</param>
/// <remarks>
/// <para>
/// <b>This is the cursor ADR 0007's model was designed for.</b> The local index cursor is a proof vehicle
/// over a sequence an author can re-enumerate; a stream sequence token is a real position in a real log,
/// issued by the provider, and rewinding to one is a thing the platform does rather than a thing this
/// package simulates. What the model asked of an adapter is exactly what a token supplies: a value that says
/// where a source was, that the source can be reopened at, and that survives being written down.
/// </para>
/// <para>
/// <b>The token is promoted from delivered rather than from received, and the difference is a real
/// number.</b> A subscription hands elements to a bounded ingress and the run pulls from that ingress, so at
/// any moment the ingress may hold several elements the provider has delivered and the run has not. Writing
/// the newest token the subscription had seen would therefore store a position past the elements still
/// sitting in the queue, and a resume would skip every one of them — a loss window created by the cursor
/// itself. So the token travels with its element through the queue, becomes <em>pending</em> when the run
/// takes the element, and becomes the <em>position</em> only when the run reports that element delivered.
/// </para>
/// <para>
/// <b>The position is a canonical value with a readable half and an opaque half, and both are deliberate.</b>
/// A token's sequence number and event index are on <see cref="StreamSequenceToken"/> itself, so they are
/// provider-independent and go in as numbers a person and a test can read. Reopening needs the token's own
/// concrete type back — a memory provider's token is not an Event Hubs one — so the token also goes in as
/// the silo serializer's bytes in base64. That one member is the single place in a checkpoint document where
/// a value is not readable outside the deployment that wrote it, and the cost is stated rather than hidden:
/// the numbers make the position auditable, and the blob makes the resume exact. A checkpoint carrying one
/// is portable to a process holding the same stream provider, which is what another silo of the same
/// deployment is.
/// </para>
/// <para>
/// <b>Where the provider is not rewindable, the cursor degrades to resume-from-now and says so.</b> Nothing
/// here decides that: a provider that refuses a token refuses the subscription, and which providers do is a
/// row in the adapter table (<c>IsRewindable</c>, probed rather than assumed — the memory provider answers
/// true). A stream a run never received an element from stores no position at all, and a resume of one
/// subscribes with no token, which is the same thing every non-cursored source does.
/// </para>
/// <para>
/// <b>Threading.</b> The pending token is written by whichever thread the provider delivers on and read by
/// the run's source thread; the position is written there and read by the capture loop while the run is
/// held quiescent. Both are single reference writes published through <see cref="Volatile"/>, so a reading
/// is a fact rather than a race the quiescence happens to have closed.
/// </para>
/// </remarks>
internal sealed class StreamSourceCursor(Serializer serializer) : DataflowSourceCursor
{
    /// <summary>The member of a stored position holding the token's sequence number.</summary>
    internal const string SequenceMember = "sequence";

    /// <summary>The member of a stored position holding the token's index within that sequence number.</summary>
    internal const string IndexMember = "index";

    /// <summary>The member of a stored position holding the token itself.</summary>
    internal const string TokenMember = "token";

    private StreamSequenceToken? _pending;
    private StreamSequenceToken? _position;
    private StreamSequenceToken? _restored;

    /// <inheritdoc/>
    /// <remarks>
    /// A run that has delivered nothing has no position and contributes nothing to the checkpoint, which is
    /// what an empty value means to the document: the table simply has no entry for this node, and a resume
    /// therefore opens the subscription as a fresh run would.
    /// </remarks>
    public override CanonicalJsonValue Position
    {
        get
        {
            if (Volatile.Read(ref _position) is not { } reached)
            {
                return default;
            }

            // Interpolated straight into the text rather than escaped, because base64 is drawn from an
            // alphabet with nothing in it a JSON string escapes; the members are written in ordinal order so
            // that the text this builds is already canonical and the parse below is a validation.
            string token = Convert.ToBase64String(serializer.SerializeToArray(reached));

            return CanonicalJsonValue.Parse(string.Create(
                CultureInfo.InvariantCulture,
                $"{{\"{IndexMember}\":{reached.EventIndex},\"{SequenceMember}\":{reached.SequenceNumber},\"{TokenMember}\":\"{token}\"}}"));
        }
    }

    /// <summary>Gets the token a resumed subscription opens at.</summary>
    /// <value>
    /// The token this cursor was restored to, or <see langword="null"/> for a run that is not a resume or
    /// whose previous attempt had delivered nothing.
    /// </value>
    /// <remarks>
    /// Read by the opener the factory closed over this cursor, once, when the run first pulls. It stays the
    /// restored value for the life of the run rather than following <see cref="Position"/>, because a
    /// subscription is opened once and reopening it is a resume rather than a step.
    /// </remarks>
    internal StreamSequenceToken? Restored => _restored;

    /// <summary>Records the token of an element the run has just taken from the ingress.</summary>
    /// <param name="token">The token the provider delivered with it, which may be absent.</param>
    /// <remarks>
    /// Called from the sequence, one element before <see cref="Delivered"/> confirms it. Exactly one element
    /// is outstanding between the two, because the run pulls one, pushes it through its segment, and only
    /// then reports the delivery.
    /// </remarks>
    internal void Took(StreamSequenceToken? token) => Volatile.Write(ref _pending, token);

    /// <inheritdoc/>
    public override void Delivered()
    {
        if (Volatile.Read(ref _pending) is { } taken)
        {
            Volatile.Write(ref _position, taken);
        }
    }

    /// <inheritdoc/>
    public override void RestoreTo(CanonicalJsonValue position)
    {
        if (position.IsDefault ||
            position.ToElement().ValueKind is not JsonValueKind.Object ||
            !position.ToElement().TryGetProperty(TokenMember, out JsonElement token) ||
            token.ValueKind is not JsonValueKind.String ||
            token.GetString() is not { } encoded)
        {
            throw Unreadable(position);
        }

        try
        {
            _restored = serializer.Deserialize<StreamSequenceToken>(Convert.FromBase64String(encoded));
        }
        catch (Exception malformed) when (malformed is FormatException or InvalidOperationException or ArgumentException or IndexOutOfRangeException)
        {
            throw Unreadable(position);
        }

        _position = _restored;
    }

    /// <summary>Builds the failure a position this cursor cannot read produces.</summary>
    /// <param name="position">The position as the checkpoint carried it.</param>
    /// <returns>The exception.</returns>
    private static InvalidOperationException Unreadable(CanonicalJsonValue position) =>
        new($"The checkpoint carries the position {position} for an Orleans stream source, and such a source's position is an object whose '{TokenMember}' member holds a stream sequence token this silo's serializer wrote. The checkpoint was written by a different graph, by a deployment whose stream provider is not this one, or by hand.");
}
