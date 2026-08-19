using System.Collections;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A stage that owns a declared chain and can hand that chain's whole state to a checkpoint and take it
/// back.
/// </summary>
/// <param name="chain">The stages of the scope, in flow order, already built for this run.</param>
/// <remarks>
/// <para>
/// The other kind of scope, and it is a scope rather than a flag on the stages for the reason supervision
/// is one: what survives a resume is a decision about a <em>region</em> of a graph, it has to be visible in
/// the document for a cluster to honor it, and a region is a stage in this vocabulary. Everything outside
/// one of these resets on resume, and the reset is the contract rather than a caveat.
/// </para>
/// <para>
/// <b>Why it is not a form of the supervision scope.</b> The two answer different questions — what happens
/// when an element fails, and what happens when the process dies — and folding them together would make
/// every author who wants durable state also declare a failure policy, and every author who wants a retry
/// also decide about durability. Worse, the two would have to agree about the one place they overlap:
/// <c>RestartStage</c> resets every state in its scope and <c>durable-state</c> keeps every state across a
/// resume, so a scope that was both would be a scope whose contract is a sentence with a hole in it. Kept
/// apart, each says one thing exactly, and the composition an author actually wants — a durable scope
/// beside or inside a supervised section — stays a composition rather than a mode.
/// </para>
/// <para>
/// <b>The walk is the supervision scope's, without the retry loop and without the catch.</b> An element
/// travels through the chain, the emissions go into a list the run reads after this method has returned,
/// and a stage that ends the stream ends the scope's stream rather than the run's. A failure inside a
/// durable scope is not caught here at all: durability is not a failure policy, and inventing one would be
/// this stage answering a question it was never asked.
/// </para>
/// <para>
/// <b>Every stage of the chain exports state or the scope is refused by name.</b> The chain is admitted by
/// the document's own reader for the shapes whose state can be canonicalized at all, and the plan then asks
/// each built stage whether it actually exports — because a scan exports only when its author bound the
/// codec, which is a fact of the binding rather than of the document. Both refusals happen before the run
/// has an element.
/// </para>
/// </remarks>
internal sealed class LocalDurableScope(IReadOnlyList<LocalElementStage> chain)
    : LocalElementStage, ILocalDurableState
{
    /// <summary>The member of an exported state holding one entry per stage of the chain.</summary>
    internal const string StagesMember = "stages";

    private readonly List<object?> _emissions = [];
    private bool _open = true;

    /// <inheritdoc/>
    internal override LocalStageOutcome Apply(object? element, out object? result)
    {
        if (!_open)
        {
            result = null;

            return LocalStageOutcome.Complete;
        }

        _emissions.Clear();

        if (!Push(element, 0))
        {
            Drain();
        }

        return Answer(out result);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The end of the stream reaches the scope's stages exactly as it reaches a segment's, because they are
    /// a chain: every one of them is asked in flow order and each residue travels through the stages below
    /// the one that gave it. A scope whose chain has already ended its own stream refuses the walk, for the
    /// reason a spent <c>Take</c> refuses a residue offered to it.
    /// </remarks>
    internal override LocalStageOutcome Flush(out object? residue)
    {
        _emissions.Clear();

        if (_open)
        {
            Drain();
        }

        return Residue(out residue);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A durable scope exports whether or not it is inside another one, because what it is <em>for</em> is
    /// being exported. Nothing in this vocabulary nests one today, and the answer is stated all the same.
    /// </remarks>
    internal override bool ExportsState => true;

    /// <inheritdoc/>
    internal override CanonicalJsonValue ExportState() => Export();

    /// <inheritdoc/>
    internal override void RestoreState(CanonicalJsonValue state) => Restore(state);

    /// <inheritdoc/>
    /// <remarks>
    /// Positional, because the chain is positional: an inner chain has no identities, its order is the
    /// array's order, and a name per stage would be a second identity scheme for something the document
    /// already states by position. The length is what a restore checks, and a chain of a different length
    /// is a checkpoint of a different graph — which the fingerprint has already refused by the time this is
    /// reached, so the check here is the belt to that braces.
    /// </remarks>
    public CanonicalJsonValue Export()
    {
        StringBuilder text = new();

        _ = text.Append(CultureInfo.InvariantCulture, $"{{\"{StagesMember}\":[");

        for (int stage = 0; stage < chain.Count; stage++)
        {
            _ = text.Append(stage == 0 ? string.Empty : ",")
                .Append(CultureInfo.InvariantCulture, $"{chain[stage].ExportState()}");
        }

        return CanonicalJsonValue.Parse(text.Append("]}").ToString());
    }

    /// <inheritdoc/>
    public void Restore(CanonicalJsonValue state)
    {
        JsonElement declared = state.IsDefault ? throw Unreadable(state) : state.ToElement();

        if (declared.ValueKind is not JsonValueKind.Object ||
            !declared.TryGetProperty(StagesMember, out JsonElement stages) ||
            stages.ValueKind is not JsonValueKind.Array)
        {
            throw Unreadable(state);
        }

        if (stages.GetArrayLength() != chain.Count)
        {
            throw new InvalidOperationException(string.Create(
                CultureInfo.InvariantCulture,
                $"The checkpoint carries {stages.GetArrayLength()} stage states for a durable scope of {chain.Count} stages. A scope's exported state is positional, so a chain of a different length is a checkpoint of a different graph."));
        }

        int position = 0;

        foreach (JsonElement stage in stages.EnumerateArray())
        {
            chain[position].RestoreState(CanonicalJsonValue.FromElement(stage));
            position++;
        }
    }

    /// <summary>Builds the failure an exported state this scope cannot read produces.</summary>
    /// <param name="state">The state as the checkpoint carried it.</param>
    /// <returns>The exception.</returns>
    /// <remarks>
    /// <para>
    /// <b>The message says what the value is and never what it holds.</b> A durable scope's stored state is
    /// whatever the author's own export function wrote — up to the canonical limit of 256 KiB of their
    /// data — and this message does not stay where it is thrown: it is remembered on the run grain and
    /// handed back to every caller that polls it. Quoting the value would put the author's data on that
    /// trip, and the whole of what a reader needs here is the shape.
    /// </para>
    /// <para>
    /// The shape is enough for the diagnosis this failure has. Every way to reach it is a mismatch between
    /// what was stored and what this scope is — a checkpoint of a different graph, or one written by hand —
    /// and the naming of the scope's own arity beside the shape that arrived is what makes the two
    /// comparable.
    /// </para>
    /// </remarks>
    private InvalidOperationException Unreadable(CanonicalJsonValue state) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"The checkpoint carries a state a durable scope of {chain.Count} stages cannot read: {Describe(state)}. Such a scope's state is an object with a '{StagesMember}' member holding one entry per stage of the chain, in the chain's own order. The stored value is not quoted here, because an exported state is the author's own data and this message travels with the run. The checkpoint was written by a different graph or by hand."));

    /// <summary>Names the shape of a stored state without reading anything out of it.</summary>
    /// <param name="state">The state as the checkpoint carried it.</param>
    /// <returns>A phrase naming its kind, its size, and the first thing wrong with it.</returns>
    /// <remarks>
    /// The size is a fact about the value rather than a fact in it, and it is the one number that tells a
    /// checkpoint written by a different graph apart from one that was truncated or never written.
    /// </remarks>
    private static string Describe(CanonicalJsonValue state)
    {
        if (state.IsDefault)
        {
            return "it carries no JSON at all";
        }

        JsonElement declared = state.ToElement();

        if (declared.ValueKind is not JsonValueKind.Object)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"it is {Name(declared.ValueKind)} of {state.ByteLength} bytes rather than an object");
        }

        if (!declared.TryGetProperty(StagesMember, out JsonElement stages))
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"it is an object of {state.ByteLength} bytes with no '{StagesMember}' member");
        }

        return string.Create(
            CultureInfo.InvariantCulture,
            $"it is an object of {state.ByteLength} bytes whose '{StagesMember}' member is {Name(stages.ValueKind)} rather than an array");
    }

    /// <summary>Names one JSON kind as a noun phrase.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The phrase, which reads after the word <c>is</c>.</returns>
    private static string Name(JsonValueKind kind) =>
        kind switch
        {
            JsonValueKind.Object => "an object",
            JsonValueKind.Array => "an array",
            JsonValueKind.String => "a string",
            JsonValueKind.Number => "a number",
            JsonValueKind.True or JsonValueKind.False => "a boolean",
            JsonValueKind.Null => "the null literal",
            _ => "no value at all",
        };

    /// <summary>Answers with whatever the chain emitted while this stage was being asked.</summary>
    /// <param name="result">The one element, the sequence of them, or an unspecified value.</param>
    /// <returns>The outcome the count implies, and whether the scope's stream survived it.</returns>
    private LocalStageOutcome Answer(out object? result)
    {
        switch (_emissions.Count)
        {
            case 0:
                result = null;

                return _open ? LocalStageOutcome.Drop : LocalStageOutcome.Complete;
            case 1:
                result = _emissions[0];

                return _open ? LocalStageOutcome.Emit : LocalStageOutcome.EmitAndComplete;
            default:
                result = ((IEnumerable)_emissions).GetEnumerator();

                return LocalStageOutcome.EmitMany;
        }
    }

    /// <summary>Answers with whatever the chain handed over as the stream ended.</summary>
    /// <param name="result">The one element, the sequence of them, or an unspecified value.</param>
    /// <returns>The outcome the count implies, which never ends a stream that has already ended.</returns>
    private LocalStageOutcome Residue(out object? result)
    {
        switch (_emissions.Count)
        {
            case 0:
                result = null;

                return LocalStageOutcome.Drop;
            case 1:
                result = _emissions[0];

                return LocalStageOutcome.Emit;
            default:
                result = ((IEnumerable)_emissions).GetEnumerator();

                return LocalStageOutcome.EmitMany;
        }
    }

    /// <summary>Pushes one element through the scope's stages from one of them onwards.</summary>
    /// <param name="element">The element entering the stage named by <paramref name="from"/>.</param>
    /// <param name="from">The first stage to apply.</param>
    /// <returns><see langword="true"/> when the scope's stream is still open.</returns>
    private bool Push(object? element, int from)
    {
        bool completing = false;

        for (int stage = from; stage < chain.Count; stage++)
        {
            LocalStageOutcome outcome = chain[stage].Apply(element, out element);

            if (outcome is LocalStageOutcome.EmitAndComplete)
            {
                completing = true;

                continue;
            }

            if (outcome is LocalStageOutcome.Emit)
            {
                continue;
            }

            // Defensive, and recorded as defensive: no shape a durable scope may hold answers with a
            // sequence today, because every admitted shape answers one element with at most one element.
            // Handling it here keeps that a statement about which stages are admitted rather than about
            // this walk.
            if (outcome is LocalStageOutcome.EmitMany)
            {
                return Expand((IEnumerator)element!, stage + 1) && !completing;
            }

            return outcome is not LocalStageOutcome.Complete && !completing;
        }

        _emissions.Add(element);

        return !completing;
    }

    /// <summary>Pushes every element of one stage's sequence through the stages below it.</summary>
    /// <param name="inner">The sequence, which this method owns and releases.</param>
    /// <param name="from">The first stage below the one that produced it.</param>
    /// <returns><see langword="true"/> when the scope's stream is still open.</returns>
    private bool Expand(IEnumerator inner, int from)
    {
        try
        {
            while (inner.MoveNext())
            {
                if (!Push(inner.Current, from))
                {
                    return false;
                }
            }

            return true;
        }
        finally
        {
            (inner as IDisposable)?.Dispose();
        }
    }

    /// <summary>Ends the scope's stream and emits whatever its stages were still holding.</summary>
    private void Drain()
    {
        _open = false;

        for (int stage = 0; stage < chain.Count; stage++)
        {
            LocalStageOutcome outcome = chain[stage].Flush(out object? residue);

            if (outcome is LocalStageOutcome.Emit && !Push(residue, stage + 1))
            {
                return;
            }

            if (outcome is LocalStageOutcome.EmitMany && !Expand((IEnumerator)residue!, stage + 1))
            {
                return;
            }
        }
    }
}
