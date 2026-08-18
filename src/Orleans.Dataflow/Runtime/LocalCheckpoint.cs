using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One checkpoint read back out of a store: what graph it was taken of, and the three tables of values a
/// resume restores from.
/// </summary>
/// <param name="Graph">The fingerprint of the graph the snapshot was taken of.</param>
/// <param name="Revision">The revision of that graph.</param>
/// <param name="Cursors">One position per source that declared a cursor, keyed by node.</param>
/// <param name="States">One exported state per durable scope, keyed by node.</param>
/// <param name="Marks">One commit mark per sink that declared one, keyed by node.</param>
/// <remarks>
/// <para>
/// The read side of <see cref="LocalCheckpointDocument"/>, and it is a value rather than a reader over the
/// document because a resume touches every table twice — once to check that the checkpoint describes this
/// graph, once per seam to hand a value back — and re-parsing canonical bytes for each of those would be
/// paying twice for one answer.
/// </para>
/// <para>
/// <b>The fingerprint is the first thing a resume looks at and the reason this type carries it.</b> V1's
/// rule is same-revision resume only: a checkpoint of a different fingerprint describes a graph whose
/// nodes may not be these nodes, so restoring a cursor into it would be restoring a position into a source
/// that is not the one it counted. Cross-revision migration is a recorded deferral (ADR 0007), and until it
/// exists the refusal by name is the honest answer.
/// </para>
/// </remarks>
internal sealed record LocalCheckpoint(
    GraphFingerprint Graph,
    GraphRevision Revision,
    IReadOnlyDictionary<NodeId, CanonicalJsonValue> Cursors,
    IReadOnlyDictionary<NodeId, CanonicalJsonValue> States,
    IReadOnlyDictionary<NodeId, CanonicalJsonValue> Marks);
