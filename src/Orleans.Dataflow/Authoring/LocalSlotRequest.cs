using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// One result a shape asks its closure to expose: the name, the occurrence that produces it, and the
/// branch binding waiting for the graph's identity when the name came from a branch.
/// </summary>
/// <param name="Id">The author-stable name to expose the result under.</param>
/// <param name="Stage">The position of the producing occurrence in authoring order.</param>
/// <param name="Binding">
/// The binding to fill once the graph exists, or <see langword="null"/> when the closing call hands the slot
/// back itself and has the graph in hand.
/// </param>
/// <remarks>
/// A chain asks for at most one, because a chain has one terminal. A junction graph asks for one per
/// result-bearing branch, which is what "named multiple results" means concretely: the names are the
/// author's, the producers are the branch terminals, and the document declares one slot for each.
/// </remarks>
internal readonly record struct LocalSlotRequest(ResultSlotId Id, int Stage, BranchSlotBinding? Binding);
