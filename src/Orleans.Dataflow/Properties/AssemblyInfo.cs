using System.Runtime.CompilerServices;

// The authoring-side binding table a RunnableGraph carries is internal on purpose: delegates are not
// durable topology and must not appear on the public surface. The table is also the one piece of state
// that no public API can observe, so the test assembly is granted access to assert that a closed graph
// binds exactly the delegates the author supplied, keyed by the node identifiers the document declares.
[assembly: InternalsVisibleTo("Orleans.Dataflow.Tests")]

// The Orleans hosting package is the runtime-factory seam's one consumer in this repository, and the seam
// is internal on purpose: what a provider hands the engine is the engine's own executor vocabulary, and
// publishing that vocabulary would fix the M4 provider SDK's shape by accident. The two packages ship in
// lockstep from one repository, so friend access is the honest spelling of a boundary they already share.
[assembly: InternalsVisibleTo("Orleans.Dataflow.Orleans")]

// The cluster tests write runtime factories directly against that seam, exactly as the unit tests assert
// against the binding table: both are statements about internals that no public API can observe, and both
// would otherwise force a public surface into existence to let a test exist.
[assembly: InternalsVisibleTo("Orleans.Dataflow.OrleansTests")]

// The testing package is a second authoring frontend over part of this one's vocabulary rather than a
// consumer of its public API: a probe declares a local stage, binds a per-run object to it, and reads the
// demand accounting of the queue behind it, none of which is public and none of which should become public
// to let a test helper exist. What stays here is everything that is runtime semantics — the rendezvous, the
// stop discipline, the pause accounting — so that there is one implementation of those and not two.
[assembly: InternalsVisibleTo("Orleans.Dataflow.Testing")]

// The F# frontend is an equal authoring frontend over this package's algebra — the shape, the descriptor
// vocabulary, the graph builder, and the slot factory — and never over the C# fluent facade, whose
// spellings are one language's and would import every C#-ism into a package that exists to not have them
// (F-SHARP-API.md, binding rule). The seam is friend access rather than a public surface because what the
// F# modules consume is the very state the fluent methods consume: publishing it would fix the algebra's
// shape as API by accident, and the two frontends ship from one repository in lockstep. Drift between the
// frontends is impossible by construction, not by discipline: both call the one descriptor vocabulary and
// the one builder, and the runtime's delegate adapter is the single owner of how a typed lambda meets a
// boxed element.
[assembly: InternalsVisibleTo("Orleans.Dataflow.FSharp")]
