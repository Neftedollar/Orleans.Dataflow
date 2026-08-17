using System.Runtime.CompilerServices;

// The authoring-side binding table a RunnableGraph carries is internal on purpose: delegates are not
// durable topology and must not appear on the public surface. The table is also the one piece of state
// that no public API can observe, so the test assembly is granted access to assert that a closed graph
// binds exactly the delegates the author supplied, keyed by the node identifiers the document declares.
[assembly: InternalsVisibleTo("Orleans.Dataflow.Tests")]

// The testing package is a second authoring frontend over part of this one's vocabulary rather than a
// consumer of its public API: a probe declares a local stage, binds a per-run object to it, and reads the
// demand accounting of the queue behind it, none of which is public and none of which should become public
// to let a test helper exist. What stays here is everything that is runtime semantics — the rendezvous, the
// stop discipline, the pause accounting — so that there is one implementation of those and not two.
[assembly: InternalsVisibleTo("Orleans.Dataflow.Testing")]
