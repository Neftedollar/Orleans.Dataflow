using System.Runtime.CompilerServices;

// The authoring-side binding table a RunnableGraph carries is internal on purpose: delegates are not
// durable topology and must not appear on the public surface. The table is also the one piece of state
// that no public API can observe, so the test assembly is granted access to assert that a closed graph
// binds exactly the delegates the author supplied, keyed by the node identifiers the document declares.
[assembly: InternalsVisibleTo("Orleans.Dataflow.Tests")]
