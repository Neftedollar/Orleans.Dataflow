using System.Runtime.CompilerServices;

// The grains, the silo registry, and the persisted coordinator state are internal because none of them is
// something a consumer addresses: a client talks to grain interfaces and a deployment talks to the
// registration surface. What a test has to assert about them is exactly what no public API exposes —
// that every persisted and every wire-crossing type actually round-trips through Orleans' serializer, and
// that the coordinator's register survives an activation. Granting the cluster test assembly access is the
// same trade the core package already makes for its own tests.
[assembly: InternalsVisibleTo("Orleans.Dataflow.ClusterTests")]
