namespace Orleans.Dataflow.Grains;

/// <summary>
/// The names Orleans.Dataflow's grains use to address grain storage.
/// </summary>
/// <remarks>
/// Published rather than internal because a deployment has to configure a store under these names before
/// a silo can run anything, and a name that only appears inside an attribute is a name nobody can find.
/// Which store stands behind a name is the deployment's decision — memory in tests, a real store in
/// production — and nothing in this library makes it.
/// </remarks>
public static class OrleansDataflowStorage
{
    /// <summary>The grain storage provider the pipeline coordinator keeps its run register in.</summary>
    /// <remarks>
    /// The coordinator's state is also its fencing primitive, so the store behind this name has to
    /// implement optimistic concurrency honestly: a write from a superseded activation must be refused
    /// with a conflict rather than accepted. Every Orleans grain storage provider does; a store that did
    /// not would silently allow two owners.
    /// </remarks>
    public const string CoordinatorProviderName = "orleans-dataflow-coordinator";
}
