namespace Orleans.Dataflow.Testing;

/// <summary>
/// When a fault point throws: never, once, or from one arrival onwards.
/// </summary>
/// <remarks>
/// <para>
/// The arming vocabulary lives here rather than in the shipping package because that is what the seam is:
/// ADR 0007's failure injection is test-support surface, and a library nobody should be injecting faults with
/// should not publish the words for doing it. The stage itself is a shape of the local vocabulary — a
/// document has to be able to name what it is running — exactly as the probe sink is, and this is the only
/// spelling that reaches it.
/// </para>
/// <para>
/// <b>Every arming is deterministic and none of it is random.</b> A fault point counts the arrivals it has
/// been handed and throws at the ones its arming names, so a test states "the second element" and gets the
/// second element, on every run and on every machine.
/// </para>
/// </remarks>
public enum FaultPointMode
{
    /// <summary>The fault point passes every element through.</summary>
    /// <remarks>
    /// The default, and what a graph declares when the test intends to arm the point through its control
    /// once the run is going.
    /// </remarks>
    Never,

    /// <summary>Exactly the arrival at the declared position throws, and the ones after it pass.</summary>
    /// <remarks>
    /// Throw-once-then-heal, and — with a position past one — throw-on-the-Nth. It is the arming a retry
    /// test wants: the first attempt fails, the re-offer is the next arrival, and it passes.
    /// </remarks>
    Once,

    /// <summary>The arrival at the declared position throws, and so does every arrival after it.</summary>
    /// <remarks>
    /// Throw-always, which is what exhausts a retry ladder however long it is, and what a test uses to prove
    /// that a scope goes on containing failures rather than containing one.
    /// </remarks>
    Always,
}
