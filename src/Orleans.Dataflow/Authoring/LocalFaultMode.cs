namespace Orleans.Dataflow.Authoring;

/// <summary>
/// When a fault point throws: never, once, or from one arrival onwards.
/// </summary>
/// <remarks>
/// <para>
/// Internal, and that is the decision rather than an accident. A fault point is the failure-injection seam
/// of ADR 0007 and is test-support surface: the stage lives in this vocabulary because the vocabulary is one
/// closed set, exactly as <c>sink-probe</c> does, and the only spelling an author can reach it through is in
/// the testing package. Publishing an arming vocabulary from the shipping package would put fault injection
/// into the surface of a library nobody should be injecting faults with.
/// </para>
/// <para>
/// The testing package's own <c>FaultPointMode</c> is the public spelling and maps onto this one value for
/// value. Two enumerations rather than one shared public type is the price of that boundary, and it is paid
/// in one mapping that the payload writer refuses to be wrong about: a value no member declares has no
/// spelling, so it cannot be written into a document at all.
/// </para>
/// </remarks>
internal enum LocalFaultMode
{
    /// <summary>The fault point passes every element through.</summary>
    /// <remarks>
    /// A fault point a test arms while the run is running starts here, which is why this is the first value
    /// and the default of the payload's own reading.
    /// </remarks>
    Never,

    /// <summary>Exactly the arrival at the declared position throws, and the ones after it pass.</summary>
    /// <remarks>The throw-once-then-heal arming, and — with a position past one — the throw-on-the-Nth one.</remarks>
    Once,

    /// <summary>The arrival at the declared position throws, and so does every arrival after it.</summary>
    Always,
}
