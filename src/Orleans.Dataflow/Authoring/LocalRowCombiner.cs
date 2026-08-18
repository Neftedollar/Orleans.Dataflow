namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place a typed row-building function becomes the boxed one a joining junction is bound to.
/// </summary>
/// <remarks>
/// <para>
/// A combiner takes one element per wired input, so unlike an unzip's projections it cannot be recovered
/// from its own delegate type: that would need one template per arity, and the widest fan-in would have
/// none at all. The authoring surface is where the element types are known, so this is where the conversion
/// happens — exactly as a collecting sink's projection and an ingress queue's facade are built where their
/// element types are known.
/// </para>
/// <para>
/// The array the run loop hands over is as long as the junction has wired inputs, and the positions are the
/// junction's port order. Reading a fixed number of them is therefore safe for a combiner built for a
/// junction of that arity, which is the only way one of these is ever built.
/// </para>
/// </remarks>
internal static class LocalRowCombiner
{
    /// <summary>Boxes a two-input row-building function.</summary>
    /// <typeparam name="T1">The element type of the first input.</typeparam>
    /// <typeparam name="T2">The element type of the second input.</typeparam>
    /// <typeparam name="TOut">The element type of the row.</typeparam>
    /// <param name="combine">The author's function, already known to be non-null.</param>
    /// <returns>The combiner in the boxed vocabulary the run loop speaks.</returns>
    internal static Func<object?[], object?> Of<T1, T2, TOut>(Func<T1, T2, TOut> combine) =>
        parts => combine((T1)parts[0]!, (T2)parts[1]!);
}
