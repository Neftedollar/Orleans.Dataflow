namespace Orleans.Dataflow.Testing;

/// <summary>
/// The failure a fault point throws when a test has not said what it should throw.
/// </summary>
/// <remarks>
/// <para>
/// A type of its own rather than a bare <see cref="InvalidOperationException"/>, so that a test asserting on
/// a run's outcome can say "this is the failure I injected" and not "this is a failure". It carries the
/// one-based position of the arrival that threw, because that is the fact a test declared and the fact worth
/// reading in a message: a run that fails at the third element when the arming said the second is a run that
/// re-offered one.
/// </para>
/// <para>
/// It is not special to the engine in any way. It travels exactly as an author's own exception does — to the
/// run loop unwrapped, or to the supervision scope the fault point stands inside — and a test that wants a
/// different type gives the fault point a factory of its own.
/// </para>
/// </remarks>
public sealed class FaultInjectedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="FaultInjectedException"/> class.</summary>
    public FaultInjectedException()
        : base("A fault point threw the failure it was armed to throw.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FaultInjectedException"/> class.</summary>
    /// <param name="message">The message.</param>
    public FaultInjectedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FaultInjectedException"/> class.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The failure this one is reported over.</param>
    public FaultInjectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FaultInjectedException"/> class.</summary>
    /// <param name="arrival">The one-based position of the arrival that threw.</param>
    internal FaultInjectedException(long arrival)
        : base(string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"A fault point threw at arrival {arrival}, which is where its arming said it would."))
        => Arrival = arrival;

    /// <summary>Gets the one-based position of the arrival that threw.</summary>
    /// <value>Zero for an instance built through one of the framework constructors.</value>
    public long Arrival { get; }
}
