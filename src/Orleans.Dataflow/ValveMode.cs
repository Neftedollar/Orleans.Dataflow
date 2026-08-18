namespace Orleans.Dataflow;

/// <summary>
/// Whether a valve lets elements through.
/// </summary>
/// <remarks>
/// The state a valve starts a run in is written into the document, because it changes what the graph does
/// from its first element: a graph whose valve starts closed produces nothing until something opens it, and
/// a graph whose valve starts open is an ordinary chain until something closes it. What the valve is set to
/// afterwards is a run's own business and is never durable topology.
/// </remarks>
public enum ValveMode
{
    /// <summary>Elements pass through the valve.</summary>
    /// <remarks>The default: a valve nobody touches is a stage that does nothing at all.</remarks>
    Open,

    /// <summary>Elements wait at the valve until it is opened.</summary>
    /// <remarks>
    /// Waiting and never dropping. A closed valve holds the element the stage has in its hand and
    /// backpressures everything above it, which is the same thing a full buffer does and the reason a valve
    /// needs no capacity of its own.
    /// </remarks>
    Closed,
}
