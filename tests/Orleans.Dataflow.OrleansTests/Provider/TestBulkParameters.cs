using System.Globalization;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// How the test bulk sink states how large its result is.
/// </summary>
/// <remarks>
/// The size is in the document rather than in the factory, so a test can ask for a result on either side of
/// the silo's cap without registering a second stage — and so that two pipelines asking for two sizes are
/// two pipelines, which is what the cap is a property of the deployment rather than of the graph means in
/// practice.
/// </remarks>
internal static class TestBulkParameters
{
    /// <summary>The payload member holding the size of the result, in bytes.</summary>
    internal const string BytesMember = "bytes";

    /// <summary>Writes the payload of a bulk sink of one size.</summary>
    /// <param name="bytes">How many bytes the result carries.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(int bytes) =>
        CanonicalJsonValue.Parse(
            string.Create(CultureInfo.InvariantCulture, $"{{\"{BytesMember}\":{bytes}}}"));
}
