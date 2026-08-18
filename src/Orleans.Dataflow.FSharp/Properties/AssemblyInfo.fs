namespace Orleans.Dataflow.FSharp

open System.Runtime.CompilerServices

// The F# test project constructs this package's shapes directly in exactly one place: a tests-only bridge
// that reads a Testing-package value's occurrence chain (through its own friendship with the core package)
// and wraps it in the F# Sink or Flow it corresponds to. The constructors stay internal because a public
// from-chain constructor would be an invitation to bypass the modules; the tests are the one consumer with
// a reason, and they ship from this repository in lockstep.
[<assembly: InternalsVisibleTo("Orleans.Dataflow.FSharpTests")>]
do ()
