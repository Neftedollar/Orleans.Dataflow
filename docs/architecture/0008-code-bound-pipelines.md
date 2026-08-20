# 8. Code-bound pipelines

Status: proposed.

## The problem

A pipeline that runs anywhere but the process that wrote it must name its
steps. Naming them costs a provider identity, a stage identity, a stage
reference, an element contract, a result contract, a parameter contract, port
specifications, a catalog, and a factory — measured at twenty-nine distinct
public type names against a median of seven for every other task in the library.
The tutorial page where this arrives introduces ninety new names against nine on
the page before it, and it is where readers stop.

Every one of those artifacts exists for a reason, and none of them is what an
author came to write. What they came to write is:

```csharp
Source.From(orderEvents).Where(order => order.IsValid).Select(OrderDocument.FromEvent)
```

and they want it to run on a silo.

## Why it cannot today

A document holds stages, connections and numbers. It holds no delegate, no CLR
type name, no assembly reference. A graph built from lambdas says so about
itself — it declares `nondeployable`, and `AsPipeline` refuses it by name.

The stated reason has been that a document naming CLR members would cause code
loading in whatever process reads it. That is true and it is not the primary
reason, which is worth correcting because the imprecision closes off a design
that is otherwise sound.

**The primary requirement is that a document's identity covers its behaviour.**
Two things rest on it. A silo that cannot run a document must refuse it by name
rather than run something else; and a checkpoint must refuse a document it was
not taken of, because a stored position names nodes of one graph and means
nothing in another. Both are comparisons, and a comparison is only as good as
what the compared thing contains.

A CLR member reference breaks that: the name is stable while the behaviour
behind it changes. Redeploy with an edited method body and every fingerprint
still matches, every silo still accepts, and a resumed run restores its
accumulator into behaviour that did not exist when the accumulator was written.
The failure is silent, which is the only kind this library refuses to ship.

**What the current design actually has is weaker than that, and this should be
said plainly.** A graph's fingerprint does not cover lambda bodies at all —
changing a predicate's constant leaves it byte-identical, which is demonstrated
in the samples on purpose. What a *pipeline* has is that behaviour is reachable
only through a named reference carrying a major version, and that version is the
author's declaration that behaviour changed. The safety is contractual, not
mechanical. A stage whose author forgets to raise the version has exactly the
silent failure described above.

## The precedent

Apache Flink ships a serialised job graph to a cluster and ships user code
beside it, as jars the nodes must have. It is the same trade this proposal
makes. Flink also requires operators to carry stable, author-assigned
identifiers, and warns that without them a savepoint cannot be restored across a
change of topology — arrived at independently, for the reason described above.

Akka Streams, by contrast, does not have this problem, because it does not carry
a portable topology at all: a graph is objects and closures in one process,
distribution is a reference to a materialised endpoint rather than a shipped
graph, and durability is the source's offset rather than the graph's state. That
is a coherent design and it covers most pipelines. It does not cover a run whose
intermediate state — a scan's accumulator, a window, a group-by's live keys, a
sink's commit mark — must be rebuilt somewhere else, which is what this library
exists for.

## The proposal

Add a third capability token beside the two that exist.

| Token | Says |
|---|---|
| `nondeployable` | a stage's behaviour lives in the process that built me |
| `ephemeral-identity` | my node identifiers are positions, so nothing can anchor to them |
| **`code-bound`** | **I name CLR members; a host that has the assembly can run me, and my identity does not cover my behaviour** |

A `code-bound` document carries, per stage, the assembly-qualified member the
host is to bind — the same information an expression tree holds, in canonical
form. `AsPipeline` accepts it where it refuses `nondeployable`, so the author
writes ordinary lambdas and gets a pipeline.

Three refusals keep it honest.

**A durable run refuses a `code-bound` document, by name.** This is the whole
price and it is not negotiable: a checkpoint's guarantee is that it is restored
into the graph it was taken of, and a document whose identity does not cover its
behaviour cannot make that promise. The refusal names the token and says what to
do instead — register the stages, or drop durability.

**A silo runs `code-bound` documents only when the deployment says so.** Not a
document-level flag alone: a document that causes code loading is an execution
primitive, and a coordinator accepts documents from any client that can reach
the cluster. The opt-in belongs beside the catalog registration, so a deployment
that never enables it cannot be handed one.

**A rolling upgrade refuses rather than diverges.** During a roll, silos hold
different builds of the same assembly, and a member reference resolves to
whichever one is local. A `code-bound` document therefore carries the assembly
version it was authored against, and a silo whose assembly does not match
refuses by name rather than running its own copy.

## What this does not do

It does not make lambdas portable. The behaviour still lives in an assembly and
still has to be deployed, which is the trade being made rather than a limitation
being papered over.

It does not remove the publishing story. A provider shipping stages for other
people to use still declares a vocabulary, because a name that other authors
write is the point. What it removes is the obligation on an author whose stages
are only ever used by their own pipeline.

It does not extend to sources and sinks that talk to the outside world. A grain
call, a stream, a database cursor — those are what a registered stage is for,
and their configuration belongs in the document as data rather than as a bound
method.

## The alternative it does not preclude

Embedding a restricted expression language in the document — a whitelist of
comparisons, arithmetic, and field access, refused at build time when an author
writes anything outside it — gives deployability *and* keeps identity covering
behaviour, because the expression is in the document and changing it changes the
fingerprint. It is strictly stronger than this proposal and considerably more
work: element contracts would need a schema for field access to resolve against,
the runtime would need an interpreter, and the language itself would need a
compatibility story.

The two are complementary. Expressions cover pure computation with full safety;
`code-bound` covers everything else with the safety stated on the document.
