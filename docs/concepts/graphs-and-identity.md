# Graphs and identity

*What exactly is a pipeline, and what makes two of them "the same"?*

You will meet three answers to "what is a pipeline" while using this library —
the value you author, the document it compiles to, and the pipeline definition
you can deploy — and they are genuinely different things rather than three names
for one. This page is about all three, and about the identity each of them
carries.

## A graph is a document

The value you build with `Source`, `Flow` and `Sink` closes into a
[RunnableGraph](../reference/glossary.md#runnablegraph), and inside every
`RunnableGraph` is a [graph document](../reference/glossary.md#graph-document).
The document is the pipeline. Everything else is either a way of producing it or
a way of executing it.

A document holds:

- **[nodes](../reference/glossary.md#node)** — one per
  [occurrence](../reference/glossary.md#occurrence) of a stage, each with an
  identifier, the stage it refers to, and its parameters;
- **edges** — from one node's output port to another node's input port;
- **[result slots](../reference/glossary.md#result-slot)** — named, typed places
  where values come out;
- **capabilities** — a small sorted set of tokens the document declares *about
  itself*;
- **an identity and a revision**, and a format version.

And that is all. Notably absent, and each absence is a decision worth
understanding.

### Why a document holds no delegates

A delegate is a pointer into a specific assembly, loaded into a specific process,
usually carrying captured state from a specific scope. There is no honest way to
write one into a portable document. You could try to serialize it — and then
every version bump of your assembly, every trimmed or ahead-of-time-compiled
deployment, every attempt to read the graph from a different language, and every
piece of untrusted graph data becomes a way to execute arbitrary code named by
data. A document that could name code to load is not validated data any more; it
is an executable payload.

So the library does not try. A stage backed by your lambda is a
[local stage](../reference/glossary.md#local-stage): the document records that a
`select` stands at this position, the delegate lives in a table beside the
document keyed by node identifier, and the document declares the capability
`nondeployable` to say so out loud. Nothing is silently attempted and nothing
silently fails.

### Why a document holds no CLR type names

Element types have to be checked — a `Flow<OrderEvent, OrderDocument>` must not
connect to a `Sink<Invoice>` — but the check cannot be "do these two
assembly-qualified type names match". A type name ties the document to one
runtime's idea of identity, so the same pipeline written in F# and in C# would
produce different documents, and a silo running a different assembly version
would reject a document that is in fact fine.

Instead a port declares a **contract reference**: a stable identifier plus a
major version, such as `order-created@v1`. Your deployment asserts, in code, "in
this process, contract `order-created@v1` is the CLR type `OrderCreated`". The
document stores only the reference. Two processes that agree on the reference and
bind different CLR types to it is a deployment error the document cannot see —
that limit is stated rather than hidden, and it is checked where it can be
checked, by comparing catalog fingerprints and by the cluster's own serializer
contracts.

### Why a document holds no connection strings

A document is durable, comparable, loggable, and — on a cluster — readable by
anything that can address the coordinator. Infrastructure credentials have no
business in an artifact with those properties. Where a stage needs a connection,
the *name* of the thing it connects to is a parameter and the connection itself
is registered on the host. Documents describe topology; deployments hold secrets.

## Canonical form: one document, one spelling

A document has exactly one byte form. Not one preferred form — one form, and
readers reject anything else rather than normalizing it.

- UTF-8 with no byte order mark, minified, no insignificant whitespace anywhere.
- Every object type has a fixed property order defined by the format version, and
  every property is always written, in that order, with nothing omitted for being
  a default. An absent optional value is an explicit `null`.
- Collections are canonically ordered: nodes by node identifier, edges by
  (from-node, from-port, to-node, to-port), result slots by slot identifier,
  capability tokens by token — all ordinal.
- Numbers are integers only, must fit in a signed 64-bit integer, and are written
  in minimal decimal: no leading zeros, no fraction, no exponent, no `-0`.
- Strings use a fixed escape table — `"`, `\`, and control characters as
  lowercase `\u00xx`, nothing else. Non-ASCII stays as raw UTF-8 bytes, and no
  Unicode normalization is applied: the bytes you supplied are the bytes stored.

The integer rule is the one that catches people, so it is worth its sentence:
floating point has more than one spelling for the same value and more than one
value for the same spelling across runtimes, which would make "one document, one
byte form" false. Anything fractional is modeled explicitly instead — durations
travel as integer ticks, rates as integer permille — so the schema states the
precision rather than inheriting it from a numeric type.

[Canonical JSON](../reference/glossary.md#canonical-json) is what makes the next
section possible. Without it, a fingerprint would be a checksum of one particular
serializer's mood.

## The fingerprint

The [fingerprint](../reference/glossary.md#fingerprint) is the SHA-256 of a
document's canonical bytes, rendered as `sha256:` followed by 64 lowercase
hexadecimal digits. Because there is exactly one byte form, **the fingerprint is
an identity, not a checksum**: two documents share a fingerprint when and only
when they are the same document, and a fingerprint computed on one machine, one
runtime, or one process is the same number computed anywhere else.

It gets used for four things.

**Comparing two authorings.** The sample application writes every scenario twice,
once in C# and once in F#, and asserts that both produce the same 32 bytes. A
frontend that had drifted would still compile; the fingerprint is what makes the
drift a build failure.

**Refusing a resume under the wrong document.** A [durable run](../reference/glossary.md#durable-run)
stores its fingerprint with its position. An attempt to continue it under a
document that is not the one it was written for is refused by name, with both
fingerprints in the message. There is no attempt to guess whether the change was
compatible.

**Binding a result slot to a run.** A slot carries the fingerprint of the
document that declared it, and a run accepts a slot only from its own document.

**Ordering by content rather than by object.** A document's ordinary object hash
code is seeded per process and is meaningless across one; the fingerprint is what
a store, a log, or a comparison uses.

### The fingerprint identifies shape, not behavior

This is the sharpest thing on this page, and it is a direct consequence of a
document holding no delegates. Consider two graphs that differ only in what their
lambda computes:

```csharp
RunnableGraph doubling = Source.From(new[] { 1, 2, 3 })
    .Select(n => n * 2)
    .To(s => s.Aggregate(0L, (sum, n) => sum + n), "total", out ResultSlot<long> doublingTotal);

RunnableGraph tripling = Source.From(new[] { 1, 2, 3 })
    .Select(n => n * 3)
    .To(s => s.Aggregate(0L, (sum, n) => sum + n), "total", out ResultSlot<long> triplingTotal);
```

```text
doubling sha256:1e2cc15c4ae0aa7db7d1891ffbfe1db5e92d9779cb85c798b95e7a7f0b23bbaf
tripling sha256:1e2cc15c4ae0aa7db7d1891ffbfe1db5e92d9779cb85c798b95e7a7f0b23bbaf
same fingerprint: True
```

The same, and correctly so: the documents *are* identical, because neither
records what its `select` computes. Which raises an obvious hazard — if a slot
bound only to the fingerprint, `triplingTotal` would happily resolve against a
run of `doubling` and hand you a number computed by the wrong code.

It does not, because a slot of a local graph also binds to the **instance** that
declared it:

```text
same slot:        False
doubling slot renders as total@sha256:1e2cc15c…#631fe317
tripling slot renders as total@sha256:1e2cc15c…#4a075e5c
```

and resolving the wrong one against a run says exactly what happened:

```text
The slot 'total' belongs to a different graph: its document fingerprint sha256:1e2cc15c…
matches this run, but it was declared by another built instance of that same shape. A
document records no delegate, so two graphs built from different lambdas share a
fingerprint; a slot therefore also binds to the instance that declared it.
```

A deployable pipeline needs no such instance identity, and carries none: every
stage of one resolves from a catalog by name, so its content identity means
something on its own. The two kinds of slot are not interchangeable, and a run
tells you which world a mismatched slot came from rather than reporting two
fingerprints that happen to differ.

### Order is identity

The document lists nodes in canonical order, but the *identifiers* are assigned
in authoring order, so reordering a chain builds a different document:

```text
where-then-select sha256:557fd596b9e3d72765684df7adf69af2063132c919534d678aff2604a46f5c88
select-then-where sha256:58c21a6304074885fc18fe2ddb6281c02f06cecf17c2769163b9a5325c4e949e
same: False
```

That is correct — filtering before mapping and mapping before filtering are
different pipelines — and the same rule holds for the argument order of a
junction's branches. Numbers count too: a buffer's capacity is in the document,
so a buffer of 16 and a buffer of 8 are two pipelines.

```json
"stageRef": { "providerId": "local", "stageId": "buffer", "majorVersion": 1 },
"parameterContract": { "contractId": "local-buffer-parameters", "majorVersion": 1 },
"parameters": { "capacity": 16, "overflowPolicy": "drop-oldest" }
```

## Occurrence names

A [stage kind](../reference/glossary.md#stage-kind) is `select`. An
[occurrence](../reference/glossary.md#occurrence) is *this* select, *here*, in
*this* graph. Two `Select` calls in one pipeline are two occurrences of one kind,
and each gets a node identifier.

Occurrence names exist because several things need to point at a stage and
survive doing so:

- **A diagnostic** names the occurrence. "The stage `discount` refused its
  parameters" is a sentence you can act on; "the third lambda" is not.
- **A checkpoint** is keyed by node identifier, because a node identifier is the
  one name a document and a stored position both agree on. Without stable names,
  nothing could say *where* a resumed run had got to.
- **An upgrade** needs to know which node in the new document is the same node as
  in the old one.

Node identifiers follow one grammar: lowercase letters and digits in
hyphen-joined segments (`orders-of-the-day`), one to sixty-four characters per
segment, ASCII only, compared ordinally. A node identifier may be a `/`-joined
path of such segments — that is how fragments scope theirs — up to sixteen levels
deep.

When you do not name an occurrence, the frontend allocates one for you:
`stage-0001`, `stage-0002`, in authoring order, zero-padded so that document
order equals authoring order. Those names are **positional**, so inserting a
stage renumbers everything after it, and nothing durable can be anchored to them.
A document containing any of them declares the capability `ephemeral-identity`
about itself for exactly that reason.

## Identity and revision

A `RunnableGraph` is anonymous: its document carries the placeholder identity
`anonymous` and revision 1. Give it a real identity and you get a
[pipeline](../reference/glossary.md#pipeline):

```csharp
PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("sample-orders"), GraphRevision.Create(1));
```

- **`GraphId`** names a *lineage* — the pipeline as a thing that evolves.
- **[`GraphRevision`](../reference/glossary.md#revision)** is a positive integer,
  starting at 1, that you increase when the shape changes.

The identity is document content, not a label stuck on the side. `AsPipeline`
therefore re-closes the whole document under the real identity and revalidates
every invariant, which means **a pipeline's fingerprint differs from the
fingerprint of the anonymous graph it was made from**. That difference is the
point rather than an annoyance: what you want to compare, store, and refuse to
resume under is the *deployable* document, and a fingerprint taken over a
placeholder identity would identify something you never deploy.

Two revisions of one identity are two documents with two fingerprints — the
revision is part of the canonical bytes, so it could not be otherwise. That makes
"run the new revision beside the old one" the trivial case: two revisions already
have two positions, two ownership sequences, and two endings, because they are
two documents.

## Deployable, or local-only

Whether a graph can leave your process is not a judgment call. It is two
capability tokens the document declares about itself, and either one is
disqualifying:

| Token | What it says | How a graph gets it |
|---|---|---|
| `nondeployable` | A stage's behavior is bound in this process and reaches no document. | Any lambda-backed stage: `Select(x => …)`, `Where(…)`, a routing function, a fallback value, a sink callback. |
| `ephemeral-identity` | Node identifiers are positions rather than author-chosen names. | Any unnamed occurrence. |

`AsPipeline` refuses a document declaring either, listing **every** violation
rather than the first, and it does not strip them. A graph carrying those tokens
is not a pipeline with a caveat; it is a different kind of graph. A pipeline
built entirely from registered stages under names you chose never had them.

There is one composition worth knowing in advance: a
[junction](../reference/glossary.md#junction) authored through the fluent surface
is itself a local, unnamed stage, so a branching graph carries both tokens even
when its source, its flows and its branch sinks are all registered. Making a
branching pipeline deployable needs a provider that registers the junction
itself. See [Writing a custom stage](../guides/custom-stages.md).

## Fragments, and why importing one twice does not collide

A [fragment](../reference/glossary.md#fragment) is a reusable piece of a graph —
some nodes, the edges between them, and a list of ports still open at each end.
Fragments are how a `Flow` you keep in a variable and use in three pipelines
works.

The obvious problem with reuse is names. If a fragment contains a node called
`normalize` and you import it twice, you have two nodes called `normalize`, and
the document's "node identifiers are unique" invariant is broken.

The answer is **scoping by prefix**. Importing a fragment under a scope segment
`s` rewrites every node identifier `p` inside it to `s/p`, and rewrites every
edge endpoint and open port to match. Because it is pure prefixing:

- it is deterministic — the same fragment under the same scope produces the same
  identifiers every time;
- it composes — nesting an import inside an import nests the prefixes;
- it is collision-free across distinct scopes, by construction rather than by a
  uniqueness check.

So importing one fragment twice gives you two independent copies whose stages are
`first/normalize` and `second/normalize`, and a checkpoint or a diagnostic that
names one of them is unambiguous about which copy it meant. This is also why
fragments themselves declare no result slots: a slot identifier is a single
segment and cannot be path-scoped, so slots are declared when a graph is *closed*
and refer to the (already scoped) node that produces them.

## Limits worth knowing

These exist so a generated or hostile document is refused rather than absorbed.
A pipeline a person wrote never meets any of them.

| Bound | Value |
|---|---|
| Document bytes, measured before decoding | 4 MiB |
| Nodes in a document | 10,000 |
| One canonical parameter payload | 256 KiB |
| Payload nesting depth | 64 levels |
| Duplicate keys in a payload object | rejected |

## Where to go next

- [Runs and results](runs-and-results.md) — from a document to a running thing,
  and how values come back.
- [Writing a custom stage](../guides/custom-stages.md) — how to register
  behavior by name so a graph becomes deployable.
- [The cluster model](cluster-model.md) — what a silo does with a document it is
  handed.
- [Durability](durability.md) — why a stored position is refused under a
  different fingerprint.
