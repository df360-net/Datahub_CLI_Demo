# DataHub Fundamentals — Learning Plan

A guided curriculum for Jianmin, grounded in the CardCompass project he built.

## The one idea everything hangs on

**DataHub is a graph database with a stream at its heart. Every piece of metadata is an (Entity, Aspect) pair, addressed by a URN, written as an event, logged, and fanned out into three read-optimized stores.**

Everything else is detail hanging off that sentence. CardCompass already exercised most of it — this curriculum names what was absorbed implicitly.

## Foundational concept: the graph

A graph database stores data as two things: **nodes** (the things) and **edges** (the relationships between them). The defining move is that **relationships are stored directly, as first-class data** — not computed at query time.

**Relational way (MSSQL):** relationships are *implicit*. `card_txn.merchant_id` points at `merchant.merchant_id`, but nothing is stored as a relationship — you reconstruct it every query with a JOIN. One hop is cheap; a multi-hop traversal ("everything downstream of `card_auth`, however many hops") needs recursive CTEs and gets progressively expensive with depth.

**Graph way:** store the relationship itself as an edge.

```
(card_auth) --feeds--> (card_txn) --feeds--> (daily_summary)
                                  --feeds--> (top_merchants)
```

"What's downstream of `card_auth`?" is now a **walk**, not a join — follow the `feeds` edges outward. Three hops costs three pointer-follows, not three nested joins. Both nodes and edges can carry properties.

- Edges are **directed** and **typed** — `DownstreamOf`, `OwnedBy`, `PartOf` are different edge types, and direction matters.
- A node plus its edges is its **neighborhood**.

**Why DataHub is built this way:** almost every question it answers is a relationship question — "if `card_auth` is late, what breaks?", "who owns this?", "what's in the Payments domain?", "trace this field to its source." These are traversals. A graph makes them cheap. So DataHub models datasets, columns, users, and domains as **nodes**, and lineage, ownership, and containment as **edges**.

**Nuance (see Module 4):** DataHub isn't *only* a graph. The graph is one specialized read store optimized for traversal. Truth lives in a relational store (MySQL); search lives in a third (OpenSearch). DataHub copies relationship data into a graph index so traversals fly, while keeping truth elsewhere.

### Typical DataHub typed edges

Edges are not a fixed enum — they are declared by `@Relationship` annotations on aspect fields in DataHub's PDL models, so the set is extensible. The canonical ones:

| Edge | From -> To | Declared on aspect |
|---|---|---|
| `DownstreamOf` | Dataset -> upstream Dataset (also Job -> upstream Job) | `upstreamLineage` |
| `Consumes` | DataJob -> input Dataset | `dataJobInputOutput` |
| `Produces` | DataJob -> output Dataset | `dataJobInputOutput` |
| `OwnedBy` | any entity -> CorpUser / CorpGroup | `ownership` |
| `IsMemberOfGroup` | CorpUser -> CorpGroup | `groupMembership` |
| `IsPartOf` | entity -> Container; DataJob -> DataFlow; child Domain -> parent | `container`, `dataJobInfo`, domain |
| `TaggedWith` | entity / schemaField -> Tag | `globalTags` |
| `TermedWith` | entity / schemaField -> GlossaryTerm | `glossaryTerms` |
| `AssociatedWith` | entity -> Domain | `domains` |
| `ForeignKeyToField` / `ForeignKeyToDataset` | schemaField -> schemaField / Dataset | `schemaMetadata.foreignKeys` |
| `IsA`, `HasA`, `RelatedTo`, `ValueOf` | GlossaryTerm -> GlossaryTerm | `glossaryRelatedTerms` |
| `InstanceOf` | DataProcessInstance -> its template (Job / Flow) | `dataProcessInstanceRelationships` |

Two things to internalize:

1. **Every edge is born from an aspect.** You never create edges directly — you write an aspect (e.g. `ownership`), and the `@Relationship`-annotated field inside it *becomes* the edge when GMS indexes it. In CardCompass, the `upstreamLineage` aspect is literally what minted the `DownstreamOf` edges.
2. **Direction is stored one way, queryable both.** You assert `DownstreamOf` pointing at the upstream, but DataHub indexes the inverse too, so the UI answers both "what feeds this?" and "what does this feed?" from one stored edge.

### Are the relationship names predefined?

No — not by the framework. The `@Relationship` annotation has a `name` field that is just an arbitrary string the model author types. There is no master enum the framework validates against; the edge "type" *is* whatever string was written.

```pdl
record Upstream {
  @Relationship = {
    "name": "DownstreamOf",        // just a string literal, chosen by the author
    "entityTypes": [ "dataset" ]    // which target entity types are legal
  }
  dataset: DatasetUrn
}
```

So the standard names are predefined only **by convention in DataHub's shipped core models** (`UpstreamLineage.pdl`, `Ownership.pdl`, ...), not **enforced by the framework**. Extend the model with a custom aspect and you can declare your own `@Relationship` name; GMS will index a brand-new edge type with no framework changes. One guardrail: `entityTypes` constrains *what kind of node* an edge may point at, so names are open but each edge is type-checked on its targets.

### The crucial caveat: name-agnostic engine, name-aware product

It is tempting to conclude "DataHub gives no special treatment to any relationship." That is only half true, and the other half matters. Think of it as **two layers that disagree**:

- **The graph / storage layer is name-agnostic.** GMS indexes every edge the same way regardless of name. A `DownstreamOf` edge and a hypothetical `Banana` edge are stored, traversed, and queryable via the generic relationships API identically.

- **The application layer is deeply opinionated about specific names.** Large parts of DataHub are hard-wired to known relationship names and aspects:
  - **Lineage** — the Lineage tab, impact analysis, and column-level lineage only light up for edges flagged as lineage. The flag lives on the annotation itself:
    ```pdl
    @Relationship = { "name": "DownstreamOf", "isLineage": true, "isUpstream": true }
    ```
    A custom `FeedsInto` edge is stored and traversable, but will **not** appear in the Lineage tab.
  - **Ownership** — the owners panel, the "owned by me" search filter, and ownership policies specifically know `OwnedBy` / the `ownership` aspect.
  - **Domains, Tags, Glossary** — each has a dedicated UI surface, a search facet, and bespoke GraphQL resolvers tied to its named relationship.

So a custom edge is a **first-class citizen of the graph but a second-class citizen of the product**: reachable via the relationships / GraphQL API, but no UI tab, no search filter, no impact analysis for free.

This is what turns "follow conventions" from etiquette into engineering. Using `DownstreamOf` inherits the lineage graph, impact analysis, and column lineage at no cost. Inventing `FeedsInto` throws all of that away for an edge only an API call can see. **Invent a new relationship only when you are also prepared to build (or do without) the product surface that reads it.**

### How you actually query a graph

A common intuition is "you query a graph by using relationships to glue the nodes together." That is half right — and the missing half is the most important difference from SQL. A graph query has **two distinct moves**, and relationships are only one of them:

1. **Anchor — find your starting nodes by property.** A query almost always *begins* with a node lookup on an attribute, not a relationship: "the dataset whose URN is `...card_auth...`", or "all datasets tagged PII". That is an index lookup on node properties — no edges involved yet.
2. **Traverse — walk relationships to reach other nodes.** *Now* relationships come in. You follow edges outward from the anchor to connected nodes. This is the part the "glue" intuition is reaching for.

So a graph query reads like: *anchor by property -> follow edges -> maybe filter by property along the way -> follow more edges -> return.* In a CardCompass-flavored pseudo-query:

```
MATCH (d:Dataset {urn: "...card_auth..."})<-[:DownstreamOf*1..5]-(downstream)
RETURN downstream
```

Find the anchor node by its URN (property), then walk up to 5 hops of `DownstreamOf` edges to collect everything downstream.

**The refinement that matters:** "glue" suggests *joining things by matching values* — which is what a SQL JOIN does (`ON t.merchant_id = m.merchant_id`, matching key values at query time). A graph traversal does **not** match values. The edge is a **stored pointer** that already knows which two nodes it connects, so traversal is just *following the pointer* — no comparison, no scan.

That is the whole performance story:

- **SQL JOIN** = "find all rows where these column values are equal" — work proportional to table size, repeated every hop.
- **Graph traversal** = "follow this edge to the node it already points at" — work proportional to the number of edges actually walked.

A truer phrasing: relationships are the **roads you travel between nodes**, and node-property lookups are **where you get on the road**. You do not glue nodes together at query time — the edges were laid down at write time (minted from aspects), and querying just walks them.

**DataHub specifics:** you never write raw Cypher against it. GMS wraps the graph behind its relationships API and GraphQL, so you ask "give me downstream entities of this URN" and it does the anchor-and-traverse for you. The mental model is exactly this; the syntax is hidden.

## Foundational concept: the stream

The stream in "a graph database with a stream at its heart" is **Kafka** — the `datahub-kafka-broker-1` container on Murphy. But "stream" means more than "Kafka is installed": DataHub is a **log-driven architecture**. Every committed metadata change becomes an **event** on a Kafka topic, and the read stores (search, graph) are **materialized views built by consuming that event log**. The log is the spine; the indexes are derivatives.

The topics that matter:

**Write side — proposals**
- `MetadataChangeProposal_v1` (**MCP**) — "please change this aspect." A *request*, not yet truth.

**Commit side — the actual stream**
- `MetadataChangeLog_Versioned_v1` (**MCL**, versioned)
- `MetadataChangeLog_Timeseries_v1` (**MCL**, timeseries)

That split is significant: the versioned-vs-timeseries distinction goes all the way down to **separate Kafka topics**. CardCompass's `assertionInfo` (versioned) and `assertionRunEvent` (timeseries) literally travel on different streams.

### How a CardCompass write actually flowed

The `DatahubRestEmitter` POSTed synchronously to GMS (`/api/gms/aspects`) — it did *not* write to Kafka directly. But internally:

```
emitter POST -> GMS commits aspect to MySQL (truth)
            -> GMS produces an MCL event to Kafka   <- the stream
            -> consumers read MCL, update OpenSearch (search) + graph (lineage)
```

GMS is the only writer of truth; the **MCL is what it announces after each commit**, and the indexers are just Kafka consumers downstream.

### This is the mechanism behind CDC idempotency

When a byte-identical versioned aspect is re-emitted:

```
emitter POST -> GMS sees the aspect is unchanged -> commits nothing
            -> NO MCL event produced
            -> consumers have nothing to read -> indexes untouched
```

No event on the stream means no change propagated. That is why a stable `assertionInfo` no-ops while a daily `assertionRunEvent` always appends — the timeseries aspect always produces a new event on its topic. "Stream at its heart" means the whole system is *commit -> emit event -> consumers materialize views*.

## Structure

The plan splits into the **core machine** (the real fundamentals) and the **application layer** (how you use it). Building CardCompass already covered much of Part B; Part A is the part absorbed implicitly and never named.

### Part A — the core machine (the actual fundamentals)

1. **The atom: URN + Entity + Aspect.** Why metadata is sliced into typed aspects, not one big record. The `dataset` URN and `schemaMetadata` aspect from CardCompass.
2. **Versioned vs timeseries aspects.** The deepest distinction in DataHub. Proven in CardCompass: `assertionInfo` (versioned, no-ops when identical) vs `assertionRunEvent` (timeseries, appends daily).
3. **The write path: MCP -> GMS -> MCL.** What actually happened when the emitter POSTed. Why identical versioned aspects produce *no* change log — the CDC behavior.
4. **The serving architecture: 5 stores, one truth.** GMS + MySQL (truth) + OpenSearch (search) + graph (lineage) + Kafka (the stream). Each maps to a container running on Murphy.
5. **The graph: relationships & lineage.** How aspects become edges. The CardCompass table + column lineage, and how the index answers "what's downstream."

### Part B — using the machine

6. **Search & discovery** — DataHub's whole reason to exist.
7. **API surfaces** — Rest.li (`/api/gms`) vs OpenAPI (`/openapi/v3`) vs GraphQL. CardCompass used the first two; GraphQL drives the UI.
8. **Ingestion framework** — push (what CardCompass did) vs pull (recipes/sources/transformers — the dominant pattern not yet used).
9. **Organizing & governing** — domains, glossary, tags, owners, data products, structured properties.
10. **Access & policies** — the frontend session the Auth Proxy wraps.

## How each module is taught

Three beats, kept short:

1. **Concept** — the idea itself.
2. **Grounded** — tied to a CardCompass artifact already built.
3. **Verify live** — inspect it against Murphy (GMS is up: real entities, the write path, even the Kafka MCL stream).

Then one check question before moving on.

**Recommended pace:** Part A deeply, one module per sitting, hands-on against Murphy. Part B can mostly be sped through, since it was lived during CardCompass.

## Murphy reference (the live lab)

- DataHub frontend / UI: http://192.168.0.16:9002
- DataHub GMS: http://192.168.0.16:8080
- MSSQL: 192.168.0.16:1433, DB `DCF_DB`

## Progress

- [ ] Module 1 — URN + Entity + Aspect
- [ ] Module 2 — Versioned vs timeseries aspects
- [ ] Module 3 — The write path: MCP -> GMS -> MCL
- [ ] Module 4 — The serving architecture: 5 stores
- [ ] Module 5 — The graph: relationships & lineage
- [ ] Module 6 — Search & discovery
- [ ] Module 7 — API surfaces
- [ ] Module 8 — Ingestion framework
- [ ] Module 9 — Organizing & governing
- [ ] Module 10 — Access & policies
