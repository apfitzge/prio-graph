# prio-graph ![example workflow](https://github.com/apfitzge/prio-graph/actions/workflows/rust.yml/badge.svg)

A library for building a directed acyclic graph that is lazily evaluated as new
transactions are added. Edges are only present for the next-highest priority
conflict for a particular resource,.

The `PrioGraph` structure keeps track of the nodes in the graph, the directed
edges between them, and a main queue. For example:

```mermaid
graph LR;
A((A)) --> B((B)) --> C((C)) & D((D));
E((E)) --> F((F));
```

`A` and `E` have no conflicts and are the highest priority items within their
prospective chains. These node's associated ids would be in the main queue.
If a transaction were added that conflicts with both chains, then these chains
would be joined.

```mermaid
graph LR;
A((A)) --> B((B)) --> C((C)) & D((D)) --> G((G));
E((E)) --> F((F)) --> G;
```
