# PINGMapper × VForce — Flow + Lakehouse, working together

The reference example for how **VForce Flow** (orchestration) and **VForce Lakehouse** (storage + catalog + analytics) combine, using [PINGMapper](https://cameronbodine.github.io/PINGMapper/) to turn consumer **side-scan sonar** recordings into georeferenced habitat imagery.

```
PINGMapper_lakehouse_flow.ipynb     process a recording → store the imagery in Lakehouse → the Flow pipeline that ties it together
PINGMapper_lakehouse_cleanup.ipynb  delete the ingested batch (objects + gold rows + jobs); admin purge for a full reset
flows/pingmapper-ingest.yaml        the Flow pipeline as code (validated by flowc)
```

## The pattern
1. **Process** — `read → rectify → map` (PINGMapper) → GeoTIFFs, sonograms, substrate maps.
2. **Store** — `POST /api/v1/ingest/archive` (or `/ingest/document`) → the imagery becomes Lakehouse objects + catalog rows, searchable and queryable in SQL.
3. **Orchestrate** — `flows/pingmapper-ingest.yaml`: a Flow triggers on a new recording, calls PINGMapper, and uses the **`lakehouse`** connector to store the results. Runs show in the Flow **Runs** view.

## Flow ↔ Lakehouse is bidirectional
- **Flow → Lakehouse**: Flow's `lakehouse` connector (`ingestDocument`, `ingestData`, `runSQL`) — a flow reads/writes the lakehouse (this repo).
- **Lakehouse → Flow**: Flow is embedded in the Lakehouse as **Jobs & Pipelines / Lakeflow** — the lakehouse launches and monitors flows.

## Sourced in git (Flow Projects → repo)
A Flow **Project** binds to a git repo, so the flow YAML here is the source of truth (git-sync + write-back). `flowc build flows/ -o dist/flowpack.tar.gz` compiles it into a deployable object any CI/CD can ship.
