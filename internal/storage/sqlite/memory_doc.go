package sqlite

/*
Memory Data Architecture Note (Phase 4B Decision)

This subsystem handles autonomous information lifecycle and retention.

Current State:
The subsystem currently heavily utilizes fragmented shapes (`workspace_memory`, `knowledge_claims`, `session_compaction_snapshots`) as the primary durable substrates.
The `memory_graph_nodes` structure serves as a wider, mixed-shape lattice that calculates dimensions like epistemic drift and salience.
At present, there is recognized synchronous graph coupling/debt (e.g., in runtime_memory, control_plane, episode_packs) where the graph is updated synchronously alongside primary records.

Branch Decision (P4B-002):
The chosen branch is `retain_current_canonical_shape`.
`memory_graph` is NOT the canonical durable substrate.
It remains a derived compatibility/projection surface.

Canonical Durable Shape (P4B-003):
1. `workspace_memory` is authoritative for workspace memory note content and archive/recovery lifecycle.
2. `knowledge_claims` is authoritative for epistemic claim type, status, supersession, conflict, review, and archive lifecycle.
3. `session_compaction_snapshots` and `episode_packs` are authoritative for compaction/replay summary artifacts and their lineage.
4. `memory_graph` node fields such as `memory_type`, `memory_layer`, `epistemic_status`, `lifecycle_state`, drift, salience, and retention state are derived compatibility/projection state, not source-of-truth state machines.
5. Projection nodes must keep stable origin mapping (`origin_kind`, `origin_id`) back to canonical rows and must not invent authority that contradicts the canonical record.

Operational Consequence:
The synchronous updates mentioned above are remediation targets, not proof of canonical authority.
They should be moved out of hot write paths via reconciler/outbox infrastructure in Phase 4C where safe.
*/
