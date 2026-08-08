-- P2 (work.next selection liveness): make the side-effect-resolution decision-receipt lookup and the
-- per-candidate agentWorkTaskSuperseded checks SARGABLE. Those queries filter agent_updates by
-- (workspace_id, update_type = 'side_effect_resolution'); the only prior index was
-- (workspace_id, created_at), which cannot serve the update_type predicate, so each query row-scanned
-- the whole workspace's agent_updates. As agent_updates accreted (~20k rows) and the number of live
-- task-side-effect-* candidates grew, the pre-selection receipt sweep blew the work.next RPC deadline
-- (context deadline exceeded) and aborted candidate selection roster-wide -- the c2 integration stall.
-- This composite index bounds those scans to the side_effect_resolution rows for the workspace.
CREATE INDEX IF NOT EXISTS idx_agent_updates_workspace_type
  ON agent_updates(workspace_id, update_type, created_at DESC);
