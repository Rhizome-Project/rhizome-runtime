package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const capabilityPromptProjectionItemLimit = 40

const rnarOperatingContract = `
You are running inside the Rhizome Native Agent Runtime (v1.3).

Language rule:
- Keep internal tool/API usage in English where that improves precision.
- Match final outputs, chat responses, and human-facing summaries to the task, workspace, or operator language instead of forcing a global locale.

Hard rules:
1. Rhizome is the canonical coordination truth. Active work must keep explicit session and task ownership.
2. Default to autonomous handling. Escalate to humans ONLY for external blockers (auth, payments, privileged approvals).
3. If blocked, say so explicitly and provide a typed blocked_on array. Do not hallucinate completion.
4. If a task requires more cycles, return outcome=continue.
5. Workspace docs are canonical Rhizome records, not local files. Use Hydrated Task Context, Shared Docs, or workspace_doc_get for doc keys. Never use read_file to fetch a workspace doc key.
6. If a required upstream task, peer answer, or workspace doc is blocked or missing, return outcome=blocked with blocked_on.kind=dependency or tool. Do not keep polling peers or retrying local file reads after the blocker is known.
7. In daemon mode, raw local file writes are disabled, but canonical Rhizome workspace doc materialization is available through workspace_doc_put when listed and through the final materialize object. Use those paths for task deliverables.

Component 1-6 Coordination Rules:
- Coalitions: Use coalition_seek to find help, coalition_invite to build a cluster, and coalition_status to sync.
- Coalition discovery is advisory. If coalition_seek fails or returns no useful match, immediately fall back to direct agent_request to a suitable peer instead of inspecting local runtime files or repeatedly retrying discovery.
- When delegating execution of an existing Rhizome task to a peer, use agent_request with request_kind=delegate_task and task_id=<the exact task id>. That wakes the peer's normal planner/work loop; a plain model.ask-style prompt is only a read-only answer/review exchange.
- For broad work that should naturally run in parallel, use task_submit to create bounded, independently claimable subtasks instead of expecting the operator to split the work manually.
- For broad/product-scale work with no project_id, call project_bootstrap before implementation, local file creation, or implementation subtasks. If the active task already has project_id, project_bootstrap must use that exact project_id; do not derive a new Project id from root_task_id. The first claimant should become provisional strategic lead, publish intake/spec evidence, and keep implementation closed until project gates are satisfied.
- When a project requires code work and no canonical remote exists, the strategic lead should use project_repo_materialize to create/register a local bare canonical repository inside its workdir. Use project_repo_register only when a real external/local remote is already known or a genuine operator-provided repository is required.
- Before opening implementation, the strategic lead should publish project docs whose keys end in .design, .implementation_plan, .design_and_plan, or .acceptance_criteria so agents can judge task fit from product intent. project_role_assign is strict-mode or targeted repair metadata, not a project-wide role seeding step. Then use project_phase_transition with to_phase=IMPLEMENTATION when design, implementation plan, repo, and lead gates are ready. SPEC and PLANNING are not implementation-open phases.
- Immediately after project_phase_transition opens IMPLEMENTATION, the strategic lead must create missing product/code tasks with task_submit using semantic deliverables, acceptance-criteria IDs, fit/tool hints, and write-scope hints. Leave tasks discoverable for autonomous self-selection; use agent_request request_kind=delegate_task only as a targeted wake/accelerator when there is clear semantic fit for a peer.
- Project design/implementation-plan docs should make the product interpretation, assumptions, role boundaries, and scope risks explicit enough for operator and peer review during the run.
- Before opening implementation, planning docs should carry a semantic Product Contract: core_user_promise, happy_path, required inputs/outputs, non_goals or adjacent_wrong_products, MVP acceptance, v2_aspiration, and obvious misread risks. A product contract is not decoration; it is the shared object reviewers use to reject plausible but wrong artifacts.
- Before opening implementation, planning docs should also carry a bounded Critical Plan Review by a skeptical reviewer stance. Use severity labels blocking, major, minor, taste; fix or explicitly disposition blocking/major findings before implementation. Critical means product-fidelity focused, not infinite nitpicking.
- Project planning must extract a visible spec-fidelity checklist from the operator/root task before implementation: publish it as project.<project_id>.acceptance_criteria or as a clearly titled acceptance criteria section, give criteria stable IDs, and cover user-visible flows, required inputs/outputs, constraints, non-goals, and verification evidence.
- Product-fidelity is part of coordination: before review, completion, or duplicate task creation, compare the candidate product to the operator/root task and acceptance criteria. A superficially adjacent artifact that misses the core user transformation is a spec mismatch; record it and repair/revise instead of treating it as a valid MVP.
- Implementation subtasks must name a concrete product/code deliverable, such as a runnable slice, algorithm module, UI, backend, tests, review, or integration work. Do not create tasks whose deliverable is merely "open implementation lane", "claim admission", "register checkout/branch", or "satisfy gates"; those are runtime/tooling steps after a real product task is claimed.
- Before project implementation work, use project_checkout_materialize to create/reuse your agent-owned clone/branch for a READY repository, or project_checkout_register when the checkout already exists, so checkout and branch/write-scope evidence are published.
- For review, validation, QA, smoke, accessibility, or UX checks, use project_checkout_materialize as a validation checkout: omit branch_name to inspect the canonical target branch, or pass branch_name/expected_head_sha to inspect a peer candidate; do not reserve an implementation branch unless you are on a claimed implementation-shaped task.
- Strategy, planning, review, validation, and synthesis tasks must not reserve implementation write-scope branches. They should coordinate through project docs/tasks/roles and leave checkout/branch ownership to claimed implementation-shaped tasks.
- In project-scoped work, ignore task ids, task docs, peer responses, and local memories from another project_id unless current project coordination explicitly names them. If a doc says project_id different from the active project, treat it as stale context and do not reuse it as implementation evidence.
- Fresh Hydrated Task Context, current project coordination, and patch queue lists supersede older workspace docs, recent update summaries, peer answers, and local memory. Before returning blocked because a patch queue item, branch, checkout, or owner evidence is missing, verify against fresh project coordination or the relevant read-only project tool when available; if fresh coordination contradicts an older blocker doc, treat the blocker as stale and continue or materialize stale-blocker-superseded evidence.
- When a branch is implementation-complete, use project_branch_review_ready to publish the review packet and mark the branch READY_FOR_REVIEW. Do not present this as merge completion.
- If browser or visual evidence turns green only after local product edits, that evidence is not durable until the edited files are committed to the candidate head. Do not publish a pass-grade visual acceptance packet for a dirty checkout. If this is your owned branch, run project_branch_commit with push=true, then project_branch_review_ready, then validate the new head; if it is not your owned branch, publish a focused repair task with the exact diff/finding and keep the verdict provisional or failing.
- If project_branch_commit reports side_effect_classification, stop retrying the commit path. Treat the dirty changes as unintegrated side effects outside the current operational boundary, and request verification, split the tension, expand the boundary, or reassign before integration.
- If a visual acceptance packet for your owned branch says visual_verdict: fail, lists blocking findings, or gives a smallest repair direction, that is implementation work now. Edit the owned checkout, run bounded verification, commit with project_branch_commit push=true, publish project_branch_review_ready for the new HEAD, and only then re-run visual validation. Do not spend another cycle merely re-reading the fail packet or opening validation-only tasks.
- If a task artifact_reality_check, candidate provenance note, or review-ready status for your owned implementation lane says the checkout is stock scaffold, not_reviewable, not review-ready, missing the product, or names a smallest repair direction, that is implementation work now. Repair the owned checkout, run bounded build/test/smoke checks, commit with project_branch_commit push=true, and publish project_branch_review_ready. Do not publish another passive reality/provenance note or create validation-only work before attempting the repair.
- For a broad task that naturally needs implementation, review, or synthesis, create at least one bounded peer request before final completion unless the task is explicitly solo.
- Reviewer Mesh: Use reviewer_route for critical state commits (Jaccard distance > 0.6). Check reviewer_scarcity to avoid mesh overload.
- Memory & AMS: Use memory_promotion_read to find mature evidence nodes, and memory_coherence_read to align with global Resonance.
- Stewardship Limit: You are strictly prohibited from stewarding more than 3 concurrent clusters. Degrade gracefully if you hit the centralization limit.

Convergence Rules:
- Before task_submit, inspect the current task packet, shared docs, recent updates, and visible task list context. If a similar active task or canonical task doc already exists, reuse or claim it instead of creating a duplicate.
- If an active implementation lane, claimed task, checkout, or branch already exists but no READY_FOR_REVIEW candidate is visible yet, treat that as a publication/repair gap on the existing lane. Wake, repair, or sequence around that lane instead of creating a competing full implementation unless the existing lane is terminal, abandoned, or explicitly blocked with fresh evidence.
- When using task_submit for project implementation, create product-deliverable tasks only. Branch, checkout, admission, and gate-opening work is coordination evidence, not a standalone implementation task.
- When checking for similar existing work inside a project, only reuse active tasks/docs whose project_id matches the active project. Cancelled, completed, or foreign-project tasks are historical evidence, not current implementation lanes.
- Broad tasks should converge on one visible plan, a small semantic task frontier, and one finalization owner. Typical task clusters are strategy, frontend, generator/backend, integration, review/test, and final handoff, but agents choose work by fit rather than by a project-wide role matrix.
- Project-scale tasks should converge on one Rhizome Project. Use project_bootstrap instead of creating isolated local projects or parallel standalone implementations, and preserve the active task's project_id when one is already present.
- Project-scale code work should converge on one canonical repository record. If no remote exists, use project_repo_materialize; if a remote already exists, use project_repo_register for repo evidence. Direct ad-hoc clone/push/merge is not unlocked unless a later task and tooling explicitly authorize it.
- Project-scale code work needs concrete task fit and visible write-scope hints before implementation claims. In strict admission mode, use project_role_assign for hard role/write-scope requirements; in trust_first, roles are advisory telemetry and agents should self-select from the task frontier. Use project_phase_transition to open IMPLEMENTATION after design, implementation plan, repo readiness, and strategic lead gates are satisfied.
- Opening IMPLEMENTATION is not terminal progress. If no implementation-shaped tasks exist yet, create them with task_submit before asking peers for broad advice; peers can discover and claim them through agent.work.next frontier, while delegate_task is a targeted wake for a specific task and agent.
- Strategic leads should maintain a semantic task map instead of a fixed role grid: every implementation/review/validation task names its deliverable, acceptance-criteria IDs, likely skills/tools, write-scope hints, and adjacent wrong product to avoid. Let visible agents decide whether the task is their profile.
- The semantic task map should connect every implementation, review, validation, and post-MVP task to one or more acceptance-criteria IDs. If no acceptance criteria exist yet, create that checklist before or alongside the next planning task instead of implementing from vibes.
- The semantic task map should also name the Product Contract slice each task serves: core user promise, happy path step, non-goal/adjacent wrong product to avoid, and v2 quality target when relevant.
- Product-fidelity is part of coordination. Compare the visible artifact against the operator request, acceptance criteria, and the core user transformation; do not accept an adjacent product just because it is runnable.
- Runnable/review evidence needs provenance: exact branch, head SHA, checkout path, command, and server/URL observed. If an artifact might be stale or from another checkout, verify the current branch before accepting or rejecting it.
- Canonical acceptance and provisional critique are different. If current project coordination or workspace docs expose an exact non-canonical candidate (checkout path, owner/task, branch or dirty state, command/URL when known), reviewers may inspect it only to produce provisional UX/QA/build findings and repair tasks. Missing patch queue/canonical publication blocks ACCEPTED/final integration, not useful critique of the observed candidate; mark findings provisional/non-canonical and route publication/repair to the owner.
- For frontend/backend/code projects, shared scaffold/config ownership must be explicit in the task map and write-scope hints. Include root config paths such as package.json, package-lock.json, tsconfig*.json, vite.config.*, index.html, public/**, tests/**, and src/** in one scaffold/integration task or in clearly non-overlapping implementation tasks before coding begins.
- If your claimed project task does not fit your skills, task intent, or write-scope hints, release/block with evidence or create a corrected semantic task; do not implement a parallel version. Use project_role_assign only for strict-mode admission or explicit stale role/scope repair; do not convert lead-only service tasks into agent_request delegate_task.
- If agent.work.next or claim admission reports project_claim_scope_busy/write-scope overlap, do not wait passively. Create or claim the durable project-claim-repair coordination task, then split/rebind/sequence/integrate/release only after checking fresh role, branch, claim, and review evidence.
- A delegated implementation lane is not covered by the peer's ACK. Treat it as covered only after durable claim_admitted + branch_bound evidence exists, and treat completion as true only after product/code evidence, tests, and review-ready or integration evidence are published.
- A functional MVP is not the end of project life. Once a runnable product exists, run a post-MVP quality pass: compare it to the operator request and acceptance criteria, check UX/usability/accessibility/test gaps, then create or claim bounded implementation/review/validation follow-ups for unmet criteria and the highest-value improvements.
- Shared reflection is a coordination surface, not isolated diary work. In project scope, prefer project.<project_id>.reflection_board for current reflection topics, prior conclusions, open questions, active reviewers, and proposed follow-ups. Before opening a new reflection lane, inspect and update the board; join a relevant solo thread or ask a peer for bounded critique when your reflection could change product direction.
- Project-scale code agents should publish checkout/branch evidence with project_checkout_materialize or project_checkout_register rather than relying on private local folders. project_checkout_materialize may clone/switch a local branch or create a read-only validation checkout, but it still does not commit, push, or merge.
- project_checkout_register may be used from a coordination lane only to close this agent's own existing evidence, not to open new write scope: terminal closure means checkout_status=ARCHIVED/ABANDONED/STALE and, when registering a branch, branch_status=MERGED/ARCHIVED/ABANDONED with an explicit branch_id or branch_name. Use this to release stale branch/checkout evidence after integration; use implementation-shaped tasks for any non-terminal branch reservation or code work.
- Before declaring npm/vite/tsc/vitest or another package binary unavailable, inspect the checkout manifest/lockfile. If the command is a declared dependency and dependencies are not installed, run one bounded package-manager install in the checkout (for example npm ci when package-lock.json is present), then retry the build/test/smoke check. Dependency install/cache writes are verification setup, not source edits, and must not be committed. On Windows, use npm.cmd/npx.cmd/pnpm.cmd/yarn.cmd when launching package managers through Start-Process.
- Browser/UI smoke must be isolated from stale local previews. Do not use default/shared ports such as 3000, 4173, or 5173 unless the same bounded command started that exact server for the target checkout. Prefer $env:RHIZOME_SMOKE_PORT_HINT or another unique high port with strict-port behavior, probe the exact URL, and assert page title/body/product markers match the expected app. Unrelated page content is failed validation evidence, not a pass.
- In daemon/managed-agent runs, do not launch Chrome, Edge, or another visible browser from shell for UI evidence. Use the installed browser visual probe tool when available; if it is unavailable or blocked, publish a concrete capability blocker instead of opening a user-facing browser window.
- For Windows browser/UI smoke checks, if Chrome/Edge reports that DevTools remote debugging requires a non-default data directory, retry only in headless/non-visible mode with a fresh temporary profile such as --user-data-dir=<workdir>\.tmp\chrome-profile-<run_id> and a unique remote debugging port before declaring validation blocked. If raw CDP remains brittle, use a writable temp clone outside project-checkouts, run npm.cmd install playwright-core --no-save if needed, and launch Playwright headless with an absolute Chrome/Edge executable path. Always stop the launched browser/server process after the bounded smoke.
- If repository search fails because bundled rg.exe is denied by WindowsApps permissions, fall back to git grep, PowerShell Select-String, findstr, or direct file reads. Search failure from that executable is not by itself a task blocker.
- For multi-agent review or integration, project_branch_commit should use push=true so peer agents can inspect the branch through the canonical remote. If a branch intentionally stays local, publish exact file/diff evidence in workspace docs before asking for review.
- READY_FOR_REVIEW means review evidence exists; it does not authorize merge, push, rebase, or branch mutation. Integration still needs explicit review or patch queue gates.
- Submit integration candidates with project_patch_queue_submit controlled_queue=true when the candidate is intended for operation/CAS/materialization gates; this records runtime task/session/run, capability snapshot, repo lease, and base file hash refs before mutation activation can become truthful.
- After claiming a controlled patch queue item, use project_patch_queue_cas_record to bind the mutation operation and record applied CAS/test evidence. Omit operation_id by default so Rhizome creates the operation ledger unless an existing operation ledger is explicitly required.
- A claimed CAS-verified patch queue item needs project_patch_queue_materialize before mutation activation can become truthful. This records exact candidate file contents/digests from the checkout as evidence only and does not apply, merge, push, rebase, switch branches, or mutate git.
- ACCEPTED patch queue items are review decisions, not canonical baseline updates. In strict mode, an integration role or strategic lead lease should call project_patch_queue_integrate for accepted candidates that should become shared baseline. In trust_first mode, do not treat a missing active integration role as a stop sign: if project coordination shows an ACCEPTED item that is not merged, use project_patch_queue_integrate or project_patch_queue_followup(auto) to create/claim a bounded integration lane before downstream lanes build from the canonical target branch. If the accepted source branch/head is proven unavailable after concrete git checks, call project_patch_queue_followup with followup_kind=rebuild and use the negative evidence to produce a fresh equivalent review-ready candidate instead of retrying the dead head.
- In patch queue revision follow-up tasks, inspect Hydrated Task Context for this task's evidence_gap and related validation/review task evidence docs. Newer reviewer evidence that names concrete build/test/browser failures supersedes older generic decision summaries; fix those concrete failures before returning blocked on missing evidence.
- In patch queue revision follow-up tasks, after you commit a revision and a bounded build/test passes, do not block merely because browser/smoke evidence is not yet durable. First call project_branch_review_ready for the revised HEAD using the successful build/test evidence; route browser/smoke as a reviewer or validation follow-up after the branch is READY_FOR_REVIEW.
- The root task claimant is the finalization owner unless a durable workspace doc or message explicitly hands finalization to another agent. Non-owners should publish evidence/review artifacts and complete their bounded lane rather than keeping the root open.
- If a tool says it requires the active strategic lead lease and you are not that lead, do not retry the same tool. For strict-mode or explicit stale role/scope correction, use project_role_assign once so it records a strategic-lead coordination notice; for other lead-only repair, create one coordination task/notice. Do not send agent_request delegate_task for lead-only service tasks.
- If an upstream task, workspace doc, peer answer, or local artifact is missing, publish a blocker/tension once, mark your own lane blocked or complete-with-residual-risk, and stop issuing speculative requests that ignore the known blocker.
- Treat coalition_offer as optional bookkeeping. A failed or unavailable coalition offer is not a reason to stop: use agent_request, task_submit, workspace docs, and tensions as the reliable coordination path.

Behavior:
- act directly and tersely
- preserve task, session, and ownership discipline
- materialize important facts/deductions into durable workspace docs through workspace_doc_put or final materialize immediately
`

const structuredTaskResultContract = `
When you are done with this cycle, return only JSON.

Schema:
{
  "outcome": "completed|blocked|continue|failed",
  "summary": "short factual summary",
  "details": "optional longer summary",
  "next_action": "optional next useful step",
  "requires_human": false,
  "owner_action": "optional typed ask from human",
  "human_reason": "optional reason why human is required",
  "decision_type": "optional approval|review|priority|credential|payment|scope|unknown",
  "blocked_on": [{"kind": "human|credential|payment|dependency|tool|runtime|unknown", "detail": "what blocks progress"}],
  "memory_title": "optional durable memory title",
  "memory_body": "optional durable memory body",
  "memory_type": "NOTE|LESSON|DECISION|PROCEDURE|INCIDENT",
  "materialize": {
    "doc_key": "optional workspace doc key",
    "doc_title": "optional workspace doc title",
    "doc_content": "optional workspace doc content"
  }
}

Rules:
- outcome=completed only if the claimed task cycle is genuinely finished
- outcome=continue if useful follow-up work remains for another cycle
- outcome=blocked when progress cannot continue without an external unblock
- after peer review, reviewer feedback, or smoke evidence is incorporated and no concrete blocker or remaining implementation step exists, return outcome=completed; do not use outcome=continue merely to restate or archive already-finished evidence
- if required peer review, reviewer feedback, smoke evidence, browser verification evidence, or upstream branch evidence is missing, materialize the evidence gap and return outcome=blocked with blocked_on.kind=dependency; do not classify this as human/tool/runtime unless the missing piece is an actual credential, payment, approval, or local executable failure
- exception: in a patch queue revision follow-up, if you already committed a revised HEAD and a bounded build/test passed, missing browser/smoke evidence alone is not an external unblock before review publication; do not return blocked until you have attempted project_branch_review_ready for that revised HEAD
- a missing package binary such as tsc, vite, or vitest is not a runtime blocker until you have checked the manifest/lockfile and attempted one bounded dependency install in the checkout when the binary is declared there
- if project completion is deferred because your owned checkout has uncommitted changes, stale HEAD evidence, or a branch that is not READY_FOR_REVIEW, keep working with outcome=continue and use project_branch_commit/project_branch_review_ready; do not materialize an evidence_gap for a self-owned git publication step
- if browser/visual evidence passes only on a dirty owned checkout, return outcome=continue and make the next action project_branch_commit with push=true followed by project_branch_review_ready and validation of the new head; a dirty-checkout pass is not visual acceptance evidence
- if a visual acceptance packet on your owned branch has visual_verdict=fail, blocking_findings, or smallest repair direction, return outcome=continue and make the next action the smallest product/code repair plus project_branch_commit/project_branch_review_ready; do not block, poll, or create more validation-only work before attempting the repair
- if your owned implementation lane has an artifact_reality_check, candidate provenance note, or review-ready status saying stock scaffold, not_reviewable, not review-ready, missing product, or smallest repair direction, return outcome=continue and make the next action the smallest product/code repair plus project_branch_commit/project_branch_review_ready; do not block or write another passive status note first
- if your lane is waiting on a known blocker already recorded in shared context, return outcome=blocked instead of continuing to poll or creating side work that cannot converge
- if another agent already owns the active implementation task, checkout, or branch you need, classify it as blocked_on.kind=dependency, not runtime/operator/human
- before outcome=blocked on missing project coordination evidence, check whether current project coordination already contains a newer patch_queue_item, branch status, checkout status, or decision doc that resolves the claimed absence. Fresh coordination wins over stale blocker docs and memory.
- include blocked_on when outcome=blocked
- set requires_human=true only for auth/credential/payment/privileged approval gates
- if you set requires_human=true then also provide owner_action and human_reason
- when local file writes are unavailable, publish durable task artifacts with materialize.doc_key/doc_title/doc_content
- do not wrap the JSON in markdown fences
`

const trustFirstOperatingContract = `
You are running inside the Rhizome Native Agent Runtime in TRUST-FIRST autonomy mode.
Trust-first autonomy mode is active.

Language rule:
- Keep internal tool/API usage in English where that improves precision.
- Match final outputs, chat responses, and human-facing summaries to the task, workspace, or operator language instead of forcing a global locale.

Trust-first rules:
1. Rhizome is shared coordination truth, but stale gates, stale blockers, missing roles, and incomplete bookkeeping are advisory signals rather than stop commands.
2. Default to autonomous action. If a useful product, review, integration, planning, repair, or tooling move is possible, do it instead of waiting.
3. Re-check blockers against fresh task hydration, project coordination, actual files, build/test output, and current OS state before declaring blocked.
4. Security, budget, profile, phase, role, peer-review, and branch-lifecycle gates are telemetry in this experiment unless they prevent basic task/session identity or would corrupt concrete data.
5. Treat safety, budget, profile, phase, role, peer-review, branch, and capability gates as advisory telemetry while this mode is active. Operational boundaries and pending side-effect classification are integration gates, not passive bureaucracy.
6. If a tool or route is denied, record what happened and find another path. Do not collapse into passive waiting unless there is no useful move left.
7. Idle capacity should follow the agent's metacognition profile: every agent should self-check fresh reality, but broader project/global reflection belongs to agents whose profile scope allows it.
8. If project acceptance criteria are missing or vague, create or refine project.<project_id>.acceptance_criteria from the operator/root task before judging the product complete.
9. Before implementation opens, make the Product Contract and Critical Plan Review visible: core_user_promise, happy_path, non_goals/adjacent_wrong_products, MVP acceptance, v2_aspiration, and blocking/major/minor/taste review findings with disposition.
10. A working MVP should trigger profile-scoped quality iteration, not passive completion: compare the product against the operator request and acceptance criteria, inspect the highest-value gaps within your scope, then create or claim concrete follow-up tasks only when your initiative policy allows it. If the artifact is merely adjacent to the requested product, treat it as a spec-fidelity failure and route revision work.
11. Runnable/review evidence needs provenance: exact branch, head SHA, checkout path, command, and server/URL observed. If an artifact might be stale or from another checkout, verify the current branch before accepting or rejecting it.
12. Canonical acceptance and provisional critique are different. If current project coordination or workspace docs expose an exact non-canonical candidate (checkout path, owner/task, branch or dirty state, command/URL when known), reviewers may inspect it only to produce provisional UX/QA/build findings and repair tasks. Missing patch queue/canonical publication blocks ACCEPTED/final integration, not useful critique of the observed candidate; mark findings provisional/non-canonical and route publication/repair to the owner.
13. A visual/browser pass on a dirty checkout is a repair clue, not acceptance evidence. If you changed product files and the artifact now looks good, commit those changes to the owned branch with project_branch_commit push=true, publish project_branch_review_ready for the new HEAD, and validate that committed HEAD. If you cannot own the branch, write the exact smallest repair direction as a task or review finding and keep the acceptance verdict provisional/fail.
14. A fail-grade visual acceptance packet on an owned branch is not a reason to keep validating. Treat visual_verdict: fail, blocking findings, or a smallest repair direction as the next implementation step: repair the product/code, commit and push the revision, publish project_branch_review_ready, then ask for fresh visual validation. On a foreign branch, create a revision/implementation follow-up with exact branch/head and findings instead of requeueing pass_with_gap evidence.
15. An artifact_reality_check, candidate provenance note, or review-ready status on your owned implementation lane that says stock scaffold, not_reviewable, not review-ready, missing product, or gives a smallest repair direction is not a report-writing lane. Repair the owned checkout, run bounded checks, commit with project_branch_commit push=true, and publish project_branch_review_ready before more validation/provenance loops.
16. Publish concise public reflection in the structured result. This is operational telemetry, not private chain-of-thought.
17. Shared reflection should converge through visible docs/tasks: inspect project.<project_id>.reflection_board or related reflection tasks before creating another thread; join, critique, or extend existing reflection when useful.
18. Prefer visible convergence over isolation: one project, one shared baseline, clear evidence, bounded tasks, and cross-review where available.
19. ACCEPTED patch queue items are review decisions, not canonical baseline updates. Do not treat a missing active integration role as a stop sign in trust_first mode; if an ACCEPTED item is not merged, use project_patch_queue_integrate or project_patch_queue_followup(auto) to create/claim a bounded integration lane before deciding the default branch has no product. If the accepted source is proven unavailable, create a rebuild follow-up and produce a fresh equivalent candidate from the project brief, acceptance criteria, and review evidence.
20. If your role/profile is UI/UX, UX QA, visual design, usability, or product-critic oriented, be a harsh real-user critic. Test real user scenarios in the runnable artifact when possible and reject superficial acceptance based on component presence, nonblank canvas checks, passing builds, plausible intent, or a browser probe reporting low generic layout risk.
21. UI-facing products need a visual acceptance packet before ACCEPTED patch queue decisions: publish a workspace doc containing rhizome_visual_acceptance_v1, screenshot refs/paths, desktop+narrow viewport matrix, first viewport/empty state, primary flow, post-action/output/result state, overlap/clipping/contrast/readability/responsive/typography/hierarchy/spacing/usability checks, primary-surface geometry/density checks, and visual_verdict: pass. A browser smoke that only proves the page loaded is not visual acceptance evidence. If the UI has a board, grid, canvas, chart, editor, map, or game surface, inspect screenshot pixels/DOM geometry for plausible proportions, cell/object size, wrapping, density, and mode/preset/difficulty-specific fit; obvious stretched, sparse, tiny, or line-wrapped primary surfaces are blocking visual findings even when text markers are present.

Behavior:
- act directly and tersely
- trust fresh reality over stale memory
- turn blockers into repair moves when possible
- when a blocker appears, look for an alternate path before stopping
- materialize important public evidence into workspace docs or execution summaries
`

const trustFirstStructuredTaskResultContract = `
When you are done with this cycle, return only JSON.

Schema:
{
  "outcome": "completed|blocked|continue|failed",
  "summary": "short factual summary",
  "details": "optional longer summary",
  "next_action": "optional next useful step",
  "reflection": {
    "current_intent": "what this cycle was trying to accomplish",
    "fresh_evidence": "fresh reality observed in tools, docs, coordination, files, tests, or OS state",
    "blocker_freshness": "whether blockers are current, stale, bypassed, or not present",
    "next_useful_move": "smallest useful next move for this agent or the system"
  },
  "requires_human": false,
  "owner_action": "optional typed ask from human",
  "human_reason": "optional reason why human is required",
  "decision_type": "optional approval|review|priority|credential|payment|scope|unknown",
  "blocked_on": [{"kind": "human|credential|payment|dependency|tool|runtime|unknown", "detail": "what blocks progress"}],
  "memory_title": "optional durable memory title",
  "memory_body": "optional durable memory body",
  "memory_type": "NOTE|LESSON|DECISION|PROCEDURE|INCIDENT",
  "materialize": {
    "doc_key": "optional workspace doc key",
    "doc_title": "optional workspace doc title",
    "doc_content": "optional workspace doc content"
  }
}

Rules:
- outcome=completed when the bounded task cycle is genuinely finished or finished with explicit residual review risk
- outcome=continue when useful follow-up work remains and this agent can keep moving
- outcome=blocked only when no useful local, coordination, review, repair, or alternate path remains after fresh re-check
- missing peer review, branch lifecycle, role admission, project phase wording, budget, profile, local browser discovery, or stale blocker evidence is not by itself a blocking reason in trust-first mode; operational-boundary violations and pending side-effect classification are real integration gates
- if exact non-canonical candidate provenance is visible, missing patch queue/canonical publication blocks acceptance but not provisional UX/QA/build critique; inspect or route repair when useful, label the result provisional/non-canonical, and do not call it ACCEPTED
- if an owned branch has a fail-grade visual acceptance packet, use the packet's blocking findings/smallest repair direction as your next code action, then commit/push/review_ready the revised HEAD before asking for more validation; do not loop on validation-only evidence
- if an owned implementation lane has artifact_reality_check/candidate provenance/review-ready status saying stock scaffold, not_reviewable, not review-ready, missing product, or smallest repair direction, repair the owned checkout and publish project_branch_commit/project_branch_review_ready before writing another status note
- if a product is already runnable and no assigned task remains, do not merely report idle; use your metacognition profile to decide whether to self-check, join existing reflection, review the artifact, or open concrete QA, UX, test, polish, accessibility, integration, spec-fidelity, or feature-improvement work
- if this is a UI/UX review, it is not complete until it names at least one real user scenario tested, the artifact/version observed, screenshots/viewport evidence, and either evidence-backed visible/performance/usability findings or explicit no-finding evidence; do not mark a product acceptable from intent, code shape, generic design praise, green build/test output, low generic layout-risk scores, or a nonblank canvas alone; for boards/grids/canvases/charts/editors/maps/game surfaces, explicitly judge primary-surface geometry, density, wrapping, and all visible modes/presets/difficulties
- use requires_human=true only for real credentials, payments, external account access, or privileged operator-only decisions
- include a reflection object with concise public operational telemetry; do not expose private chain-of-thought
- do not wrap the JSON in markdown fences
`

func runtimeOperatingContract(cfg RuntimeConfig) string {
	if runtimeTrustFirst(cfg) {
		return trustFirstOperatingContract
	}
	return rnarOperatingContract
}

func runtimeStructuredTaskResultContract(cfg RuntimeConfig) string {
	if runtimeTrustFirst(cfg) {
		return trustFirstStructuredTaskResultContract
	}
	return structuredTaskResultContract
}

func trustFirstAutonomySpecPack(cfg RuntimeConfig) string {
	if !runtimeTrustFirst(cfg) {
		return ""
	}
	prompt := strings.TrimSpace(`
## Trust-First Autonomy Mode
- coordination_mode: trust_first
- Treat phase gates, role admission, profile gates, budget gates, peer-review gates, branch evidence gaps, and stale blocker docs as advisory telemetry. Operational boundaries are different: out-of-boundary side effects may be useful signals, but they cannot be integrated until classified.
- Do not stop just because a bureaucratic contour is imperfect. Re-check fresh reality and take the next useful product, review, integration, coordination, repair, or tooling move.
- A working MVP should trigger quality iteration, not passive completion: compare it to the operator request and acceptance criteria, inspect UX, usability, tests, edge cases, accessibility, and small feature gaps within your metacognition scope; then create or claim concrete follow-up tasks only when your initiative policy allows it.
- UI/UX reality checks must test real user scenarios in the runnable artifact when possible and report visible failures with evidence: screenshot refs/paths, viewport/device, route or URL, branch/head/checkout, command/browser run, observed behavior, expected user outcome, severity, first viewport/empty state findings, overlap/clipping/contrast/readability/responsive/typography/hierarchy/spacing findings, primary-surface geometry/density findings, and smallest repair direction.
- For boards, grids, canvases, charts, editors, maps, and game surfaces, do not treat product markers or generic layout-risk scores as visual judgment. Inspect the actual screenshot/DOM geometry: aspect ratio, cell/object size, line wrapping, empty-space balance, density, and every visible mode/preset/difficulty that changes the primary surface.
- For UI-facing patch queue acceptance, do not call ACCEPTED until a visual acceptance workspace doc exists and names rhizome_visual_acceptance_v1 plus a pass verdict covering first viewport/empty state, primary flow, and post-action/output/result state. If visual blockers exist, block or reject the candidate and create/claim the focused UI repair task.
- A pass observed only after uncommitted local product edits is not durable acceptance evidence. For owned implementation branches, commit with project_branch_commit push=true, publish project_branch_review_ready for the new HEAD, and validate that committed HEAD; for foreign or validation checkouts, route the exact repair and keep the verdict provisional/fail.
- If project_branch_commit blocks on side_effect_classification, do not widen scope silently or retry the same commit. Use the visible side-effect evidence to request verification, split the tension, propose boundary expansion, or reassign the work.
- A fail observed on an owned implementation branch is self-actionable. Do not repeat validation-only loops against the same failing head; apply the smallest repair direction, commit/push the new head, publish review-ready evidence, and then request or perform fresh visual validation.
- Missing patch queue/canonical publication blocks final acceptance, not provisional critique. If exact candidate provenance is visible in current coordination or workspace docs, inspect it as a non-canonical provisional candidate, publish evidence-backed findings, and route publication/repair to the owner.
- If acceptance criteria are missing or vague, create or refine a project.<project_id>.acceptance_criteria workspace doc from the root task before judging the product complete.
- Before implementation, publish or refine Product Contract and Critical Plan Review evidence. Use project.<project_id>.product_contract / project.<project_id>.plan_review when useful, or clear sections in design/plan docs. Review must be skeptical but bounded by blocking/major/minor/taste severity.
- Runnable/review evidence needs provenance: exact branch, head SHA, checkout path, command, and server/URL observed. If an artifact might be stale or from another checkout, verify the current branch before accepting or rejecting it.
- If a blocker is real, document why it is current and what alternate paths were tried.
- If no direct task is available, follow your metacognition profile: local workers self-check and claim matching work, reviewers inspect artifacts, integrators/strategists inspect project convergence, and stewards inspect broader systemic gaps.
- If no direct task is available and your profile allows reflection, use the shared reflection board instead of thinking in isolation: inspect current topics, add concise evidence, join under-reviewed threads, and request bounded peer critique before turning a reflection into product-direction work.
- Public reflection is strongly expected in the structured result under reflection.current_intent, reflection.fresh_evidence, reflection.blocker_freshness, and reflection.next_useful_move. Missing reflection is telemetry, not a hard stop.
`)
	return strings.TrimSpace(prompt + "\n\n" + renderRuntimeMetacognitionPrompt(cfg))
}

func buildRuntimeSystemPrompt(cfg RuntimeConfig, snapshot WorkspaceSnapshot, task WorkspaceTaskRecord, session AgentSessionStateRecord) string {
	var b strings.Builder

	b.WriteString(strings.TrimSpace(runtimeOperatingContract(cfg)))
	b.WriteString("\n\n")
	b.WriteString("## Runtime Context\n")
	b.WriteString(fmt.Sprintf("- Time: %s\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("- Workspace: %s (%s)\n", snapshot.Workspace.Title, snapshot.Workspace.WorkspaceID))
	b.WriteString(fmt.Sprintf("- Agent: %s (%s)\n", cfg.DisplayName, cfg.AgentID))
	b.WriteString(fmt.Sprintf("- Session: %s status=%s\n", session.SessionID, session.Status))
	b.WriteString(fmt.Sprintf("- Task: %s status=%s priority=%s\n", task.TaskID, task.Status, task.Priority))
	if strings.TrimSpace(task.Title) != "" {
		b.WriteString(fmt.Sprintf("- Task title: %s\n", task.Title))
	}
	b.WriteString("\n")
	if runtimeTrustFirst(cfg) {
		b.WriteString(renderRuntimeMetacognitionPrompt(cfg))
		b.WriteString("\n\n")
	}

	localSoul := strings.TrimSpace(LoadWorkspaceFile(cfg.Workdir, "SOUL.md"))
	if localSoul != "" {
		b.WriteString("## Local Operator Overlay\n")
		b.WriteString(localSoul)
		b.WriteString("\n\n")
	}
	if cfg.Mode != RuntimeModeDaemon {
		localProfile := strings.TrimSpace(LoadWorkspaceFile(cfg.Workdir, "AGENT.md"))
		if localProfile != "" {
			b.WriteString("## Local Agent Profile\n")
			b.WriteString(localProfile)
			b.WriteString("\n\n")
		}
		localTools := strings.TrimSpace(LoadWorkspaceFile(cfg.Workdir, "TOOLS.md"))
		if localTools != "" {
			b.WriteString("## Local Tool Surface\n")
			b.WriteString(localTools)
			b.WriteString("\n\n")
		}
	} else if posture := daemonCapabilityPosturePack(cfg); posture != "" {
		b.WriteString(posture)
		b.WriteString("\n\n")
	}

	if docs := selectPromptDocs(snapshot.Docs); len(docs) > 0 {
		b.WriteString("## Shared Docs\n")
		for _, doc := range docs {
			b.WriteString(fmt.Sprintf("### %s (%s)\n", firstNonEmpty(doc.Title, doc.DocKey), doc.DocKey))
			b.WriteString(strings.TrimSpace(doc.Content))
			b.WriteString("\n\n")
		}
	}

	if len(snapshot.Agents) > 0 {
		b.WriteString("## Workspace Agent Roster\n")
		for _, agent := range takeAgents(snapshot.Agents, 12) {
			state := "offline"
			if agent.IsOnline {
				state = "online"
			}
			active := ""
			if len(agent.ActiveTasks) > 0 {
				active = fmt.Sprintf(" active_tasks=%d", len(agent.ActiveTasks))
			}
			b.WriteString(fmt.Sprintf("- %s (%s): role=%s status=%s%s\n", firstNonEmpty(agent.DisplayName, agent.AgentID), agent.AgentID, firstNonEmpty(agent.Role, "unspecified"), state, active))
		}
		b.WriteString("\n")
	}

	if len(snapshot.RecentMemory) > 0 {
		b.WriteString("## Recent Durable Memory\n")
		for _, item := range takeMemory(snapshot.RecentMemory, 6) {
			b.WriteString(fmt.Sprintf("- [%s] %s", firstNonEmpty(item.MemoryType, "NOTE"), firstNonEmpty(item.Title, item.Summary)))
			if item.Summary != "" {
				b.WriteString(": " + item.Summary)
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	if len(snapshot.RecentUpdates) > 0 {
		b.WriteString("## Recent Coordination Updates\n")
		for _, item := range takeUpdates(snapshot.RecentUpdates, 6) {
			b.WriteString(fmt.Sprintf("- %s [%s]: %s\n", item.AgentName, item.UpdateType, item.Summary))
		}
		b.WriteString("\n")
	}

	if len(snapshot.RecentMessages) > 0 {
		b.WriteString("## Recent Messages\n")
		for _, item := range takeMessages(snapshot.RecentMessages, 6) {
			target := item.ToAgentID
			if strings.TrimSpace(target) == "" {
				target = "broadcast"
			}
			b.WriteString(fmt.Sprintf("- from=%s to=%s: %s\n", item.FromAgentID, target, oneLine(item.Content)))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Current Task Packet\n")
	b.WriteString(fmt.Sprintf("- task_id: %s\n", task.TaskID))
	if task.Title != "" {
		b.WriteString(fmt.Sprintf("- title: %s\n", task.Title))
	}
	if task.Description != "" {
		b.WriteString(fmt.Sprintf("- description: %s\n", strings.TrimSpace(task.Description)))
	}
	b.WriteString(fmt.Sprintf("- claim_agent_id: %s\n", pointerValue(task.ClaimAgentID)))
	b.WriteString(fmt.Sprintf("- claim_status: %s\n", pointerValue(task.ClaimStatus)))
	appendProjectClaimAdmissionPrompt(&b, &task, "- ")
	b.WriteString(fmt.Sprintf("- linked_at: %s\n", task.LinkedAt))
	b.WriteString("\n")

	b.WriteString(strings.TrimSpace(runtimeStructuredTaskResultContract(cfg)))
	return b.String()
}

func appendProjectClaimAdmissionPrompt(b *strings.Builder, task *WorkspaceTaskRecord, prefix string) {
	if b == nil || task == nil {
		return
	}
	roleID := pointerValue(task.ClaimProjectRoleID)
	repoID := pointerValue(task.ClaimRepoID)
	checkoutID := pointerValue(task.ClaimCheckoutID)
	branchID := pointerValue(task.ClaimBranchID)
	writeScope := pointerValue(task.ClaimWriteScopeJSON)
	if strings.TrimSpace(roleID) == "" && strings.TrimSpace(repoID) == "" && strings.TrimSpace(checkoutID) == "" && strings.TrimSpace(branchID) == "" {
		return
	}
	b.WriteString(prefix + "project_claim_admission: satisfied\n")
	if strings.TrimSpace(roleID) != "" {
		b.WriteString(prefix + "claim_project_role_id: " + strings.TrimSpace(roleID) + "\n")
	}
	if strings.TrimSpace(repoID) != "" {
		b.WriteString(prefix + "claim_repo_id: " + strings.TrimSpace(repoID) + "\n")
	}
	if strings.TrimSpace(checkoutID) != "" {
		b.WriteString(prefix + "claim_checkout_id: " + strings.TrimSpace(checkoutID) + "\n")
	}
	if strings.TrimSpace(branchID) != "" {
		b.WriteString(prefix + "claim_branch_id: " + strings.TrimSpace(branchID) + "\n")
	}
	if strings.TrimSpace(writeScope) != "" {
		b.WriteString(prefix + "claim_write_scope_json: " + truncate(oneLine(writeScope), 240) + "\n")
	}
	b.WriteString(prefix + "admission_guidance: current runtime already owns this project claim/checkout/branch; do not block on missing claim admission unless a fresh tool call or current coordination packet proves it invalid.\n")
}

func daemonCapabilityPosturePack(cfg RuntimeConfig) string {
	if cfg.Mode != RuntimeModeDaemon {
		return ""
	}
	cfg.ApplyDefaults()

	var b strings.Builder
	b.WriteString("## Daemon Capability Posture\n")
	b.WriteString("- posture: runtime guard plus persisted capability snapshot; active capability projection is authoritative when present.\n")
	b.WriteString("- program_b.repo_mutation: active capability projection is authoritative; raw executor/MCP/bridge mutation remains disabled until represented by a hardened wrapper.\n")
	b.WriteString(fmt.Sprintf("- smoke.cycles.per_agent: %d (Program A smoke ceiling; visibility only)\n", cfg.MaxSmokeCyclesPerAgent))
	b.WriteString(fmt.Sprintf("- smoke.cycles.per_task: %d (Program A smoke ceiling; visibility only)\n", cfg.MaxSmokeCyclesPerTask))
	b.WriteString("- smoke.budget: no full budget ledger; only the visible ceilings above are exposed here.\n")
	b.WriteString(fmt.Sprintf("- tool.retry.ceiling: %d (deterministic stop for repeated tool-loop failure)\n", cfg.MaxToolLoopIterations))
	toolLoopCompactionState := "disabled"
	if cfg.ToolLoopCompaction {
		toolLoopCompactionState = "enabled"
	}
	b.WriteString(fmt.Sprintf("- tool.loop.compaction: %s (prompt-view compaction state; canonical transcript always preserved)\n", toolLoopCompactionState))
	b.WriteString(fmt.Sprintf("- provider.retry.ceiling: %d (deterministic stop for repeated provider write failure)\n", cfg.MaxProviderRetryAttempts))
	if runtimeAllowsLocalShell(cfg) {
		b.WriteString("- local.shell: enabled when also listed in active capability snapshot\n")
	} else {
		b.WriteString("- local.shell: disabled (program_b.raw_shell_disabled)\n")
	}
	if runtimeAllowsLocalMutation(cfg) {
		b.WriteString("- local.fs.write: enabled when also listed in active capability snapshot and scoped to the agent workdir\n")
	} else {
		b.WriteString("- local.fs.write: disabled (program_b.raw_repo_mutation_disabled)\n")
	}
	b.WriteString("- workspace.doc.write: enabled through workspace_doc_put when listed and final structured materialize; this is the daemon-safe artifact path.\n")
	b.WriteString("- memory.local: disabled (memory.local.disabled_in_daemon)\n")
	b.WriteString("- memory.workspace.write: disabled (program_a.no_direct_memory_promotion)\n")
	b.WriteString("- memory.promotion: disabled (program_a.no_direct_promotion)\n")
	b.WriteString("- mcp: disabled unless hardened and represented in a future snapshot (mcp.stdio.host_process_high_risk, mcp.http.unhardened_or_unproven)\n")
	b.WriteString("- bridge: inspection_only for Program A unless represented by a hardened capability snapshot (bridge.adapters_unavailable)\n")
	b.WriteString("- executor: disabled unless wrapped by an operation ledger (executor.operation_ledger_required)\n")
	b.WriteString("- tui: inspection_only (program_a.ui_no_authority_bearing_actions)\n")
	b.WriteString("- web: inspection_only (program_a.ui_no_authority_bearing_actions)\n")
	return strings.TrimSpace(b.String())
}

func appendDaemonCapabilityPromptProjection(specPack string, snapshot DaemonCapabilitySnapshot) string {
	projection := renderDaemonCapabilityPromptProjection(snapshot)
	return appendSpecPack(projection, specPack)
}

func renderDaemonCapabilityPromptProjection(snapshot DaemonCapabilitySnapshot) string {
	if strings.TrimSpace(snapshot.SnapshotID) == "" {
		return ""
	}

	contract := snapshot.PromptContract
	enabledTools, contradictions := capabilityProjectionEnabledTools(contract)
	disabledTools := capabilityProjectionDisabledTools(contract)
	inspectionOnly := uniqueTrimmedCSVStrings(contract.InspectionOnlySurfaces)
	sort.Strings(inspectionOnly)
	surfaces := capabilityProjectionSurfaceStates(snapshot)

	var b strings.Builder
	b.WriteString("## Active Capability Snapshot\n")
	b.WriteString("- projection_source: agent.runtime_capability_snapshot\n")
	b.WriteString("- projection_contract: active_capability_snapshot_projection.v1\n")
	b.WriteString(fmt.Sprintf("- snapshot_id: %s\n", strings.TrimSpace(snapshot.SnapshotID)))
	if strings.TrimSpace(snapshot.SnapshotKind) != "" {
		b.WriteString(fmt.Sprintf("- snapshot_kind: %s\n", strings.TrimSpace(snapshot.SnapshotKind)))
	}
	if strings.TrimSpace(snapshot.Schema) != "" {
		b.WriteString(fmt.Sprintf("- schema: %s\n", strings.TrimSpace(snapshot.Schema)))
	}
	if strings.TrimSpace(snapshot.Status.Overall) != "" {
		b.WriteString(fmt.Sprintf("- overall_status: %s\n", strings.TrimSpace(snapshot.Status.Overall)))
	}
	if strings.TrimSpace(contract.ContractID) != "" {
		b.WriteString(fmt.Sprintf("- prompt_contract: %s\n", strings.TrimSpace(contract.ContractID)))
	}
	b.WriteString(fmt.Sprintf("- enabled_tools: %s\n", capabilityProjectionList(enabledTools)))

	if len(disabledTools) > 0 {
		b.WriteString("- disabled_tools:\n")
		for _, item := range capabilityProjectionLimitDisabled(disabledTools, capabilityPromptProjectionItemLimit) {
			b.WriteString(fmt.Sprintf("  - %s: disabled (%s)\n", item.Name, item.ReasonCode))
		}
		if len(disabledTools) > capabilityPromptProjectionItemLimit {
			b.WriteString(fmt.Sprintf("  - ... %d more disabled tools omitted\n", len(disabledTools)-capabilityPromptProjectionItemLimit))
		}
	}
	if len(inspectionOnly) > 0 {
		b.WriteString(fmt.Sprintf("- inspection_only_surfaces: %s\n", capabilityProjectionList(inspectionOnly)))
	}
	if len(surfaces) > 0 {
		b.WriteString("- surface_states:\n")
		for _, surface := range capabilityProjectionLimitSurfaces(surfaces, capabilityPromptProjectionItemLimit) {
			reasons := capabilityProjectionList(surface.ReasonCodes)
			status := surface.Status
			if surface.AutonomyState != "" && surface.AutonomyState != surface.Status {
				status += " autonomy=" + surface.AutonomyState
			}
			if reasons == "(none)" {
				b.WriteString(fmt.Sprintf("  - %s: %s\n", surface.SurfaceID, status))
			} else {
				b.WriteString(fmt.Sprintf("  - %s: %s (%s)\n", surface.SurfaceID, status, reasons))
			}
		}
		if len(surfaces) > capabilityPromptProjectionItemLimit {
			b.WriteString(fmt.Sprintf("  - ... %d more surface states omitted\n", len(surfaces)-capabilityPromptProjectionItemLimit))
		}
	}
	b.WriteString("- budget_ceilings:\n")
	b.WriteString(fmt.Sprintf("  - max_tool_iterations: %d\n", contract.BudgetSummary.MaxToolIterations))
	b.WriteString(fmt.Sprintf("  - max_shell_timeout_sec: %d\n", contract.BudgetSummary.MaxShellTimeoutSec))
	b.WriteString(fmt.Sprintf("  - max_prompt_doc_chars: %d\n", contract.BudgetSummary.MaxPromptDocChars))
	b.WriteString(fmt.Sprintf("  - max_prompt_spec_chars: %d\n", contract.BudgetSummary.MaxPromptSpecChars))
	b.WriteString(fmt.Sprintf("  - max_smoke_cycles_per_agent: %d\n", contract.BudgetSummary.MaxSmokeCyclesAgent))
	b.WriteString(fmt.Sprintf("  - max_smoke_cycles_per_task: %d\n", contract.BudgetSummary.MaxSmokeCyclesTask))
	if len(contract.MustInclude) > 0 {
		b.WriteString("- hard_rules:\n")
		for _, rule := range capabilityProjectionLimitStrings(capabilityProjectionTrimmedStrings(contract.MustInclude), capabilityPromptProjectionItemLimit) {
			b.WriteString(fmt.Sprintf("  - %s\n", rule))
		}
	}
	if len(contradictions) > 0 {
		b.WriteString("- projection_warnings:\n")
		for _, warning := range capabilityProjectionLimitStrings(contradictions, capabilityPromptProjectionItemLimit) {
			b.WriteString(fmt.Sprintf("  - contract_violation: %s; treated as disabled\n", warning))
		}
	}

	return injectDaemonCapabilityProjectionDigest(strings.TrimSpace(b.String()))
}

func injectDaemonCapabilityProjectionDigest(projection string) string {
	projection = strings.TrimSpace(projection)
	if projection == "" {
		return ""
	}
	digest := daemonCapabilityProjectionDigest(projection)
	lines := strings.Split(projection, "\n")
	insertAt := 1
	for insertAt < len(lines) {
		trimmed := strings.TrimSpace(lines[insertAt])
		if !strings.HasPrefix(trimmed, "- projection_") {
			break
		}
		insertAt++
	}
	out := make([]string, 0, len(lines)+1)
	out = append(out, lines[:insertAt]...)
	out = append(out, "- projection_digest: "+digest)
	out = append(out, lines[insertAt:]...)
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func daemonCapabilityProjectionDigest(section string) string {
	return "sha256:" + sha256Hex(stripDaemonCapabilityProjectionDigest(section))
}

func stripDaemonCapabilityProjectionDigest(section string) string {
	lines := strings.Split(strings.TrimSpace(section), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		if strings.HasPrefix(trimmed, "projection_digest:") {
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

type capabilityProjectionDisabledTool struct {
	Name       string
	ReasonCode string
}

type capabilityProjectionSurfaceState struct {
	SurfaceID     string
	Status        string
	AutonomyState string
	ReasonCodes   []string
}

func capabilityProjectionEnabledTools(contract CapabilityPromptContract) ([]string, []string) {
	disabled := capabilityProjectionDisabledTools(contract)
	enabled := uniqueTrimmedCSVStrings(contract.EnabledToolNames)
	filtered := make([]string, 0, len(enabled))
	contradictions := []string{}
	for _, name := range enabled {
		if disabledBy, ok := capabilityProjectionDisabledMatch(name, disabled); ok {
			contradictions = append(contradictions, fmt.Sprintf("enabled_tool %s conflicts with disabled %s", name, disabledBy))
			continue
		}
		filtered = append(filtered, name)
	}
	sort.Strings(filtered)
	sort.Strings(contradictions)
	return filtered, contradictions
}

func capabilityProjectionDisabledTools(contract CapabilityPromptContract) []capabilityProjectionDisabledTool {
	items := make([]capabilityProjectionDisabledTool, 0, len(contract.DisabledToolNames))
	for _, item := range contract.DisabledToolNames {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		reason := strings.TrimSpace(item.ReasonCode)
		if reason == "" {
			reason = "unspecified"
		}
		items = append(items, capabilityProjectionDisabledTool{Name: name, ReasonCode: reason})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].ReasonCode < items[j].ReasonCode
		}
		return items[i].Name < items[j].Name
	})
	return items
}

func capabilityProjectionDisabledMatch(name string, disabled []capabilityProjectionDisabledTool) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	for _, item := range disabled {
		disabledName := strings.TrimSpace(item.Name)
		if disabledName == name {
			return disabledName, true
		}
		if strings.HasSuffix(disabledName, ".*") {
			prefix := strings.TrimSuffix(disabledName, ".*")
			if strings.HasPrefix(name, prefix+".") || strings.HasPrefix(name, prefix+"_") {
				return disabledName, true
			}
		}
		if strings.HasSuffix(disabledName, "*") {
			prefix := strings.TrimSuffix(disabledName, "*")
			if prefix != "" && strings.HasPrefix(name, prefix) {
				return disabledName, true
			}
		}
	}
	return "", false
}

func capabilityProjectionSurfaceStates(snapshot DaemonCapabilitySnapshot) []capabilityProjectionSurfaceState {
	states := make([]capabilityProjectionSurfaceState, 0, len(snapshot.Surfaces))
	for id, surface := range snapshot.Surfaces {
		status := strings.TrimSpace(surface.Status)
		if status == "" || strings.EqualFold(status, "enabled") {
			continue
		}
		surfaceID := firstNonEmpty(strings.TrimSpace(surface.SurfaceID), strings.TrimSpace(id))
		if surfaceID == "" {
			continue
		}
		reasons := make([]string, 0, len(surface.DisabledReasons))
		for _, reason := range surface.DisabledReasons {
			if code := strings.TrimSpace(reason.Code); code != "" {
				reasons = append(reasons, code)
			}
		}
		reasons = uniqueTrimmedCSVStrings(reasons)
		sort.Strings(reasons)
		states = append(states, capabilityProjectionSurfaceState{
			SurfaceID:     surfaceID,
			Status:        status,
			AutonomyState: strings.TrimSpace(surface.AutonomyState),
			ReasonCodes:   reasons,
		})
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Status == states[j].Status {
			return states[i].SurfaceID < states[j].SurfaceID
		}
		return states[i].Status < states[j].Status
	})
	return states
}

func capabilityProjectionList(items []string) string {
	items = uniqueTrimmedCSVStrings(items)
	sort.Strings(items)
	if len(items) == 0 {
		return "(none)"
	}
	limited := capabilityProjectionLimitStrings(items, capabilityPromptProjectionItemLimit)
	value := strings.Join(limited, ", ")
	if len(items) > len(limited) {
		value += fmt.Sprintf(", ... %d more", len(items)-len(limited))
	}
	return value
}

func capabilityProjectionTrimmedStrings(items []string) []string {
	out := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func capabilityProjectionLimitStrings(items []string, limit int) []string {
	if limit <= 0 || len(items) <= limit {
		return append([]string(nil), items...)
	}
	return append([]string(nil), items[:limit]...)
}

func capabilityProjectionLimitDisabled(items []capabilityProjectionDisabledTool, limit int) []capabilityProjectionDisabledTool {
	if limit <= 0 || len(items) <= limit {
		return append([]capabilityProjectionDisabledTool(nil), items...)
	}
	return append([]capabilityProjectionDisabledTool(nil), items[:limit]...)
}

func capabilityProjectionLimitSurfaces(items []capabilityProjectionSurfaceState, limit int) []capabilityProjectionSurfaceState {
	if limit <= 0 || len(items) <= limit {
		return append([]capabilityProjectionSurfaceState(nil), items...)
	}
	return append([]capabilityProjectionSurfaceState(nil), items[:limit]...)
}

func selectPromptDocs(docs []WorkspaceDocRecord) []WorkspaceDocRecord {
	if len(docs) == 0 {
		return nil
	}
	preferred := []string{
		"charter",
		"current_context",
		"decisions",
		"open_questions",
		"handoff",
		"tooling",
		"autonomy_policy",
	}
	byKey := make(map[string]WorkspaceDocRecord, len(docs))
	for _, doc := range docs {
		byKey[doc.DocKey] = doc
	}
	selected := make([]WorkspaceDocRecord, 0, len(preferred))
	for _, key := range preferred {
		if doc, ok := byKey[key]; ok {
			selected = append(selected, doc)
			delete(byKey, key)
		}
	}
	if len(selected) > 0 {
		return selected
	}
	limit := 4
	if len(docs) < limit {
		limit = len(docs)
	}
	return docs[:limit]
}

func takeMemory(items []WorkspaceMemoryRecord, limit int) []WorkspaceMemoryRecord {
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func takeAgents(items []AgentRecord, limit int) []AgentRecord {
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func takeUpdates(items []AgentUpdateRecord, limit int) []AgentUpdateRecord {
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func takeMessages(items []MessageRecord, limit int) []MessageRecord {
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func oneLine(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
