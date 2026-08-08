# Agent Tool Library

This directory is an operator-side catalog of ready-to-install tool bundles.
It is a convenience collection, not runtime authority. Agents do not discover
or reference this library directly, and they can use bundles copied from this
catalog, downloaded from elsewhere, or written locally in the agent workdir.

Install a tool for one agent by copying the selected bundle directory into that
agent's workdir:

```text
<agent-workdir>/.runtime-config/tool-bundles/<bundle-name>/tool.json
<agent-workdir>/.runtime-config/tool-bundles/<bundle-name>/...
```

At startup the agent runtime scans the agent-local install roots in this order:

- `.runtime-config/tool-bundles/*/tool.json`
- `tools/*/tool.json`

The managed `.runtime-config/tool-bundles` root has precedence over legacy
`tools` bundles. If two manifests resolve to the same tool name, the first
discovered bundle is installed and the later bundle is reported as a collision.
Missing manifests are reported as skipped entries, and malformed manifests are
reported as errors in `ToolBundleDiscoveryReport`.

An agent workdir may also include an optional local registry file:

```text
<agent-workdir>/.runtime-config/tool-bundles.json
```

Example:

```json
{
  "schema_version": "tool_bundle_registry.v1",
  "enabled": ["browser_visual_probe"],
  "disabled": ["legacy_probe"]
}
```

When `enabled` is non-empty, discovery runs in explicit mode and only matching
bundle names are registered. `disabled` always wins. Missing configured bundles,
disabled bundles, and skipped unenabled bundles are surfaced in prompt,
capability snapshot, and `runtime.status` diagnostics.

Managed agents write this registry file during anatomy materialization. The
manager enables the bundle list requested by registry defaults, the agent
record, and anatomy-inferred tool suites, while preserving existing `enabled`
entries for third-party/self-written local bundles and existing `disabled`
entries so local operator vetoes keep winning.

Agents also have a core `tool_bundle_registry` tool. It can list the current
discovery state, register/enable a locally copied bundle, disable a bundle,
scaffold a new self-written bundle under `tools/<name>`, install a bundle from a
directory containing `tool.json`, download a third-party zip bundle from
`source_url`, migrate legacy manifests to the v2 metadata contract, and refresh
runtime discovery. A downloaded zip may contain
`tool.json` at the archive root or inside one bundle directory. If the archive
contains several bundles, pass `name=<bundle>` to select one. The download path
has bounded HTTP timeout and compressed/uncompressed byte limits, rejects unsafe
zip paths, and stores redacted source provenance. Newly refreshed bundle tools
are guaranteed to be available to the next LLM tool loop; a loop that already
started may need to finish and replan before the new tool appears in its prompt
tool definitions.

For self-written tools, `tool_bundle_registry` action `scaffold` writes a
minimal `tool.json` plus a tiny JSON-stdin Node or Python template. Pass
`capability_suites` so heartbeat suite readiness can immediately match the new
bundle after refresh. Custom runtimes are still supported by passing an
explicit `command` array and then editing the generated bundle files directly.

For copied legacy manifests, `tool_bundle_registry` action `migrate` rewrites
agent-local `tool.json` files in place as `tool_bundle.v2` while preserving
unknown third-party fields. Pass `name`/`names` to target selected bundles, or
omit them to migrate all discovered local manifests. Pass `capability_suites`
when the agent needs the migrated bundle to satisfy heartbeat suite readiness.

Copied v1 manifests still load with the original fields:

- `name`
- `description`
- `command`
- `timeout_seconds`
- `parameters`

Manifest v2 metadata is optional and additive. The runtime accepts:

- `schema_version`
- `version`
- `capability_suites`
- `artifact_contracts`
- `healthcheck`
- `dependencies`
- `concurrency`
- `provenance`

The manifest command is executed without a shell. The runtime passes tool call
arguments as JSON on stdin and provides these environment variables:

- `RHIZOME_TOOL_WORKDIR`
- `RHIZOME_TOOL_BUNDLE_DIR`
- `RHIZOME_TOOL_ARTIFACT_DIR`

This keeps Rhizome generic: profiles decide which local tools an agent receives,
while the agent sees only concrete installed tools in its own workdir.

Managers can materialize requested bundles from the operator library when a
matching catalog entry exists. The registry file is still written when the
library is unavailable or a requested bundle is not present, so externally
copied local bundles can satisfy the same configured tool contract. The library
root is resolved from `RHIZOME_OPERATOR_TOOL_LIBRARY_ROOT`, then
`RHIZOME_TOOL_LIBRARY_ROOT`, then executable-adjacent `tool_library` locations,
then the source-tree `agent/tool_library` fallback.
