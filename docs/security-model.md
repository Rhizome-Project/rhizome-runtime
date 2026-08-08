# Security model

Rhizome Runtime is a trusted-operator research runtime. It coordinates local
agents and tools; it is not a hostile multi-tenant isolation boundary.

## Deployment boundary

The quick start binds to `127.0.0.1`. Rhizome does not provide built-in TLS.
Do not expose the service directly to the public Internet. For remote access,
place it behind a TLS-terminating reverse proxy and firewall, and run it under
a dedicated operating-system identity or container with narrowly scoped file
permissions.

Both dashboards display a corresponding-source link. Distributors of modified
builds should set `RHIZOME_SOURCE_URL` to the exact public source tree or
revision served by that build.

The dashboard HTML shell and low-detail `/health` and `/ready` endpoints are
public. Protected data APIs, JSON-RPC methods, and event streams require the
appropriate authentication. Registration and login endpoints are reachable
without a token but are password-gated and rate-limited.

## Credentials

- The first workspace requires an operator-selected
  `RHIZOME_WORKSPACE_PASSWORD`; there is no shared built-in password.
- Workspace and human passwords use PBKDF2-SHA256 with 310,000 iterations.
- Access tokens are stored as hashes in SQLite.
- Agent bootstrap passwords and issued tokens can be stored in local JSON
  profiles with restrictive file permissions. Those files and their backups
  are sensitive.
- Secret command-line flags are intentionally unsupported. Prefer environment
  variables or protected local profiles. Same-user processes may still be able
  to inspect environment variables on some operating systems.
- Authenticated diagnostics read `RHIZOME_TOKEN`; patch-queue operator
  enablement reads the short-lived `RHIZOME_PATCH_QUEUE_CLAIM_TOKEN`.

Use separate evaluation credentials, rotate them after suspected exposure, and
avoid placing secrets in task descriptions, prompts, logs, or repository files.

## Data at rest

SQLite databases, workspace files, agent profiles, logs, and backups are not
encrypted by Rhizome. They can contain prompts, source code, tool results,
coordination history, and identifiers. Use encrypted storage when needed,
restrict filesystem permissions, and test backup/restore procedures without
copying credentials into public artifacts.

## Code execution

Agents can use local shell and filesystem tools, and the optional server
executor can run code. These paths are not a security sandbox. Do not run
untrusted tasks or tool bundles on a privileged host. Restrict the service
account, workspace directory, network access, provider credentials, and
available tools to the smallest practical scope.

Model providers receive prompt and tool context according to the selected
provider and its policies. Review data-handling requirements before using real
project material.

## Reporting vulnerabilities

Follow the private reporting instructions in [SECURITY.md](../SECURITY.md).
Do not include credentials or exploitable details in a public issue.
