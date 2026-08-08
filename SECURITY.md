# Security policy

## Supported versions

Rhizome Runtime is currently an unversioned research preview. Security fixes
are applied only to the latest commit on `main` until the first tagged preview
release. There is no production support or long-term-support branch.

## Private reporting

Do not open a public issue for a vulnerability that could expose credentials,
data, or a working exploit.

Use the repository's **Security** tab to submit a private vulnerability report
or draft advisory. If private reporting is unavailable, contact the
Rhizome-Project organization owners through GitHub and request a private channel
before sending technical details.

Include:

- affected commit and operating system;
- a minimal reproduction without real credentials or private data;
- impact and required preconditions;
- suggested mitigation, if known.

Maintainers will acknowledge a complete report when it is reviewed, coordinate
remediation privately, and publish credit and disclosure timing with the
reporter when appropriate. Response times are best-effort during the research
preview.

## Scope reminders

Rhizome executes trusted local tools and optional executor code. Lack of a
hostile-code sandbox, built-in TLS, or encrypted-at-rest storage is a documented
architecture boundary rather than a vulnerability by itself. Unexpected
bypasses, credential disclosure, cross-workspace access, stale-writer authority
failures, and unsafe default exposure are in scope.
