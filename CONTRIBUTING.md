# Contributing

Rhizome Runtime is an early research preview. Focused bug reports, reproducible
experiments, documentation improvements, and small, well-tested changes are
welcome.

## Before opening a change

1. Search existing issues and pull requests.
2. Open an issue before a large behavioral, schema, security, or architecture
   change so the direction can be discussed.
3. Keep private infrastructure, credentials, production data, and generated
   runtime state out of issues and commits.

## Development setup

The repository contains two Go modules plus optional Python executor code.

```bash
go test ./cmd/... ./internal/...
(cd agent && go test ./...)
go build -o /tmp/rhizome ./cmd/rhizome
(cd agent && go build -o /tmp/rhizome-bot .)
python -m unittest discover -s internal -t . -p 'test_*.py'
python scripts/check_repository_hygiene.py
python scripts/check_markdown_links.py
go run github.com/zricethezav/gitleaks/v8@v8.29.1 git . --no-banner --redact
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
```

The secret scan requires full Git history. Run `gofmt` on changed Go files. Add
focused tests for behavior changes and keep the documented quick start accurate.

## Pull requests

- explain the problem and the chosen boundary;
- link the issue when one exists;
- include verification commands and results;
- call out schema, security, compatibility, and operator-impact changes;
- keep unrelated refactors out of the same pull request.

## Developer Certificate of Origin

Contributions use an inbound-equals-outbound model: submitted project code is
licensed under AGPL-3.0-only. Every commit must be signed off under the
[Developer Certificate of Origin 1.1](https://developercertificate.org/):

```text
Signed-off-by: Your Name <you@example.com>
```

Use `git commit -s` to add the line. The sign-off certifies that you have the
right to submit the contribution; it is not a transfer of copyright.

Third-party code or assets must include their exact source, version, license,
and required notices. Do not add generated bundles without a reproducible
provenance record.

## Conduct and security

Participation is governed by [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Report
vulnerabilities through [SECURITY.md](SECURITY.md), not a public issue.
