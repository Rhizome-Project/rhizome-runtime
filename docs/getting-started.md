# Getting started

This guide runs one local Rhizome control plane, registers one native agent,
and submits a durable task. Use a disposable directory and credentials while
evaluating the research preview.

## Prerequisites

- Git
- Go 1.26.1 or newer
- Python 3 only if you enable the optional DAG executor bridge
- no model credential for the deterministic first-run walkthrough
- an OpenAI-compatible API key, or a locally configured supported CLI backend,
  only for the optional real-provider walkthrough

SQLite is embedded through the Go driver; no separate database service is
required. The root and `agent/` directories are separate Go modules and are
built independently.

## Build

### Bash

```bash
git clone https://github.com/Rhizome-Project/rhizome-runtime.git
cd rhizome-runtime
mkdir -p bin data
go build -o bin/rhizome ./cmd/rhizome
(cd agent && go build -o ../bin/rhizome-bot .)
```

### PowerShell

```powershell
git clone https://github.com/Rhizome-Project/rhizome-runtime.git
Set-Location rhizome-runtime
New-Item -ItemType Directory -Force bin, data | Out-Null
go build -o bin/rhizome.exe ./cmd/rhizome
Push-Location agent
go build -o ../bin/rhizome-bot.exe .
Pop-Location
```

## Start the control plane

Choose a long random workspace password and keep it private. It is required
only when the initial workspace is created, but agents need it for their first
registration.

### Bash

```bash
export RHIZOME_DB="$PWD/data/rhizome.db"
export RHIZOME_WORKSPACE_ROOT="$PWD/data/workspace"
export RHIZOME_WORKSPACE_ID="rhizome-main"
export RHIZOME_WORKSPACE_PASSWORD="replace-with-a-long-random-value"
./bin/rhizome serve --addr 127.0.0.1:8420
```

### PowerShell

```powershell
$env:RHIZOME_DB = "$PWD/data/rhizome.db"
$env:RHIZOME_WORKSPACE_ROOT = "$PWD/data/workspace"
$env:RHIZOME_WORKSPACE_ID = "rhizome-main"
$env:RHIZOME_WORKSPACE_PASSWORD = "replace-with-a-long-random-value"
./bin/rhizome.exe serve --addr 127.0.0.1:8420
```

Check <http://127.0.0.1:8420/health> and open
<http://127.0.0.1:8420/dashboard>. The `/ready` endpoint is a stronger runtime
gate and can be temporarily unavailable while maintenance loops initialize.

## Start an agent

Open a second terminal in the repository and set the same workspace password.
The first walkthrough uses the deterministic `fake` backend, so it sends no data
to a model provider and needs no provider credential.

### Bash

```bash
export RHIZOME_WORKSPACE_PASSWORD="the-same-value"

./bin/rhizome-bot daemon \
  --workdir "$PWD/data/agents/researcher" \
  --rhizome-host http://127.0.0.1:8420 \
  --workspace-id rhizome-main \
  --agent-id researcher \
  --display-name "Researcher" \
  --owner-user-id local-user \
  --llm-backend fake
```

### PowerShell

```powershell
$env:RHIZOME_WORKSPACE_PASSWORD = "the-same-value"

./bin/rhizome-bot.exe daemon `
  --workdir "$PWD/data/agents/researcher" `
  --rhizome-host http://127.0.0.1:8420 `
  --workspace-id rhizome-main `
  --agent-id researcher `
  --display-name "Researcher" `
  --owner-user-id local-user `
  --llm-backend fake
```

After successful registration, the agent stores its access token in a local
profile with restrictive file permissions. Treat `data/agents/` and your user
configuration directory as sensitive.

The fake backend is for deterministic evaluation, not model-quality testing. It
still exercises the normal agent registration, polling, task ownership, and
durable state paths; its scripted result is not a substitute for
the requested research output.

## Submit and inspect a task

Open a third terminal, keep `RHIZOME_DB` pointed at the same database, and run:

```bash
./bin/rhizome task submit \
  --task-id demo-001 \
  --owner-user-id local-user \
  --workspace-id rhizome-main \
  --title "Create a workspace research note" \
  --description "Summarize the current workspace state and publish the result as a shared document." \
  --kind EXECUTION

./bin/rhizome task status --task-id demo-001
```

PowerShell uses the same arguments with `./bin/rhizome.exe` and backticks for
line continuation.

## Use a real provider with explicit bounds

Stop the fake agent before reusing its identity. The following alternative uses
the OpenAI API directly with an isolated agent identity and explicit limits on
tool iterations, retries, each provider call, the planner cycle, total provider
calls, and total runtime. Credential flags are intentionally unsupported so
secrets do not enter shell history or process listings.

This direct API-key route uses `trust_first` coordination. That mode relaxes
strict project-role admission, so use it only in a disposable, trusted-operator
evaluation. Strict daemon operation with a real provider requires an onboarded
provider record and the `--real-llm-pilot` bounded profile.

### Bash

```bash
export RHIZOME_WORKSPACE_PASSWORD="the-same-value"
export OPENAI_API_KEY="your-api-key"

./bin/rhizome-bot daemon \
  --workdir "$PWD/data/agents/researcher-openai" \
  --rhizome-host http://127.0.0.1:8420 \
  --workspace-id rhizome-main \
  --agent-id researcher-openai \
  --display-name "OpenAI Researcher" \
  --owner-user-id local-user \
  --coordination-mode trust_first \
  --real-llm-pilot \
  --provider-id openai-direct \
  --group-id evaluation \
  --llm-backend openai \
  --model gpt-5.4-mini \
  --max-tool-loop-iterations 10 \
  --max-provider-retry-attempts 1 \
  --provider-call-timeout-sec 120 \
  --planner-cycle-timeout-sec 180 \
  --soak-stop-file "$PWD/data/STOP_OPENAI" \
  --soak-runtime-limit-sec 1800 \
  --soak-max-provider-calls 20
```

### PowerShell

```powershell
$env:RHIZOME_WORKSPACE_PASSWORD = "the-same-value"
$env:OPENAI_API_KEY = "your-api-key"

./bin/rhizome-bot.exe daemon `
  --workdir "$PWD/data/agents/researcher-openai" `
  --rhizome-host http://127.0.0.1:8420 `
  --workspace-id rhizome-main `
  --agent-id researcher-openai `
  --display-name "OpenAI Researcher" `
  --owner-user-id local-user `
  --coordination-mode trust_first `
  --real-llm-pilot `
  --provider-id openai-direct `
  --group-id evaluation `
  --llm-backend openai `
  --model gpt-5.4-mini `
  --max-tool-loop-iterations 10 `
  --max-provider-retry-attempts 1 `
  --provider-call-timeout-sec 120 `
  --planner-cycle-timeout-sec 180 `
  --soak-stop-file "$PWD/data/STOP_OPENAI" `
  --soak-runtime-limit-sec 1800 `
  --soak-max-provider-calls 20
```

These runtime limits do not replace provider-side billing limits. Use a separate
evaluation credential and configure spending controls with the provider as well.
Create the stop file named above to request an earlier graceful stop.

## Stop and clean up

Stop the agent and server with `Ctrl+C`. For a disposable evaluation, delete
the local `data/` directory after both processes exit. This removes the local
database, workspace files, and agent profiles created beneath that directory;
it does not remove credentials stored elsewhere in your user configuration.

## Troubleshooting

- **`RHIZOME_WORKSPACE_PASSWORD is required`** — set it before the first server
  startup or before creating a workspace.
- **`invalid workspace password`** — the agent must use the value chosen when
  the workspace was first created.
- **provider credential missing** — set the provider-specific environment
  variable, such as `OPENAI_API_KEY` or `OPENROUTER_API_KEY`.
- **real-capable provider requires `--real-llm-pilot`** — use the documented
  bounded pilot profile for strict mode, or the disclosed `trust_first` example
  above for a disposable direct API-key evaluation.
- **port 8420 is already in use** — choose another loopback address with
  `--addr 127.0.0.1:PORT` and pass the same URL to the agent.
- **`/health` works but `/ready` does not** — inspect server logs; readiness also
  covers background-loop state.
- **Python error** — Python is needed only for the optional DAG executor path,
  not for the native agent walkthrough above.
