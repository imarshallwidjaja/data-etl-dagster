## `.cursor/rules/` — Agent Rules (project-specific examples)

This folder contains **example “rule sets”** that configure how an AI coding agent should behave when working in this repo.
They are written for **one specific agentic setup** (Cursor rules using `.mdc` frontmatter) and **one specific dev environment** (Windows + PowerShell + conda).

If you copy these rules into another agent system, you should treat them as **templates**:

- **Adapt to your agent framework**: some tools expect `.mdc`, others use `.md`, `.yaml`, or a UI-based rules editor.
- **Adapt to your environment**: shell, OS, package manager, and workflow constraints vary.
- **Keep the intent**: these rules encode safety and architectural invariants; don’t drop them accidentally.

---

### How Cursor-style `.mdc` rules work (at a glance)

Each file uses a small YAML frontmatter block:

- **`description`**: what the rule file is for
- **`alwaysApply: true`**: means “apply this rule set to every run”

Everything after the frontmatter is natural-language instruction text.

> If your environment does not support `.mdc`, convert these rules into whatever format your agent expects.
> The most important part is the **instruction content**, not the extension.

---

### Rule sets in this folder

#### `general.mdc.example`

**Purpose**: Defines the agent’s identity, decision-making standards, and the project’s non-negotiable architecture.

**Key behaviors encoded**:

- **Docs First**
  - Always read repo-root `AGENTS.md` first.
  - Before editing, read the nearest local `AGENTS.md` in the directory hierarchy.
  - If patterns/architecture change, update the relevant `AGENTS.md`.
- **Anti-loop bugfix discipline**
  - One analyze → fix → verify attempt.
  - If verification fails, stop; revert; write a short post-mortem; ask for guidance.
- **Immutable architectural laws**
  - PostGIS is transient compute only.
  - MongoDB is the Source of Truth.
  - GDAL/heavy libs live only in `user-code` container.
  - Landing-zone → process → data-lake ingestion contract.
- **Technical standards**
  - Python 3.10+, strict typing, Pydantic v2 patterns, unit vs integration test expectations.
  - Dagster registration rule: register assets/jobs/sensors in `services/dagster/etl_pipelines/definitions.py`.

#### `mcp.mdc.example`

**Purpose**: Forces consistent use of the project’s MCP-based code-intelligence tools.

**Key behaviors encoded**:

- Use **`augment-context-engine`** first for understanding the codebase / semantic code search.
- Use exact-text search tools only for literal strings (errors, config values, log lines).
- Use **Serena tools** for code exploration/edits where possible.
- Ensure the Serena project is activated when needed.

#### `os-venv.mdc.example`

**Purpose**: Prevents “wrong-shell / wrong-env” command execution issues.

**Key behaviors encoded**:

- Commands are run in **PowerShell on Windows**.
- Use **conda** for environment/dependencies.
- Always activate **`data-etl-dagster`** conda env before running commands.
- If install fails: **stop and ask the user** (don’t continue blindly).

---

### Using these rules in your own environment

If you want to reuse these rules:

- **Rename/convert** the files to the format your agent platform supports.
- **Replace environment specifics** (PowerShell/conda/env name) with your own.
- **Keep the architectural laws intact** if you’re working on this repo.

If you’re maintaining this repo, keep rule sets short, explicit, and non-duplicative. When in doubt, prefer linking to `AGENTS.md` over copying large blocks of documentation.
