# Batch Model CI/CD Migration Playbook

> **Purpose**: This document is the single source of truth for migrating batch model repositories from the old CI/CD method to the new CI/CD method. It is designed to be consumed by Claude Code agents operating on multiple repos in parallel.

-----

## 1. Overview

We are migrating **[NUMBER]** batch model repositories from the legacy CI/CD approach to the new method. All models deploy to the internal model platform.

### Repos to Migrate

|Repo           |Status                 |Notes                   |
|---------------|-----------------------|------------------------|
|`[repo-1-name]`|✅ Done (reference repo)|Use as the gold standard|
|`[repo-2-name]`|⬜ Not started          |                        |
|`[repo-3-name]`|⬜ Not started          |                        |
|`[repo-4-name]`|⬜ Not started          |                        |

### Reference Materials

- **Successfully migrated repo**: `[path or repo URL]`
  - **Pre-migration branch**: `[branch name]` — the state before migration
  - **Post-migration branch**: `[branch name]` — the fully migrated state
- **Platform-provided template repo**: `[path or repo URL]`

> The reference repo’s two branches are your most valuable resource. Diffing them shows exactly what changed during a successful migration. Agents should run `git diff [pre-branch]..[post-branch] --stat` and inspect specific files to understand the transformation.

-----

## 2. Old vs New Structure

> Before starting migration on a repo, use the reference repo and template repo to understand the target state. Compare the current repo against both to identify what needs to change.

### Key Differences

|Aspect          |Old Method|New Method|
|----------------|----------|----------|
|Build definition|[TODO]    |[TODO]    |
|Container setup |[TODO]    |[TODO]    |
|Dependencies    |[TODO]    |[TODO]    |
|Model execution |[TODO]    |[TODO]    |
|CI/CD pipeline  |[TODO]    |Jenkins   |

### Files to Add

[TODO: list new files required by the new method, or reference the template repo]

### Files to Remove

[TODO: list files from the old method that are no longer needed]

### Files to Modify

[TODO: list files that exist in both old and new but need changes]

### Files to Keep As-Is

[TODO: list files that carry over unchanged]

-----

## 3. Migration Steps

Execute these steps in order. Each step has verification before moving on.

### Step 1: Inventory and Baseline

**Goal**: Understand the current repo state before making changes.

**Actions**:

1. List all files in the repo
1. Diff the reference repo’s pre- and post-migration branches to understand what a successful migration looks like (`git diff [pre-branch]..[post-branch]`)
1. Compare the current repo against the reference repo’s pre-migration branch to understand similarities and differences
1. Compare against the template repo to identify any additional patterns
1. Identify any repo-specific customizations (non-standard scripts, extra configs, unusual dependencies)
1. Run any existing tests or validation to confirm the repo works before migration
1. Record the inventory in the findings report

**Verification**:

```bash
# TODO: command to validate current repo builds/works under old method, if applicable
```

**Judgment calls**: If the repo has files or patterns not covered by this playbook, flag them in the findings report under “Blockers / Questions” — do not guess.

-----

### Step 2: Create New CI/CD Files

**Goal**: Add the new files required by the new CI/CD method.

**Actions**:

1. Create each new file based on the template repo (see Section 5 for templates)
1. Customize for this specific model:
- [TODO: describe what fields/values need to be repo-specific, e.g. model name, paths, dependencies]
1. Cross-reference the reference repo to confirm structure and content

**Verification**:

```bash
# TODO: commands to validate new files are syntactically correct
```

-----

### Step 3: Migrate Environment / Dependencies

**Goal**: Ensure the model’s dependencies are correctly captured in the new format.

**Actions**:

1. [TODO: describe how old dependency definitions map to the new method]
1. [TODO: any dependency pinning rules or version constraints]

**Verification**:

```bash
# TODO: command to verify dependencies resolve correctly
```

-----

### Step 4: Migrate Setup and Run Logic

**Goal**: Move any logic from old shell scripts into the appropriate new locations.

**Actions**:

1. Identify all shell scripts in the repo and understand what each one does
1. For each script, determine where its logic should live in the new method (refer to the reference repo for examples)
1. Migrate the logic accordingly

**Judgment calls**:

- If a shell script contains logic beyond standard setup (e.g. custom data downloads, environment variable hacks, workarounds), flag it in the findings report
- [TODO: add specific judgment call guidance as you discover patterns]

**Verification**:

```bash
# TODO: commands to verify the model can be invoked through the new method
```

-----

### Step 5: Update / Remove Old Files

**Goal**: Clean up files that are no longer needed.

**Actions**:

1. Remove old files that have been fully replaced by the new method
1. Update any files that are shared between old and new
1. Update any references in README or other docs
1. Confirm no remaining code references removed files

**Verification**:

```bash
# Search for references to removed files
# TODO: customize this grep for actual filenames once known
grep -r "REMOVED_FILE_PATTERN" . --include="*.py" --include="*.yml" --include="*.yaml" --include="*.md" --include="*.sh"
```

-----

### Step 6: Run Jenkins Pipeline Stages Locally

**Goal**: Simulate what Jenkins will do and confirm each stage passes.

**Jenkins stages and commands**:

```bash
# TODO: PASTE JENKINS STAGES AND COMMANDS HERE
#
# Stage 1: [name]
# [command]
#
# Stage 2: [name]
# [command]
#
# Stage 3: [name]
# [command]
```

**Verification**: All stages above should exit 0 with no errors.

-----

### Step 7: Final Validation

**Goal**: Confirm the fully migrated repo matches the expected new structure.

**Actions**:

1. Compare the repo’s file tree against the reference migrated repo
1. Diff key config files against the reference to catch any missed fields
1. Run the full verification suite one more time

**Verification**:

```bash
# TODO: final validation commands
```

-----

## 4. Known Gotchas and Fixes

> Fill this section in as you discover issues. These findings propagate to all agents in subsequent rounds.

### Gotcha 1: [Title]

- **Symptom**: [what you see]
- **Cause**: [why it happens]
- **Fix**: [what to do]
- **Applies to**: [all repos / repos with specific characteristics]

### Gotcha 2: [Title]

- **Symptom**:
- **Cause**:
- **Fix**:
- **Applies to**:

<!-- Add more as discovered -->

-----

## 5. Templates and Reference Files

> Paste templates here, or point to the template repo so the agent can read them directly.

### [New File 1] Template

```
# TODO: paste template or say "See template repo at [path]"
```

### [New File 2] Template

```
# TODO: paste template or say "See template repo at [path]"
```

-----

## 6. Decision Log

> When a judgment call is made during migration, document it here so all agents follow the same decision going forward.

|Date|Repo|Decision|Rationale|
|----|----|--------|---------|
|    |    |        |         |

-----

## 7. Agent Instructions

### Per-Round Workflow

1. Read this entire playbook before starting
1. Read the consolidated findings file (`findings/consolidated.md`) if it exists
1. Compare the current repo against both the reference repo and template repo to understand the full picture
1. Execute the migration steps in order (Section 3)
1. Stop and report (do not guess) when you encounter:
- Files or patterns not covered by this playbook
- Judgment calls not addressed in Section 4 or 6
- Verification commands that fail
1. Write your findings report to `findings/[repo-name].md` using the template below

### Findings Report Template

```markdown
## Repo: [name]
### Round: [number]

### Changes Made
- [ ] Step 1: Inventory — [done/skipped/partial]
- [ ] Step 2: New CI/CD files — [done/skipped/partial]
- [ ] Step 3: Dependencies — [done/skipped/partial]
- [ ] Step 4: Setup/run logic — [done/skipped/partial]
- [ ] Step 5: Cleanup — [done/skipped/partial]
- [ ] Step 6: Jenkins stages — [done/skipped/partial]
- [ ] Step 7: Final validation — [done/skipped/partial]

### Verification Results
[paste command outputs or summarize pass/fail]

### Judgment Calls Made
- [situation]: [what I chose and why]

### Blockers / Questions
- [anything that couldn't be resolved]

### Suggested Playbook Updates
- [patterns discovered that should be added to Section 4 or 6]
```

### Rules

- **Do not modify model source code** unless explicitly required by a migration step
- **Do not delete files** you’re unsure about — flag them instead
- **Always run verification** after each step before proceeding to the next
- **Prefer the reference repo’s approach** over the template repo when they differ, unless the playbook says otherwise
- **When in doubt, stop and ask** — document in Blockers rather than guessing

-----

## 8. Orchestration (for the human)

### Running a Round

```bash
#!/bin/bash
# run-round.sh — launch Claude Code agents in parallel

REPOS=("[repo-2-name]" "[repo-3-name]" "[repo-4-name]")
PLAYBOOK_PATH="/path/to/batch-model-migration-playbook.md"
FINDINGS_PATH="/path/to/findings/consolidated.md"
ROUND_NUMBER="1"  # increment each round

for repo in "${REPOS[@]}"; do
  cd "/path/to/$repo"
  claude --print \
    --system-prompt "$(cat $PLAYBOOK_PATH)" \
    "Execute migration round $ROUND_NUMBER for this repo per the playbook.
     Consolidated findings from prior rounds: $(cat $FINDINGS_PATH 2>/dev/null || echo 'None yet — this is round 1.')
     Write your findings report to /path/to/findings/${repo}.md" \
    &
done
wait
echo "Round $ROUND_NUMBER complete. Review findings/ directory."
```

### Between Rounds

1. Review each `findings/[repo].md`
1. Resolve any blockers or judgment calls
1. Update Sections 4 (Gotchas) and 6 (Decision Log) in this playbook
1. Merge key findings into `findings/consolidated.md`
1. Increment round number and re-run