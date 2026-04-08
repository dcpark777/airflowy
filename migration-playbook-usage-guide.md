# Migration Playbook — Detailed Usage Guide

This walks through exactly how to use `batch-model-migration-playbook.md` with Claude Code, with concrete examples at each step.

-----

## Phase 1: Fill In the Playbook

Open a Claude Code session in the directory where your playbook lives.

### 1a. Fill in repo names and branches

Edit Section 1 of the playbook with your actual values:

```markdown
| Repo | Status | Notes |
|------|--------|-------|
| `fraud-model-a` | ✅ Done (reference repo) | Use as the gold standard |
| `fraud-model-b` | ⬜ Not started | |
| `fraud-model-c` | ⬜ Not started | |

### Reference Materials

- **Successfully migrated repo**: `/Users/dan/repos/fraud-model-a`
  - **Pre-migration branch**: `main` — the state before migration
  - **Post-migration branch**: `new-cicd` — the fully migrated state
- **Platform-provided template repo**: `/Users/dan/repos/emp-batch-template`
```

### 1b. Use Claude Code to analyze the reference repo diff

Open Claude Code and ask it to study the migration:

```
You: Here's my migration playbook: [paste or point to file]

I have a reference repo at /Users/dan/repos/fraud-model-a.
The `main` branch is the pre-migration state.
The `new-cicd` branch is the post-migration state.

Please diff the two branches and help me fill in Section 2 of the playbook
(old vs new structure, files to add/remove/modify/keep).
```

Claude Code will run something like:

```bash
cd /Users/dan/repos/fraud-model-a
git diff main..new-cicd --stat
git diff main..new-cicd -- .   # full diff
```

And then help you populate the playbook. For example, Section 2 might end up looking like:

```markdown
### Files to Add
- `Dockerfile` — container definition (see template)
- `build-config.yml` — build pipeline configuration
- `Jenkinsfile` — pipeline definition

### Files to Remove
- `scripts/setup.sh` — logic moved into Dockerfile
- `scripts/run.sh` — logic moved into entrypoint in Dockerfile
- `legacy-deploy.yml` — old deployment config

### Files to Modify
- `environment.yml` — remove conda-specific fields, align with Dockerfile pip install
- `config/model-config.yml` — add new required fields: `platform_version`, `runtime`

### Files to Keep As-Is
- `src/**` — all model source code unchanged
- `tests/**` — all tests unchanged
- `config/features.yml` — feature definitions unchanged
```

### 1c. Paste Jenkins stages

You said you know these. Paste them into Step 6:

```markdown
### Step 6: Run Jenkins Pipeline Stages Locally

**Jenkins stages and commands**:

Stage 1: Lint config files
  yamllint *.yml config/*.yml

Stage 2: Build container
  docker build -t model-image .

Stage 3: Run unit tests
  docker run model-image pytest tests/

Stage 4: Validate model config
  docker run model-image python -m model_validator config/model-config.yml

Stage 5: Package
  docker tag model-image registry.internal/models/MODEL_NAME:latest
```

*(These are made-up examples — replace with your actual stages.)*

### 1d. Add your gotchas

From your first migration, add what you learned:

```markdown
### Gotcha 1: Python version mismatch
- **Symptom**: `docker build` fails on pip install with syntax errors
- **Cause**: Old environment.yml used Python 3.8, new base image uses 3.11
- **Fix**: Pin Python version in Dockerfile: `FROM python:3.8-slim`
- **Applies to**: Any repo using Python < 3.10

### Gotcha 2: Hardcoded paths in shell scripts
- **Symptom**: Model fails at runtime with FileNotFoundError
- **Cause**: Old run.sh referenced `/opt/conda/envs/model/...`, which doesn't exist in the new container
- **Fix**: Use relative paths or environment variables. Check for hardcoded `/opt/conda` in all .sh and .py files
- **Applies to**: All repos
```

-----

## Phase 2: Pilot Run (One Repo, Interactive)

Pick `fraud-model-b`. Open Claude Code in that repo’s directory.

### Starting the session

```
You: I'm migrating this repo from old CI/CD to new CI/CD.

Here is the migration playbook: /path/to/batch-model-migration-playbook.md
Here is the reference repo: /Users/dan/repos/fraud-model-a
  - Pre-migration branch: main
  - Post-migration branch: new-cicd
Here is the template repo: /Users/dan/repos/emp-batch-template

Please start with Step 1 (Inventory and Baseline). List all files in this repo,
diff the reference repo branches, and compare this repo against the reference's
pre-migration state. Identify what's similar and what's different.
Report your findings before making any changes.
```

### What Claude Code does

It will:

1. Run `find . -type f` or `tree` on the current repo
1. Check out the reference repo branches and diff them
1. Compare the current repo’s structure against the reference’s pre-migration branch
1. Report something like:

```
## Inventory for fraud-model-b

This repo's structure closely matches the reference repo's pre-migration state with
these differences:

- Has an extra script: `scripts/download_data.sh` (not in reference repo)
- Missing `config/features.yml` — feature config appears to be inline in `src/model.py`
- Uses `requirements.txt` instead of `environment.yml` for dependencies

### Blockers / Questions
- `scripts/download_data.sh` is not covered by the playbook. It downloads training
  data from S3 before model execution. Where should this logic live in the new method?
```

### How you respond

```
You: Good catch on download_data.sh. In the new method, data downloading is handled
by the platform before the container starts, so that script can be removed entirely.
Add this to the playbook as a gotcha:
"If a repo has a data download script, it can be removed — the platform handles
data staging in the new method."

Now proceed to Step 2.
```

### Iterating through steps

Continue step by step. After each step, Claude Code runs verification:

```
You: Step 2 looks good. Run the verification commands.

Claude: Running yamllint on config files...
  config/model-config.yml:12:3 error: wrong indentation: expected 4 but found 2
  
  There's a YAML indentation issue in the model config. The new field I added
  (`platform_version`) is indented incorrectly. Fixing now...
  [fixes file]
  Re-running yamllint... all clean.

  Step 2 verification passed. Ready for Step 3?
```

### Updating the playbook as you go

When you discover something new, tell Claude Code to update the playbook:

```
You: Add that YAML indentation issue to Section 4 as a gotcha. Also add to the
decision log: "When model-config.yml has 2-space indentation but the new fields
expect 4-space, convert the entire file to 4-space for consistency."
```

### Finishing the pilot

After Step 7, you should have:

- A fully migrated `fraud-model-b`
- An updated playbook with new gotchas and decisions from the pilot
- Confidence that the steps work

Write the findings report:

```
You: Write the findings report for this repo to /path/to/findings/fraud-model-b.md
```

-----

## Phase 3: Parallel Runs

### Setup

Create the orchestration directory:

```bash
mkdir -p migration-orchestrator/findings
cp batch-model-migration-playbook.md migration-orchestrator/
touch migration-orchestrator/findings/consolidated.md
```

Consolidate findings from the pilot into `consolidated.md`:

```markdown
# Consolidated Findings

## From fraud-model-b (Round 1 — Pilot)

### Key discoveries
- Data download scripts can be removed — platform handles data staging
- YAML files should be normalized to 4-space indentation before adding new fields
- repos using requirements.txt instead of environment.yml need different
  dependency migration path (see Gotcha 3 in playbook)

### Judgment calls made
- Chose to normalize all YAML to 4-space indentation for consistency
- Removed download_data.sh entirely rather than migrating it
```

### Option A: CLAUDE.md approach (recommended for early rounds)

Instead of the bash script, place the playbook as `CLAUDE.md` in each repo. Claude Code automatically reads `CLAUDE.md` when it starts a session.

```bash
# Copy playbook into each repo as CLAUDE.md
for repo in fraud-model-c fraud-model-d; do
  cp migration-orchestrator/batch-model-migration-playbook.md \
     /Users/dan/repos/$repo/CLAUDE.md
done
```

Then open a terminal tab per repo and start Claude Code:

```bash
# Tab 1
cd /Users/dan/repos/fraud-model-c
claude
# Then type: Execute the migration per the playbook. This is round 2.
# Consolidated findings: [paste or reference consolidated.md]
# Write findings to /Users/dan/migration-orchestrator/findings/fraud-model-c.md

# Tab 2
cd /Users/dan/repos/fraud-model-d
claude
# Same prompt
```

This gives you interactive control over each agent — you can check in, answer questions, and course-correct in real time.

### Option B: Non-interactive batch script

For fully mechanical rounds once the playbook is mature:

```bash
#!/bin/bash
# run-round.sh

REPOS=("fraud-model-c" "fraud-model-d")
PLAYBOOK="/Users/dan/migration-orchestrator/batch-model-migration-playbook.md"
FINDINGS="/Users/dan/migration-orchestrator/findings/consolidated.md"
FINDINGS_DIR="/Users/dan/migration-orchestrator/findings"
ROUND="2"

for repo in "${REPOS[@]}"; do
  echo "Starting migration for $repo (round $ROUND)..."
  
  cd "/Users/dan/repos/$repo"
  
  claude --print \
    --system-prompt "$(cat $PLAYBOOK)" \
    --allowedTools "bash,read_file,write_file,edit_file" \
    "You are migrating this repo per the playbook in your system prompt.
     This is round $ROUND.

     Consolidated findings from prior rounds:
     ---
     $(cat $FINDINGS 2>/dev/null || echo 'None yet.')
     ---

     Execute all migration steps. Run verification after each step.
     Write your findings report to ${FINDINGS_DIR}/${repo}.md
     
     If you encounter a blocker or judgment call not covered by the playbook,
     document it in your findings report and move on to the next step." \
    > "${FINDINGS_DIR}/${repo}-output.log" 2>&1 &
  
  echo "  Launched in background (PID: $!)"
done

wait
echo ""
echo "Round $ROUND complete."
echo "Review findings in: $FINDINGS_DIR/"
echo ""
for repo in "${REPOS[@]}"; do
  echo "--- $repo ---"
  cat "${FINDINGS_DIR}/${repo}.md" 2>/dev/null || echo "  (no findings file produced — check ${repo}-output.log)"
  echo ""
done
```

### Between rounds

After agents finish:

```bash
# 1. Review findings
cat migration-orchestrator/findings/fraud-model-c.md
cat migration-orchestrator/findings/fraud-model-d.md

# 2. Look for blockers or questions
grep -i "blocker\|question\|judgment" migration-orchestrator/findings/*.md

# 3. Update the playbook with new gotchas and decisions
# (edit batch-model-migration-playbook.md)

# 4. Consolidate findings
# Append new discoveries to consolidated.md

# 5. If repos need another round (e.g. blockers were resolved),
#    increment ROUND and re-run
```

-----

## Summary: The Feedback Loop

```
Round 1 (pilot):  1 repo, interactive
    ↓
Update playbook with discoveries
    ↓
Round 2: remaining repos in parallel (interactive or batch)
    ↓
Review findings, resolve blockers
    ↓
Update playbook, consolidate findings
    ↓
Round 3 (if needed): re-run repos that had blockers
    ↓
Done — all repos migrated, playbook is a complete record of decisions
```

The playbook starts sparse and ends comprehensive. Each round makes it better. By the last repo, the agent should be able to run almost entirely on autopilot.