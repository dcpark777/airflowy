# CI/CD Preflight — Concept Doc & Implementation Guide

## What This Is

A Claude Code skill that catches CI/CD errors before pushing to Jenkins. It reads the pipeline config file already in every repo to understand what the pipeline does, then runs local equivalents of each step. When something fails, it reads the error, looks at the code, proposes a fix, and re-runs — all in one conversation.

The user’s debug loop today is: commit → push → wait for Jenkins → find errors → repeat. This skill eliminates the wait.

-----

## Key Context

- Every repo has a **pipeline config file** (YAML) that configures the Jenkins pipeline — pipeline type, step names, and parameters/config for each step
- The pipeline config names steps (`lint`, `docker_build`, `unit_test`, etc.) but the **actual commands** live in a separate **platform Jenkins library** repo
- The pipeline config format is mostly standardized, but teams can add custom steps
- **The Jenkinsfile is owned by the platform team** — don’t lint or modify it
- Claude Code may run in **sandboxed mode** — some commands (Docker, network) may be blocked. The skill should always try to run commands directly first, and if blocked by the sandbox, fall back to giving the user the exact command to run in their terminal and asking them to paste the output back
- **Python and pip are available** in the sandbox

-----

## Architecture

This is a **Claude Code skill** — no separate CLI tool, no extra config files to maintain.

The skill reads two things:

1. **The pipeline config file** (in every repo) — the single source of truth for what the pipeline does
1. **A step mapping reference** (generated once from the platform Jenkins library) — maps pipeline config step names to what Jenkins actually does, so the skill builds accurate local equivalents

### Execution Model: Try First, Fallback Gracefully

The skill does NOT pre-categorize steps into “can run” and “can’t run.” Instead:

1. Try to run the local equivalent directly
1. If it works → great, check the output, continue
1. If the sandbox blocks it (command not found, permission denied, network error) → tell the user the exact command to run in their terminal, ask them to paste the output back
1. Either way, analyze the output and fix issues

This means the skill works at full capability on an unrestricted machine, and gracefully degrades in sandbox mode without any config changes.

### User Flow

```
User: "check this before I push"

Claude Code:
  1. Reads pipeline config → pipeline_type: model_deployment
     Steps: lint, docker_build, unit_test, integration_test, push, deploy

  2. Loads step mapping reference → knows what each step does

  3. Step: lint
     Tries: ruff check . && hadolint Dockerfile
     → ruff works, hadolint not found → falls back to built-in Dockerfile pattern checks
     → Finds: COPY source references file that doesn't exist
     → Fixes Dockerfile, continues

  4. Step: docker_build
     Tries: docker build --build-arg PYTHON_VERSION=3.11 -t model:test .
     → Sandbox blocks Docker → tells user:
       "I can't run Docker from here. Run this in your terminal:
        docker build --build-arg PYTHON_VERSION=3.11 -t model:test ."
     → User pastes output showing pip install failure on line 31

  5. Claude Code reads error + requirements.txt
     → cryptography package needs libffi-dev, not installed in container
     → Adds apt-get install to Dockerfile, tells user to re-run
     → User pastes: build succeeds

  6. Step: unit_test
     Tries: docker run --rm model:test pytest tests/unit -x -q
     → Sandbox blocks Docker → tells user the command
     → User pastes: 47 passed

  7. Step: integration_test
     → Knows from step mapping this needs Snowflake + Redis
     → Validates config/test.yaml exists and parses ✓
     → Validates .env.test has required keys ✓
     → Validates test fixture files exist ✓
     → Tries to run: blocked by sandbox
     → "I validated all configs and fixtures look good. If you want to
        run the actual integration tests, try:
        docker run --env-file .env.test model:test pytest tests/integration -x"

  8. Steps: push, deploy
     → Skips — these are CI-only concerns
     → But validates: image tag format is correct, K8s manifests parse

  9. "All checks passed. Safe to push."
```

-----

## Build Steps for Claude Code

### Step 1: Gather context from the user

Before building anything, ask for these in order:

**1a. The platform Jenkins library repo**

The user will clone it locally and give you the path. Read this to understand what each pipeline config step name actually does. Look for:

- Step definitions — what commands/scripts each step name maps to
- Pipeline type definitions (model_deployment, library_package, etc.) and how they differ
- Shared helper scripts or functions that steps call
- Environment variables that Jenkins injects at each stage
- Build args, registry URLs, base images
- Step-specific configuration options and their defaults
- Custom step types that teams can add and how they’re defined
- Any ordering constraints or conditional logic between steps

**1b. A sample pipeline config file**

Ask the user to show you the pipeline config file from their primary repo. Parse it to understand:

- What pipeline type is configured
- What steps are enabled
- What parameters/configs are set for each step
- Any custom steps or overrides

**1c. The target project repo**

Look at the repo itself:

- Dockerfile(s) — base image, build stages, what gets COPY’d
- Test structure — where unit vs integration tests live, what framework
- Config files — test configs, env files, what keys they need
- Dependencies — requirements.txt, pyproject.toml, setup.py
- What external services tests depend on (Snowflake, Redis, Kafka, etc.)

**1d. Recent Jenkins failure logs** (optional but very valuable)

Ask for 3-5 recent build failures. For each:

- The exact error output
- Which pipeline step failed
- What the fix was

These let you build project-specific error patterns that make the skill much smarter.

**1e. Local environment details**

- Does the user run Docker locally?
- Which services are reachable from their laptop (VPN, dev endpoints, etc.)
- Any proxy or certificate considerations for network access

### Step 2: Build the step mapping reference

This is the core intelligence of the skill. Read the platform Jenkins library and produce a reference file that maps every pipeline config step name to what Jenkins actually does.

For each step, document:

```yaml
step_name:
  description: "Human-readable summary of what this step verifies"
  pipeline_types: [model_deployment, library_package]
  jenkins_commands:
    - "The actual sh commands Jenkins runs, in order"
  local_equivalent:
    - "What to run locally to check the same thing"
  config_keys:
    - "pipeline config keys this step reads"
  jenkins_env_vars:
    - "env vars Jenkins injects that the step uses"
  required_services: []
  required_configs: []
  required_fixtures: []
  skip_locally: false     # true only for push, deploy, notification steps
  common_failures:
    - pattern: "regex matching common error output"
      meaning: "What this error means"
      fix: "How to fix it"
```

**Important:** The mapping doesn’t need to perfectly replicate what Jenkins does. It needs to catch the same categories of errors. If Jenkins runs `pytest tests/unit -x -q --timeout=120` and the local equivalent runs `pytest tests/unit -x -q`, that’s fine — both catch the same test failures.

Present the completed mapping to the user for review.

### Step 3: Build the skill (SKILL.md)

The skill file should include:

**Trigger description** (the `description` field in YAML frontmatter):

Trigger on: CI/CD debugging, Jenkins failures, pipeline errors, Docker build issues, integration test failures, “my build broke”, “Jenkins is failing”, fixing a Dockerfile, or wanting to validate changes before pushing. Also trigger on: “preflight”, “dry run”, “test my pipeline”, “check before I push”, “run the pipeline locally”, or when the user is repeatedly fixing and retesting the same build. This skill saves hours of commit-push-wait-fail cycles.

**Core workflow in the skill body:**

```
1. FIND THE PIPELINE CONFIG
   - Look for the pipeline config file in the project root
   - If not found, ask the user where it is
   - Parse it: extract pipeline_type, steps list, and config/parameters

2. LOAD THE STEP MAPPING
   - Read the step mapping reference file
   - For each step in the pipeline config, look up what it does
   - If a step isn't in the mapping (custom step), ask the user what it does

3. FOR EACH STEP (in pipeline config order):

   a. Announce what you're about to do:
      "Step 3/6: unit_test — running pytest on tests/unit"

   b. If the step mapping says skip_locally=true (push, deploy):
      - Skip it
      - But still validate any artifacts it depends on
      - "Skipping push (CI-only), but the image built successfully ✓"

   c. Otherwise, try to run the local equivalent:
      - Build the command using pipeline config + step mapping
      - Run it
      - If it works → report result, continue to next step
      - If the sandbox blocks it:
          → Tell the user the exact command to run
          → "I can't run this from the sandbox. Run this in your terminal:
             <command>
             Then paste the output back here."
          → Continue analysis with pasted output

   d. If the step FAILS (whether run by Claude or pasted by user):
      - Parse the error output against known failure patterns
      - Read the relevant source files
      - Propose and apply a fix
      - Re-run (or ask user to re-run) the failing step
      - If still fails, iterate

   e. Stop on first failure by default (like Jenkins)
      - Offer: "Want me to continue checking the remaining steps?"

4. REPORT RESULTS
   ✅ lint — passed
   ✅ docker_build — passed (ran in terminal)
   ✅ unit_test — passed (ran in terminal)
   ⚠️  integration_test — configs validated, tests need external services
   ⏭️  push — skipped (CI-only)
   ⏭️  deploy — skipped (CI-only)

   "Safe to push" or "Fix these issues first"
```

**Error pattern matching:**

When a step fails, match error output against known patterns for actionable feedback:

Docker: COPY not found, base image missing, pip failures, multi-stage reference errors
Dependencies: version conflicts, missing system libs, PySpark/Java mismatches
Tests: ModuleNotFoundError, ConnectionRefused, missing fixtures, Snowflake auth
Dockerfile lint: :latest tags, ADD vs COPY, root user, missing .dockerignore, bad layer caching

Extend with project-specific patterns from the user’s failure logs.

**Pipeline-config-aware checks:**

Use pipeline config to make checks smarter:

- If config says python_version: 3.11, verify Dockerfile FROM matches
- If config says test_markers, use them in pytest commands
- If config has step-specific params, pass them to local equivalents
- If config says pipeline_type: model_deployment, validate project structure matches expectations

### Step 4: Test with the real repo

1. Run the skill against the user’s repo
1. Walk through each step, verify the local equivalents are correct
1. Tune the step mapping based on what comes up
1. Add error patterns for failures that occur
1. Once stable, try on a second repo to verify it generalizes

-----

## Design Decisions (settled)

|Decision           |Choice                             |Rationale                                             |
|-------------------|-----------------------------------|------------------------------------------------------|
|Delivery           |Claude Code skill only             |No CLI script needed                                  |
|Config             |Pipeline config file is the config |No extra config file to maintain                      |
|Jenkinsfile linting|No                                 |Platform team owns it                                 |
|Sandbox handling   |Try first, fallback to terminal    |Full capability when unrestricted, degrades gracefully|
|Step mapping       |One-time setup from Jenkins library|Rarely changes, regenerate when it does               |
|Scope              |One repo first                     |Start with primary repo, expand later                 |
|Failure behavior   |Stop on first, offer to continue   |Like Jenkins, with flexibility                        |

-----

## What to Ask the User (Checklist)

Use this as your onboarding checklist when starting the build:

- [ ] Path to the cloned platform Jenkins library repo
- [ ] Path to the target project repo
- [ ] Show me the pipeline config file from that repo
- [ ] 3-5 recent Jenkins failure logs (the raw console output)
- [ ] Do you run Docker locally? Which services can you reach from your laptop?
- [ ] Review the step mapping I generate from the Jenkins library
- [ ] Review the skill after first pass and run it together

-----

## Appendix: Error Pattern Reference

Starting patterns — extend with project-specific patterns from the user’s actual failures.

### Docker Build Errors

**COPY/ADD file not found**
Pattern: `COPY failed: file not found in build context`
Causes: wrong path relative to context, file in .dockerignore, file not committed, typo
Fixes: check path relative to Docker context (not Dockerfile location); check .dockerignore; run git status; verify exact filename (case-sensitive on Linux)

**Base image pull failure**
Pattern: `manifest for X not found` or `pull access denied`
Causes: tag doesn’t exist, private registry needs auth, architecture mismatch
Fixes: verify image:tag exists; check docker login; add –platform linux/amd64

**pip install failures in Docker**
Pattern: `Could not find a version that satisfies the requirement`
Causes: package version yanked, missing system deps, Python version mismatch
Fixes: verify package exists; add system deps (gcc, python3-dev); check Python version in base image

**Multi-stage reference errors**
Pattern: `invalid from flag value X`
Causes: stage name typo in COPY –from=, stage order wrong
Fixes: grep -n ‘FROM|–from=’ Dockerfile

### Dependency Errors

**Version conflicts**
Pattern: `Cannot install X and Y because conflicting dependencies`
Fixes: pip install –dry-run; pin transitive dep; use pip-compile

**Missing system libraries**
Pattern: `fatal error: Python.h: No such file or directory`
Common: psycopg2→libpq-dev, cryptography→libffi-dev libssl-dev, numpy→libblas-dev, pyspark→openjdk-11-jdk-headless

**PySpark version mismatches**
Pattern: `Py4JJavaError` or `ClassNotFoundException`
Fixes: pin PySpark to cluster version; add JARs via –packages

### Test Failures

**Connection refused**
Pattern: `ConnectionRefusedError` or `socket.timeout`
Fixes: verify service running; check config; add retry logic

**Missing fixtures**
Pattern: `FileNotFoundError` in test setup
Fixes: check COPY in Dockerfile; use pathlib relative paths; check git lfs

**Import errors**
Pattern: `ModuleNotFoundError`
Fixes: verify in requirements.txt; check pip path; set PYTHONPATH

**Snowflake auth**
Pattern: `ProgrammingError: does not exist or not authorized`
Fixes: verify test env params; check credential freshness; use test warehouse

### Dockerfile Lint (static, no Docker needed)

- :latest or untagged base images → pin version
- ADD instead of COPY → use COPY
- Running as root → add non-root USER
- pip without –no-cache-dir → add flag
- COPY . before requirements install → copy requirements first
- Missing .dockerignore → create one
- COPY sources that don’t exist → fix path