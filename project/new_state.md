# PROJECT STATE - Databricks AI Classification and Auto-Retry (POC)

> **NOTE:** This file is the single source of truth for project context.  
> If starting a new conversation/session, paste this file's content first to restore full context before continuing work.

---

## 1. PROJECT GOAL

When a Databricks job fails:

1. Detect the failure.
2. Use AI to classify the error as **Transient / Permanent / Unknown**.
3. If **Transient** → auto-retry after a delay (e.g. 15 min), but first check if a newer run (any trigger type) already succeeded to avoid redundant reruns.
4. If **Permanent** → do NOT retry; rely on existing job failure notifications.
5. Before retrying, confirm no other run of that job is currently in progress.
6. Track failure history and actions taken.
7. Reuse Databricks built-in job failure notifications.

---

## 2. GROUND RULES (Working Agreement)

### Process Discipline
- Step-by-step only.
- Analyze requirements before building.
- No code without explaining approach first.
- Build from scratch.
- Clearly state user inputs needed.

### Code and Structure Discipline
- Clean separation of responsibilities.
- Simple, understandable code.
- Comment logical blocks with WHY.
- Remove temporary/debug code.
- Consistent formatting.
- Stay in scope.

### Decision Discipline
- Analyze future scenarios before locking decisions.
- Evaluate trade-offs together before committing.

---

## 3. WORKSPACE ENVIRONMENT

| Capability | Status |
|------------|---------|
| Compute for jobs | Confirmed |
| Workflows / Jobs UI | Confirmed |
| Foundation Model API (`ai_query`) | Confirmed Working |
| Unity Catalog access | Confirmed |
| Personal Access Tokens | Confirmed |
| Git Integration | GitHub linked, project not yet connected |

---

## 4. KEY DECISIONS MADE SO FAR

- AI approach: Databricks Foundation Model API (`ai_query`).
- Taxonomy: Transient / Permanent / Unknown.
- Retry logic checks for any newer successful run.
- Concurrency protection before retries.
- Use Databricks native job notifications.
- Minimal tracking table for POC.
- Git deferred until pipeline stabilization.
- Single parameterized dummy job.
- JSON classifier output.
- Parse failures mapped to Unknown + `PARSE_FAILED`.
- Separate `unknown_flagged` status.
- Orchestrator split into three functions.
- Delay based on `last_updated_at`.
- `all_run_ids` added for reliable deduplication.
- Single scheduled orchestration job every 15 minutes.

---

## 5. PROJECT STRUCTURE (Current)

```text
databricks-ai-classification-autoretry/
│
├── config/
│   └── config.py
│
├── dummy_jobs/
│   └── dummy_failure_job.py
│
├── sql/
│   └── create_tracking_table.sql
│
├── src/
│   ├── detection/
│   │   └── failure_detector.py
│   ├── classification/
│   │   └── ai_classifier.py
│   ├── tracking/
│   │   └── tracker.py
│   ├── utils/
│   │   └── databricks_client.py
│   └── orchestrator.py
│
├── tests/
│   ├── test_failure_detector
│   └── test_ai_classifier.py
│
├── run_pipeline_cycle
├── zz_explore_api
└── PROJECT_STATE.md
```

---

## 6. FAILURE_TRACKING TABLE SCHEMA

**Table:** `ai_classification_autoretry.poc.failure_tracking`

### Grain
One row per original failure incident.

### Columns
- `tracking_id`
- `job_id`
- `original_run_id`
- `latest_run_id`
- `all_run_ids`
- `error_message`
- `classification`
- `retry_count`
- `status`
- `first_failed_at`
- `last_updated_at`

### Status State Machine
- detected
- classified
- retry_scheduled
- retried
- resolved
- permanent_no_action
- unknown_flagged
- max_retries_reached

---

## 7. STEP-BY-STEP PROGRESS

### STEP 3 - COMPLETE
- Dummy jobs created and validated.
- Four Databricks jobs created.
- Success and failure paths verified.

### STEP 4 - COMPLETE
- Jobs API explored.
- Detection logic built and validated.

### STEP 5 - COMPLETE
- AI classification implemented.
- Prompt refined and tested.
- Regression suite passing.

### STEP 6 - COMPLETE
- Orchestrator implemented.
- Retry, resolution, and max-retry behavior validated.

### STEP 7 - COMPLETE
- Newer-successful-run check added.
- Concurrency protection added.
- Deduplication bug fixed.

### STEP 8 - COMPLETE
- Scheduled job created.
- End-to-end cycle validated.

### Current Status
✅ Core pipeline complete and running on a 15-minute schedule.

---

## 8. PENDING CHECKPOINTS

- [ ] Connect Git remote.
- [ ] Build full audit/history schema.
- [ ] Review Foundation Model API cost implications.
- [ ] Remove temporary `sys.path` workarounds.
- [ ] Explicitly validate newer-run-succeeded logic.
- [ ] Verify test file naming/content consistency.

### Important Lesson Learned
Files intended to be imported must be Workspace **Files**, not **Notebooks**.

---

## 9. HOW TO RESUME IF CONTEXT IS LOST

Start a new conversation and paste this file, then say:

> Continue from current step status in section 7/8.

The core pipeline (Steps 3-8) is complete and running. Future work should begin as Step 9+, such as:

- Git integration
- Audit/reporting enhancements
- Human feedback loop
- Cost optimization
