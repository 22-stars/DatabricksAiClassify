1. PROJECT GOAL
================================================================================
When a Databricks job fails:
1. Detect the failure IMMEDIATELY via Databricks system table event
   (system.lakeflow.job_run_timeline) - no workspace scanning, no polling,
   no changes to existing jobs.
2. Use AI to classify the error as Transient / Permanent / Unknown.
3. If Transient -> schedule a retry check for now + 15 min. At that time,
   check if the job's latest run already succeeded (avoid redundant reruns).
   If not succeeded, retry.
4. If Permanent -> do NOT retry; rely on existing job failure notifications.
5. Before retrying, confirm no other run of that job is currently in progress
   (avoid concurrent/duplicate runs).
6. Track failure history and actions taken; support retry-count limits and
   avoid reprocessing the same failure.
7. Notifications: reuse Databricks built-in job failure notifications.

ARCHITECTURE NOTE (updated):
- No fixed-schedule scanning job (previous 15-min design retired).
- Two lightweight jobs instead:
  a) Reactor Job - triggered by Table Update trigger on
     system.lakeflow.job_run_timeline; reacts instantly to failures anywhere
     in the workspace; zero changes required to existing 100+ jobs.
  b) Retry-Executor Job - runs every 1-2 min, but only checks OUR OWN small
     failure_tracking table for rows due for retry (next_retry_at <= now).
     Does not scan the workspace.

4. KEY DECISIONS MADE SO FAR (additions)
- Replaced 15-min workspace-scanning orchestrator with event-driven
  Reactor Job (system table trigger) + lightweight Retry-Executor Job.
- Detection logic changed from time-window scan to watermark-based read of
  new system table rows (reactive, not scheduled).
- "Newer run succeeded" check re-scoped to "latest run status check at
  retry-due time" - performed by Retry-Executor Job.
- Concurrency check retained, now performed by Retry-Executor Job at
  retry-due time.
- No modifications made to any of the 100+ existing jobs in the workspace.

5. PROJECT STRUCTURE (Updated)
databricks-ai-classification-autoretry/
│
├── config/
│   └── config.py                      (add: RETRY_DELAY_MINUTES, MAX_RETRIES)
│
├── dummy_jobs/
│   └── dummy_failure_job.py
│
├── sql/
│   └── create_tracking_table.sql      (add: next_retry_at column)
│
├── src/
│   ├── detection/
│   │   └── failure_detector.py        (MODIFIED: watermark-based read)
│   ├── classification/
│   │   └── ai_classifier.py           (unchanged)
│   ├── tracking/
│   │   └── tracker.py                 (add next_retry_at support)
│   ├── retry/
│   │   └── retry_executor.py          (NEW: due-check, concurrency, retry)
│   ├── utils/
│   │   └── databricks_client.py
│   └── orchestrator.py                (split: reactor_pipeline(),
│                                        retry_executor_pipeline())
│
├── tests/
│   ├── test_failure_detector
│   ├── test_ai_classifier.py
│   └── test_retry_executor.py         (NEW)
│
└── PROJECT_STATE.md

6. FAILURE_TRACKING TABLE SCHEMA (Updated)
Table: ai_classification_autoretry.poc.failure_tracking
Columns (added):
- next_retry_at   (NEW - timestamp for when retry check should occur)

7. STEP-BY-STEP PROGRESS (addition)
STEP 9 - IN PROGRESS
- Architecture redesign: event-driven Reactor Job + Retry-Executor Job,
  replacing single 15-min scanning job.
- Reason: avoid DBU waste from constant polling; avoid modifying 100+
  existing jobs; enable true self-healing behavior.
- Pending: build failure_detector.py watermark logic, build
  retry_executor.py, update tracking schema, configure Table Update trigger,
  configure Retry-Executor schedule, retire old scheduled orchestrator job.