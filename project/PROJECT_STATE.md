  # PROJECT STATE - Databricks AI Classification and Auto-Retry (POC)

  NOTE: This file is the single source of truth for project context.
  If starting a new conversation/session, paste this file's content first
  to restore full context before continuing work.

  ================================================================================
  1. PROJECT GOAL
  ================================================================================

  When a Databricks job fails:
  1. Detect the failure.
  2. Use AI to classify the error as Transient / Permanent / Unknown.
  3. If Transient -> auto-retry after a delay (e.g. 15 min), but first check if
    a newer run (any trigger type) already succeeded - avoid redundant reruns.
  4. If Permanent -> do NOT retry, rely on existing job failure notifications.
  5. Before retrying, confirm no other run of that job is currently in progress
    (avoid concurrent/duplicate runs).
  6. Track failure history and actions taken (for future learning/audit) - full
    implementation deferred, but minimal state tracking required now to
    support retry-count limits and avoid reprocessing the same failure.
  7. Notifications: reuse Databricks built-in job failure notifications for now
    (no custom notification system in POC phase).

  ================================================================================
  2. GROUND RULES (Working Agreement)
  ================================================================================

  Process discipline:
  - Step-by-step only - never jump ahead before current step is validated.
  - Analyze requirements/scenarios BEFORE building, to avoid backtracking later.
  - No code without first explaining approach and getting confirmation.
  - Build from scratch, nothing assumed or skipped.
  - Clearly state what is needed from the user at each step.

  Code and structure discipline:
  - Clean, proper project structure - separated by responsibility.
  - Simple, understandable code - no unnecessary cleverness.
  - Comment before every logical block, explaining WHY.
  - Clean up scratch/debug code once a step is validated.
  - Consistent clean formatting.
  - Stay in scope - no extra complexity beyond current step's goal.

  Decision discipline ("no going back" rule):
  - Before locking any decision, explicitly analyze future scenarios/edge cases
    that could break it later.
  - If multiple valid approaches exist, pause and evaluate trade-offs together
    before proceeding.

  ================================================================================
  3. WORKSPACE ENVIRONMENT (Confirmed Capabilities)
  ================================================================================

  - Compute (for jobs)................................ CONFIRMED AVAILABLE
  - Workflows/Jobs UI................................. CONFIRMED, can create jobs
  - Foundation Model API (ai_query).................... CONFIRMED WORKING
        (tested with databricks-meta-llama-3-1-8b-instruct, got valid response)
  - Unity Catalog access............................... CONFIRMED, can create schema/tables
  - Personal Access Token capability................... CONFIRMED AVAILABLE
  - Git integration..................................... GitHub already linked
        (debashishmohanty69@gmail.com) - NOT yet connected to this project
        (deferred, see Pending Checkpoints)

  ================================================================================
  4. KEY DECISIONS MADE SO FAR
  ================================================================================

  - AI approach for POC:
      Databricks Foundation Model API (ai_query).
      Reason: In-workspace, no external API keys/security review needed for
      POC; swappable later via abstraction function.

  - Classification taxonomy:
      Transient / Permanent / Unknown (3 states, not binary).
      Reason: Avoids forcing AI to guess when uncertain.

  - Retry check logic:
      Check if ANY newer run (any trigger type) of the job succeeded since
      failure, not just the same run.
      Reason: User requirement - "irrespective of trigger/manual run".

  - Concurrency check:
      Must check no other run is in progress before retrying.
      Reason: Avoid duplicate/parallel runs.

  - Notification:
      Reuse existing Databricks job-level failure notifications.
      Reason: Simpler for POC; no new channel needed yet.

  - Tracking table:
      Minimal version now (state tracking for retry-count + processed-failure
      dedup); full audit/feedback schema deferred.
      Reason: Retry logic depends on it; full feedback loop is future scope.

  - Git setup:
      Deferred - working in plain Workspace folder for now.
      Reason: Avoid blocking progress; checkpoint set to connect after
      Step 3/4.

  - Dummy job design:
      One parameterized notebook (dummy_failure_job.py) using widgets, not
      multiple separate notebooks.
      Reason: Simpler to maintain, flexible for adding more failure types
      later.

  - Classification output format:
      Structured JSON (classification, reasoning, confidence) via ai_query,
      called through spark.sql() with parameterized :prompt arg.
      Reason: Reliable programmatic parsing; avoids string-escaping bugs
      found in earlier prototyping; reasoning/confidence fields aid future
      debugging.

  - Classification parse-failure handling:
      If AI response fails JSON parsing, fall back to classification
      "Unknown" with a distinct parse_status of "PARSE_FAILED" (vs "OK"
      for normal responses).
      Reason: Fails safe without crashing pipeline, while preserving
      visibility into genuine parsing failures vs actual model uncertainty.

  - Unknown classification status:
      Unknown gets its own tracking status "unknown_flagged", separate
      from Permanent's "permanent_no_action".
      Reason: Semantically different situations (AI confident it won't
      work vs AI genuinely unsure); merging them is irreversible later
      since historical rows couldn't be retroactively separated; low cost
      to keep them distinct now.
  
  - Run_id deduplication bug fix:
     get_known_run_ids() originally checked only original_run_id and
     latest_run_id columns, but latest_run_id gets OVERWRITTEN on every
     retry - causing old intermediate run_ids to be "forgotten" and
     re-detected as brand new failures (found during testing when 3
     duplicate rows were created for the same retry chain).
     Fix: Added all_run_ids ARRAY<STRING> column that APPENDS (never
     overwrites) every run_id in a failure's full retry history;
     get_known_run_ids() now checks membership across this array instead.
     Reason: Correctness - a snapshot of only "current" run_ids can never
     reliably represent full history once values get overwritten.
  ================================================================================
  5. PROJECT STRUCTURE (Current + Planned)
  ================================================================================

  src/ [in progress]
      detection/
          failure_detector.py  [created]
      classification/
          ai_classifier.py  [created]
      retry/
          retry_handler.py
      tracking/
          tracker.py  [created - only get_known_run_ids() so far, needs insert/update functions]
      utils/
          databricks_client.py  [created]

  sql/ [created]
      create_tracking_table.sql  [exists as notebook, not yet saved as .sql file - table itself is live in Unity Catalog]

  tests/ [created]
      test_failure_detector  [created]
      test_ai_classifier.py  [created]

  ================================================================================
  6. CURRENT STEP STATUS
  ================================================================================

  STEP 5 - COMPLETE: AI Classification Logic

  Sub-progress:
  [x] Designed classification prompt (3-state taxonomy: Transient/Permanent/Unknown)
  [x] Decided structured JSON output format (classification, reasoning, confidence)
  [x] Decided to call ai_query via spark.sql() with parameterized :prompt arg
  [x] Tested prompt v1 against known dummy errors - found Unknown category too weak, model guessed instead of admitting uncertainty
  [x] Revised prompt (v2) - strengthened Unknown definition, added 2 new few-shot examples
  [x] Retested all known test errors with prompt v2 - all correct, no regressions
  [x] Tested additional edge cases (permission errors, schema mismatches, throttling) - all correct across all 3 categories
  [x] Confirmed JSON parsing reliable via json.loads() across all test cases
  [x] Decided fallback behavior for parse failures (see KEY DECISIONS MADE SO FAR)
  [x] Created src/classification/ai_classifier.py - classify_error(error_text, spark) function
  [x] Created tests/test_ai_classifier.py - regression test using all validated error cases
  [x] Ran packaged function end-to-end - all test cases pass, matches manual validation results

  Decision recorded: classify_error() takes spark as an explicit parameter rather than assuming a global variable, to keep the function testable and reusable outside notebook context.

  --------------------------------------------------------------------------------

STEP 6 - COMPLETE: Orchestrator

Sub-progress:
[x] Decided orchestrator split into 3 separate functions (detect_classify_decide, 
    execute_due_retries, check_retry_outcomes)
[x] Added new tracker.py functions: insert_new_failure, update_classification, 
    get_rows_by_status, mark_as_retried, mark_as_resolved, mark_as_max_retries_reached
[x] Added scheduled_retry_at column to failure_tracking table (via ALTER TABLE) 
    - note: ended up using last_updated_at + RETRY_DELAY_MINUTES as the due-check 
    instead, scheduled_retry_at reserved for future use if needed
[x] Added MAX_RETRY_ATTEMPTS = 3 to config.py
[x] Created src/orchestrator.py with all 3 functions
[x] Tested detect_classify_decide() - correctly detected, classified, and 
    routed 3 dummy failures (1 Transient -> retry_scheduled, 2 Permanent -> 
    permanent_no_action)
[x] Tested execute_due_retries() - correctly triggered retry via run_now(), 
    updated status to 'retried', incremented retry_count
[x] Tested check_retry_outcomes() - correctly detected still-running state 
    (skip), correctly detected failure and re-classified/looped back to 
    retry_scheduled, correctly incremented retry_count across 3 full cycles
[x] Tested max-retry cutoff - after retry_count reached 3 (MAX_RETRY_ATTEMPTS), 
    correctly set status to max_retries_reached instead of retrying again

Decision recorded: Used last_updated_at timestamp + RETRY_DELAY_MINUTES (15) as 
the "is this due for retry" check, rather than a separate scheduled_retry_at 
value. Reason: Simpler, one less field to manage; last_updated_at already gets 
set at classification time, which is the correct reference point for the delay.

STEP 7 - COMPLETE: Remaining Goal 1 Requirements

Sub-progress:
[x] Added "newer run already succeeded" check to execute_due_retries() - 
    skips retry and marks resolved if any newer run (any trigger type) 
    succeeded since the failure
[x] Added concurrency check to execute_due_retries() - skips retry if 
    another run of the job is currently RUNNING or PENDING
[x] Found and fixed run_id deduplication bug (see KEY DECISIONS)
[x] Concurrency check VALIDATED in live scheduled run - 2 of 3 due retries 
    were correctly skipped ("another run currently in progress") when they 
    shared the same job_id as a retry already in flight. Confirmed working 
    as designed, found naturally during Step 8 testing (not a planned test).
[ ] Newer-run-succeeded check still not explicitly tested - low risk, 
    simple boolean logic, deferred.

STEP 8 - COMPLETE: Scheduling

Sub-progress:
[x] Decided: 1 Databricks Job running all 3 orchestrator functions in 
    sequence, every 15 minutes
[x] Created notebooks/run_pipeline_cycle - thin runner notebook
[x] Created scheduled Databricks Job: ai_classification_autoretry_pipeline
[x] Ran manual test - full cycle executed successfully: detect (0 new) -> 
    3 retry_scheduled rows found -> 1 retried, 2 correctly skipped via 
    concurrency check -> outcome check correctly skipped still-running run

PROJECT STATUS: Core pipeline complete and now running on an automated 
15-minute schedule. All original Goal 1 requirements (Section 1) implemented and 
validated: detect, classify (Transient/Permanent/Unknown), retry with delay, 
skip permanent, concurrency-safe, newer-run-aware, max-retry capped, 
scheduled automation.
    

  --------------------------------------------------------------------------------

  ================================================================================
  7. PENDING CHECKPOINTS (Do Not Forget)
  ================================================================================

    [ ] Connect Git remote - deferred from Step 2, must be done after dummy job
        (Step 3) or retry logic (Step 4) is working, before more code
        accumulates.
    [ ] Full tracking/audit table schema + human feedback correction
        mechanism - deferred to later phase, after core pipeline works.
    [ ] Confirm whether Foundation Model API usage has any cost implications
        on Free Edition (pay-per-token) - worth checking before heavy testing.
    [ ] Remove temporary sys.path workaround in tracker.py (and any other file
      that copies it) once project is connected to a Git folder - replace
      with clean package-style imports once repo root is auto-added to path.

    [ ] IMPORTANT LESSON LEARNED: Any file meant to be imported as a Python
      module (config.py, tracker.py, databricks_client.py, and any future
      similar files) MUST be created as a Workspace "File", NOT a "Notebook".
      Notebooks are only for files meant to be run directly (e.g.
      dummy_failure_job, orchestrator). Also: plain .py files do NOT get
      "spark" auto-injected - must use SparkSession.builder.getOrCreate().

  ================================================================================
  8. HOW TO RESUME IF CONTEXT IS LOST
  ================================================================================

  If starting a new conversation, paste this entire file, then say:
  "Continue from current step status in section 6."