# ---------------------------------------------------------------------------
# webhook_handler.py
#
# Triggered by Databricks job failure webhooks - eliminates the need for a
# polling schedule. Only activates when jobs actually fail, consuming zero
# compute during idle periods.
#
# Supports 100+ jobs without modification - each failure triggers its own
# webhook invocation independently.
# ---------------------------------------------------------------------------

# WHY: In Databricks Repos, Python imports work relative to the repo root
# automatically. This import block handles both Databricks Repo context
# and local development/testing.
import sys
import os
import json
import time
from datetime import datetime, timedelta

_project_root = os.environ.get(
    "PROJECT_ROOT",
    "/Repos/debashish8101@gmail.com/databricks-ai-classify/project"
)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from pyspark.sql import SparkSession
from databricks.sdk.service.jobs import RunResultState, RunLifeCycleState

from config.config import MAX_RETRY_ATTEMPTS
from src.classification.ai_classifier import classify_error, generate_fix_suggestion
from src.tracking.tracker import (
    insert_new_failure,
    update_classification,
    get_rows_by_status,
    mark_as_retried,
    mark_as_resolved,
    mark_as_max_retries_reached,
    get_known_run_ids,
)
from src.utils.databricks_client import get_workspace_client

spark = SparkSession.builder.getOrCreate()

RETRY_DELAY_MINUTES = 15


def handle_failure_notification(job_id, run_id, error_message, triggered_at=None):
    """
    Main webhook handler - called when ANY Databricks job fails.

    Args:
        job_id: Databricks job ID that failed
        run_id: Specific run ID that failed
        error_message: Error text from the failed run
        triggered_at: Optional timestamp for when failure was detected
    """
    if triggered_at is None:
        triggered_at = datetime.utcnow()

    print(f"[webhook] Job {job_id} failed - Run {run_id} at {triggered_at}")

    # Step 1: Check if we already know about this run (dedup protection)
    known_run_ids = get_known_run_ids()
    if str(run_id) in known_run_ids:
        print(f"[webhook] Run {run_id} already tracked - skipping")
        return None

    # Step 2: Classify the error using AI
    result = classify_error(error_message, spark)
    classification = result["classification"]
    reasoning = result.get("reasoning", "")

    # Step 3: Insert tracking record immediately
    tracking_id = insert_new_failure(
        job_id=str(job_id),
        run_id=str(run_id),
        error_message=error_message,
        first_failed_at=triggered_at,
    )
    print(f"[webhook] Classified as: {classification} -> tracking_id={tracking_id}")

    # Step 4: Take action based on classification
    if classification == "Transient":
        _handle_transient_error(tracking_id, job_id, triggered_at, reasoning)
    elif classification == "Permanent":
        _handle_permanent_error(tracking_id, job_id, error_message, reasoning)
    else:  # Unknown
        _handle_unknown_error(tracking_id, job_id, error_message, reasoning)

    return tracking_id


def _handle_transient_error(tracking_id, job_id, failure_time, reasoning):
    """
    Transient errors get auto-retried after a delay.
    Applies: retry delay, newer-run check, concurrency check.
    """
    update_classification(tracking_id, "Transient", "retry_scheduled")
    print(f"[Transient] Job {job_id} marked for retry after {RETRY_DELAY_MINUTES} min")

    # Immediately check if retry is due (in case webhook fires later than delay)
    _check_and_execute_retry(tracking_id, job_id)


def _handle_permanent_error(tracking_id, job_id, error_message, reasoning):
    """
    Permanent errors get NO retry.
    Notify the job creator with:
    - Actual error message (technical details)
    - AI-generated fix suggestion (actionable steps)
    """
    update_classification(tracking_id, "Permanent", "permanent_no_action")
    print(f"[Permanent] Job {job_id} marked no-action - permanent error")

    # Generate AI-powered fix suggestion (separate from error message)
    fix_suggestion = _generate_ai_fix_suggestion(job_id, error_message, reasoning)

    # Get job creator for notification
    job_creator = _get_job_creator(job_id)

    # Send notification with clearly separated sections
    _send_permanent_error_notification(
        job_id=job_id,
        job_creator=job_creator,
        error_message=error_message,
        ai_reasoning=reasoning,
        fix_suggestion=fix_suggestion,
    )

    # Store fix suggestion in tracking table for audit
    _store_fix_suggestion(tracking_id, fix_suggestion)


def _handle_unknown_error(tracking_id, job_id, error_message, reasoning):
    """
    Unknown errors get flagged for manual review.
    """
    update_classification(tracking_id, "Unknown", "unknown_flagged")
    print(f"[Unknown] Job {job_id} flagged for manual review")


def _check_and_execute_retry(tracking_id, job_id):
    """
    Check if retry conditions are met and execute if safe.
    Called from transient handler - runs immediately after classification.
    """
    # Wait for the retry delay period
    print(f"[Retry] Waiting {RETRY_DELAY_MINUTES} minutes before retry check...")
    time.sleep(RETRY_DELAY_MINUTES * 60)

    client = get_workspace_client()
    recent_runs = list(client.jobs.list_runs(job_id=int(job_id), limit=10))
    first_failed_millis = int(datetime.utcnow().timestamp() * 1000)

    # Safety check 1: newer run already succeeded
    already_succeeded = any(
        r.state.result_state == RunResultState.SUCCESS
        and r.start_time > first_failed_millis
        for r in recent_runs
    )
    if already_succeeded:
        mark_as_resolved(tracking_id)
        print(f"[Retry] Skipped - newer run already succeeded")
        return

    # Safety check 2: no run in progress
    in_progress = any(
        r.state.life_cycle_state in (
            RunLifeCycleState.RUNNING,
            RunLifeCycleState.PENDING,
        )
        for r in recent_runs
    )
    if in_progress:
        print(f"[Retry] Skipped - run already in progress")
        return

    # Safe to retry
    resp = client.jobs.run_now(job_id=int(job_id))
    new_run_id = str(resp.run_id)
    mark_as_retried(tracking_id, new_run_id)
    print(f"[Retry] Triggered retry -> new_run_id={new_run_id}")


def process_due_retries():
    """
    Process retries that were scheduled but not executed due to:
    - Previous webhook handler being interrupted
    - Concurrency conflicts that have since cleared
    - Retries from previous sessions

    This is a fallback mechanism - normally handled by _check_and_execute_retry().
    """
    client = get_workspace_client()
    scheduled_rows = get_rows_by_status("retry_scheduled")

    print(f"Found {len(scheduled_rows)} row(s) with status 'retry_scheduled'")

    for row in scheduled_rows:
        elapsed = datetime.utcnow() - row["last_updated_at"]
        if elapsed < timedelta(minutes=RETRY_DELAY_MINUTES):
            print(f"  - tracking_id={row['tracking_id']} not due yet")
            continue

        job_id = int(row["job_id"])
        recent_runs = list(client.jobs.list_runs(job_id=job_id, limit=10))
        first_failed_millis = int(row["first_failed_at"].timestamp() * 1000)

        # Check newer successful run
        already_succeeded = any(
            r.state.result_state == RunResultState.SUCCESS
            and r.start_time > first_failed_millis
            for r in recent_runs
        )
        if already_succeeded:
            mark_as_resolved(row["tracking_id"])
            print(f"  - tracking_id={row['tracking_id']} -> resolved (newer run succeeded)")
            continue

        # Check no run in progress
        in_progress = any(
            r.state.life_cycle_state in (
                RunLifeCycleState.RUNNING,
                RunLifeCycleState.PENDING,
            )
            for r in recent_runs
        )
        if in_progress:
            print(f"  - tracking_id={row['tracking_id']} skipped (run in progress)")
            continue

        resp = client.jobs.run_now(job_id=job_id)
        new_run_id = str(resp.run_id)
        mark_as_retried(row["tracking_id"], new_run_id)
        print(f"  - tracking_id={row['tracking_id']} retried -> {new_run_id}")


def check_retry_outcomes():
    """
    Check 'retried' rows to see if the retry succeeded or needs further action.
    """
    client = get_workspace_client()
    retried_rows = get_rows_by_status("retried")

    print(f"Found {len(retried_rows)} row(s) with status 'retried'")

    for row in retried_rows:
        run = client.jobs.get_run(run_id=int(row["latest_run_id"]))

        if run.state.result_state is None:
            print(f"  - tracking_id={row['tracking_id']} still running, skipping")
            continue

        if run.state.result_state == RunResultState.SUCCESS:
            mark_as_resolved(row["tracking_id"])
            print(f"  - tracking_id={row['tracking_id']} -> resolved (SUCCESS)")
        else:
            # Failed again - check retry limit
            if row["retry_count"] >= MAX_RETRY_ATTEMPTS:
                mark_as_max_retries_reached(row["tracking_id"])
                print(f"  - tracking_id={row['tracking_id']} -> max_retries_reached")
            else:
                error_message = "No error message available"
                if run.tasks:
                    task_run_id = run.tasks[0].run_id
                    output = client.jobs.get_run_output(run_id=task_run_id)
                    if output.error:
                        error_message = output.error

                result = classify_error(error_message, spark)
                classification = result["classification"]

                if classification == "Transient":
                    new_status = "retry_scheduled"
                elif classification == "Permanent":
                    new_status = "permanent_no_action"
                else:
                    new_status = "unknown_flagged"

                update_classification(row["tracking_id"], classification, new_status)
                print(f"  - tracking_id={row['tracking_id']} re-classified as {classification} -> {new_status}")


# ============================================================================
# PERMANENT ERROR NOTIFICATION & FIX SUGGESTION FUNCTIONS
# ============================================================================

def _generate_ai_fix_suggestion(job_id, error_message, reasoning):
    """
    Generate a clear, actionable fix suggestion using AI.

    Returns a structured dict with:
    - root_cause: What went wrong
    - fix_steps: Numbered list of steps to fix
    - prevention: How to avoid this in the future
    """
    try:
        fix_suggestion = generate_fix_suggestion(
            error_text=error_message,
            spark=spark,
            job_id=job_id,
            reasoning=reasoning
        )
        return fix_suggestion
    except Exception as e:
        print(f"[FixGen] Failed to generate fix suggestion: {e}")
        return {
            "root_cause": reasoning,
            "fix_steps": ["Review the error message and Databricks job logs manually"],
            "prevention": "N/A"
        }


def _get_job_creator(job_id):
    """
    Get the creator/owner of a job for notification routing.
    Falls back to creator_user_name from job metadata.
    """
    try:
        client = get_workspace_client()
        job = client.jobs.get(job_id=int(job_id))
        return job.creator_user_name
    except Exception as e:
        print(f"[Notify] Could not get job creator: {e}")
        return "debashish8101@gmail.com"


def _send_permanent_error_notification(job_id, job_creator, error_message, ai_reasoning, fix_suggestion):
    """
    Send notification to job creator with clear separation between:
    1. Actual error (technical details)
    2. AI fix suggestion (actionable steps)

    For POC: prints formatted notification (replace with email/Slack in prod).
    """
    # WHY: Keep error message and fix suggestions clearly separated so the
    # recipient can see the technical details first, then the AI guidance.

    separator = "=" * 60

    # Section 1: Error Details
    print(f"\n{separator}")
    print("🔴 PERMANENT JOB FAILURE NOTIFICATION")
    print(separator)
    print(f"📋 Job ID: {job_id}")
    print(f"👤 Owner: {job_creator}")
    print(f"🕐 Detected: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"\n--- Actual Error (Technical Details) ---")
    print(error_message)

    # Section 2: AI Fix Suggestion
    print(f"\n{separator}")
    print("🤖 AI ANALYSIS & FIX SUGGESTION")
    print(separator)
    print(f"Root Cause: {fix_suggestion.get('root_cause', ai_reasoning)}")
    print(f"\nFix Steps:")
    for i, step in enumerate(fix_suggestion.get('fix_steps', []), 1):
        print(f"  {i}. {step}")
    print(f"\nPrevention: {fix_suggestion.get('prevention', 'N/A')}")

    print(f"\n{separator}")
    print("ℹ️  This job will NOT be retried automatically.")
    print(separator)
    print(f"[Notify] Sent to: {job_creator}")

    # Store notification in tracking table for audit
    # NOTE: Requires failure_notifications table (created in setup)
    try:
        fix_json = json.dumps(fix_suggestion)
        # Escape single quotes for SQL
        safe_error = error_message.replace("'", "\\'")
        safe_reasoning = ai_reasoning.replace("'", "\\'")
        safe_fix = fix_json.replace("'", "\\'")

        spark.sql(f"""
            INSERT INTO ai_classification_autoretry.poc.failure_notifications
            (job_id, notified_user, error_message, ai_reasoning, fix_suggestion, notification_time)
            VALUES (
                '{job_id}',
                '{job_creator}',
                '{safe_error}',
                '{safe_reasoning}',
                '{safe_fix}',
                current_timestamp()
            )
        """)
        print("[Notify] Stored in failure_notifications table")
    except Exception as e:
        print(f"[Notify] Could not store in table: {e}")


def _store_fix_suggestion(tracking_id, fix_suggestion):
    """
    Store the AI fix suggestion alongside the failure record for audit.
    """
    try:
        # Add column if it doesn't exist yet
        spark.sql(f"""
            ALTER TABLE ai_classification_autoretry.poc.failure_tracking
            ADD COLUMN IF NOT EXISTS fix_suggestion STRING
        """)

        fix_json = json.dumps(fix_suggestion).replace("'", "\\'")
        spark.sql(f"""
            UPDATE ai_classification_autoretry.poc.failure_tracking
            SET fix_suggestion = '{fix_json}'
            WHERE tracking_id = '{tracking_id}'
        """)
        print(f"[Store] Fix suggestion stored for tracking_id={tracking_id}")
    except Exception as e:
        print(f"[Store] Could not store fix suggestion: {e}")
