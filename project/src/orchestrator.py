# ---------------------------------------------------------------------------
# orchestrator.py
#
# Ties together detection, classification, and tracking into the main
# pipeline. Split into 3 independent functions since each runs on a
# different timing cycle (detect now, retry later, check outcome even later).
# ---------------------------------------------------------------------------
import sys

PROJECT_ROOT = "/Workspace/Users/debashish8101@gmail.com/databricks-ai-classification-autoretry"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from databricks.sdk.service.jobs import RunResultState, RunLifeCycleState

from config.config import MAX_RETRY_ATTEMPTS
from src.detection.failure_detector import detect_new_failures
from src.classification.ai_classifier import classify_error
from src.tracking.tracker import (
    insert_new_failure,
    update_classification,
    get_rows_by_status,
    mark_as_retried,
    mark_as_resolved,
    mark_as_max_retries_reached,
)
from src.utils.databricks_client import get_workspace_client

spark = SparkSession.builder.getOrCreate()

RETRY_DELAY_MINUTES = 15


def detect_classify_decide():
    """
    Step 1 of pipeline: finds new failures, classifies each with AI,
    inserts a tracking row, and decides the action (retry_scheduled /
    permanent_no_action / unknown_flagged).
    """
    new_failures = detect_new_failures()
    print(f"Detected {len(new_failures)} new failure(s)")

    for failure in new_failures:
        # WHY: insert immediately with status 'detected' first, so we have
        # a durable record even if classification fails for some reason
        tracking_id = insert_new_failure(
            job_id=failure["job_id"],
            run_id=failure["run_id"],
            error_message=failure["error_message"]
        )

        result = classify_error(failure["error_message"], spark)
        classification = result["classification"]

        # WHY: map classification -> tracking status, per agreed action mapping
        if classification == "Transient":
            new_status = "retry_scheduled"
        elif classification == "Permanent":
            new_status = "permanent_no_action"
        else:  # Unknown
            new_status = "unknown_flagged"

        update_classification(tracking_id, classification, new_status)

        print(f"  - job_id={failure['job_id']} run_id={failure['run_id']} "
              f"-> {classification} -> {new_status}")


def execute_due_retries():
    """
    Step 2 of pipeline: finds failures with status 'retry_scheduled' whose
    delay has elapsed, checks safety conditions (newer run already succeeded /
    concurrent run in progress), then triggers a retry and updates tracking.
    """
    client = get_workspace_client()
    scheduled_rows = get_rows_by_status("retry_scheduled")

    print(f"Found {len(scheduled_rows)} row(s) with status 'retry_scheduled'")

    for row in scheduled_rows:
        # WHY: only act if enough time has passed since classification -
        # simulates the 15-min wait before retrying
        elapsed = datetime.utcnow() - row["last_updated_at"]
        if elapsed < timedelta(minutes=RETRY_DELAY_MINUTES):
            print(f"  - tracking_id={row['tracking_id']} not due yet ({elapsed} elapsed)")
            continue

        job_id = int(row["job_id"])
        recent_runs = list(client.jobs.list_runs(job_id=job_id, limit=10))
        first_failed_millis = int(row["first_failed_at"].timestamp() * 1000)

        # WHY: check if ANY newer run (regardless of trigger type - manual,
        # scheduled, etc.) already succeeded since this failure - avoids a
        # redundant retry if someone already fixed/reran it another way
        already_succeeded = any(
            r.state.result_state == RunResultState.SUCCESS
            and r.start_time > first_failed_millis
            for r in recent_runs
        )
        if already_succeeded:
            mark_as_resolved(row["tracking_id"])
            print(f"  - tracking_id={row['tracking_id']} -> resolved "
                  f"(newer run already succeeded, skipped retry)")
            continue

        # WHY: check no run of this job is currently in progress, to avoid
        # triggering a duplicate/concurrent run
        in_progress = any(
            r.state.life_cycle_state in (
                RunLifeCycleState.RUNNING,
                RunLifeCycleState.PENDING,
            )
            for r in recent_runs
        )
        if in_progress:
            print(f"  - tracking_id={row['tracking_id']} skipped "
                  f"(another run currently in progress)")
            continue

        resp = client.jobs.run_now(job_id=job_id)
        new_run_id = str(resp.run_id)

        mark_as_retried(row["tracking_id"], new_run_id)

        print(f"  - tracking_id={row['tracking_id']} retried -> new_run_id={new_run_id}")


def check_retry_outcomes():
    """
    Step 3 of pipeline: checks failures with status 'retried' to see if the
    latest run succeeded (-> resolved), or failed again (-> re-classify and
    decide, respecting MAX_RETRY_ATTEMPTS -> max_retries_reached if exhausted).
    """
    client = get_workspace_client()
    retried_rows = get_rows_by_status("retried")

    print(f"Found {len(retried_rows)} row(s) with status 'retried'")

    for row in retried_rows:
        run = client.jobs.get_run(run_id=int(row["latest_run_id"]))

        # WHY: only act once the run has actually finished - skip if still running
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
                # WHY: re-fetch error message and re-classify, then decide
                # again (same logic as detect_classify_decide) - loops back
                # into the retry cycle if still transient
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
                print(f"  - tracking_id={row['tracking_id']} failed again -> "
                      f"re-classified as {classification} -> {new_status}")