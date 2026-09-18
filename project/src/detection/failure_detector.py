# ---------------------------------------------------------------------------
# failure_detector.py
#
# Scans our pilot job list for recently failed runs, skipping any run_id
# already known to our tracking table, and returns a clean list of newly
# detected failures with their error messages - ready to be classified.
# ---------------------------------------------------------------------------
import time

from databricks.sdk.service.jobs import RunResultState

from config.config import (
    PILOT_JOB_IDS,
    DETECTION_LOOKBACK_HOURS,
    RUNS_PER_JOB_LIMIT,
)
from src.utils.databricks_client import get_workspace_client
from src.tracking.tracker import get_known_run_ids


def detect_new_failures() -> list[dict]:
    """
    Checks all pilot jobs for recently failed runs that we haven't already
    recorded in the tracking table.

    Returns a list of dicts, one per new failure, shaped as:
        {
            "job_id": str,
            "run_id": str,
            "error_message": str,
            "start_time": int (epoch millis)
        }
    """
    client = get_workspace_client()

    # Run_ids we've already processed - used to skip duplicates
    known_run_ids = get_known_run_ids()

    # Cutoff timestamp (epoch millis) - ignore runs older than this,
    # acting as a safety net against stale/old failures.
    cutoff_time_millis = int(time.time() * 1000) - (DETECTION_LOOKBACK_HOURS * 60 * 60 * 1000)

    new_failures = []

    # Check each pilot job individually
    for job_id in PILOT_JOB_IDS:
        runs = client.jobs.list_runs(job_id=job_id, limit=RUNS_PER_JOB_LIMIT)

        for run in runs:
            run_id_str = str(run.run_id)

            # Skip runs we've already recorded
            if run_id_str in known_run_ids:
                continue

            # Skip runs older than our lookback window
            if run.start_time < cutoff_time_millis:
                continue

            # Only interested in runs that actually failed
            if run.state.result_state != RunResultState.FAILED:
                continue

            # list_runs() does not include task details by default, so we
            # fetch the full run details separately to get the task list.
            full_run = client.jobs.get_run(run_id=run.run_id)

            # Fetch the real error message from the first task's run output
            # (our pilot jobs are single-task, so this is sufficient for now)
            error_message = "No error message available"
            if full_run.tasks:
                task_run_id = full_run.tasks[0].run_id
                output = client.jobs.get_run_output(run_id=task_run_id)
                if output.error:
                    error_message = output.error

            new_failures.append({
                "job_id": job_id,
                "run_id": run_id_str,
                "error_message": error_message,
                "start_time": run.start_time,
            })

    return new_failures