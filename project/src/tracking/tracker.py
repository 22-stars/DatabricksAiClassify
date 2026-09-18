# ---------------------------------------------------------------------------
# TEMPORARY WORKAROUND (remove once project is connected to a Git folder):
# Manually telling Python where our project root is, so it can find the
# "config" folder.
# ---------------------------------------------------------------------------
import sys

PROJECT_ROOT = "/Workspace/Users/debashish8101@gmail.com/databricks-ai-classification-autoretry"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# ---------------------------------------------------------------------------
# tracker.py
#
# Read/write helpers for the failure_tracking Delta table.
#
# NOTE: Plain .py files (unlike notebooks) don't automatically get the
# "spark" variable injected - we must explicitly get the active Spark
# session ourselves.
# ---------------------------------------------------------------------------
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from config.config import TRACKING_TABLE_FULL_NAME

spark = SparkSession.builder.getOrCreate()


def get_known_run_ids() -> set:
    """
    Returns a set of all run_ids already tracked, across every failure
    chain's full retry history (not just current original/latest snapshot).

    WHY: Originally this only checked original_run_id and latest_run_id,
    but latest_run_id gets OVERWRITTEN on every retry - causing old
    intermediate run_ids to be "forgotten" and re-detected as new failures.
    Fixed by tracking full history in all_run_ids and checking membership
    across that instead.
    """
    rows = spark.sql(f"""
        SELECT explode(all_run_ids) AS run_id FROM {TRACKING_TABLE_FULL_NAME}
    """).collect()

    known_ids = {row["run_id"] for row in rows}
    return known_ids


def insert_new_failure(job_id: str, run_id: str, error_message: str) -> str:
    """
    Inserts a new failure record with status 'detected'. Returns the
    generated tracking_id.
    """
    tracking_id = str(uuid.uuid4())
    now = datetime.utcnow()

    spark.sql(f"""
        INSERT INTO {TRACKING_TABLE_FULL_NAME}
        (tracking_id, job_id, original_run_id, latest_run_id, error_message,
         classification, retry_count, status, first_failed_at, last_updated_at,
         all_run_ids)
        VALUES (:tracking_id, :job_id, :run_id, :run_id, :error_message,
                NULL, 0, 'detected', :now, :now, array(:run_id))
    """, args={"tracking_id": tracking_id, "job_id": job_id, "run_id": run_id,
               "error_message": error_message, "now": now})

    return tracking_id


def update_classification(tracking_id: str, classification: str, new_status: str):
    """Updates classification + status after AI classification decision."""
    spark.sql(f"""
        UPDATE {TRACKING_TABLE_FULL_NAME}
        SET classification = :classification, status = :status, last_updated_at = :now
        WHERE tracking_id = :tracking_id
    """, args={"classification": classification, "status": new_status,
               "now": datetime.utcnow(), "tracking_id": tracking_id})


def get_rows_by_status(status: str) -> list[dict]:
    """Returns all tracking rows matching the given status, as list of dicts."""
    rows = spark.sql(f"""
        SELECT * FROM {TRACKING_TABLE_FULL_NAME} WHERE status = :status
    """, args={"status": status}).collect()
    return [row.asDict() for row in rows]


def mark_as_retried(tracking_id: str, new_run_id: str):
    """
    After triggering a retry: appends new_run_id to all_run_ids, updates
    latest_run_id, increments retry_count, status -> 'retried'.
    """
    spark.sql(f"""
        UPDATE {TRACKING_TABLE_FULL_NAME}
        SET latest_run_id = :new_run_id,
            all_run_ids = array_union(all_run_ids, array(:new_run_id)),
            retry_count = retry_count + 1,
            status = 'retried', last_updated_at = :now
        WHERE tracking_id = :tracking_id
    """, args={"new_run_id": new_run_id, "now": datetime.utcnow(), "tracking_id": tracking_id})


def mark_as_resolved(tracking_id: str):
    """Marks a failure as resolved (a newer run succeeded)."""
    spark.sql(f"""
        UPDATE {TRACKING_TABLE_FULL_NAME}
        SET status = 'resolved', last_updated_at = :now
        WHERE tracking_id = :tracking_id
    """, args={"now": datetime.utcnow(), "tracking_id": tracking_id})


def mark_as_max_retries_reached(tracking_id: str):
    """Marks a failure as having exhausted its retry attempts."""
    spark.sql(f"""
        UPDATE {TRACKING_TABLE_FULL_NAME}
        SET status = 'max_retries_reached', last_updated_at = :now
        WHERE tracking_id = :tracking_id
    """, args={"now": datetime.utcnow(), "tracking_id": tracking_id})