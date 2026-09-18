# ---------------------------------------------------------------------------
# config.py
#
# Central configuration for the AI Classification & Auto-Retry POC.
# All tunable values (job IDs, retry limits, model name, etc.) live here,
# so logic files never hardcode these directly.
# ---------------------------------------------------------------------------

# List of job IDs this system will monitor for failures.
# For POC, this is our 4 dummy jobs. Later, this becomes the real pilot
# job list, and eventually could be replaced by a tag-based lookup.
# NOTE: these are JOB IDs (from the job-level URL), not run IDs.
PILOT_JOB_IDS = [
    "1097868525461869",  # dummy_job_success
    "640497011436482",   # dummy_job_transient_timeout
    "270740654024095",   # dummy_job_permanent_not_found
    "107833774383562",   # dummy_job_permanent_bad_code
]

# Catalog and schema where our tracking table lives.
TRACKING_CATALOG = "ai_classification_autoretry"
TRACKING_SCHEMA = "poc"
TRACKING_TABLE = "failure_tracking"

# Fully qualified name, built from the above - used in SQL queries.
TRACKING_TABLE_FULL_NAME = f"{TRACKING_CATALOG}.{TRACKING_SCHEMA}.{TRACKING_TABLE}"

# How far back (in hours) detection should look when scanning for failed
# runs. Acts as a safety net so we never process very old/stale failures,
# even if a job has a long run history.
DETECTION_LOOKBACK_HOURS = 24

# Max number of recent runs to fetch per job when checking for failures.
# Kept small for POC scope - our dummy jobs won't have many runs anyway.
RUNS_PER_JOB_LIMIT = 5

MAX_RETRY_ATTEMPTS = 3