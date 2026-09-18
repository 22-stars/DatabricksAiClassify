# Databricks notebook source
# ---------------------------------------------------------------------------
# Widget setup: allows this notebook to receive a "failure_type" parameter
# from the Databricks Job configuration. Default is "success" so that
# manual/ad-hoc runs without a parameter don't accidentally simulate failure.
# ---------------------------------------------------------------------------
dbutils.widgets.text("failure_type", "success")

# Read the parameter value passed in for this run
failure_type = dbutils.widgets.get("failure_type")

# Print it out so we can see in the job logs which mode this run used
print(f"Running dummy job with failure_type = {failure_type}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Branching logic: based on failure_type, simulate either a successful run
# or one of the defined failure scenarios by raising a realistic exception.
# The exact error message matters - this text is what the AI classifier
# will later read to decide Transient vs Permanent.
# ---------------------------------------------------------------------------

if failure_type == "success":
    # Simulate a normal successful job run - no error raised
    print("Job completed successfully. No errors.")

elif failure_type == "transient_timeout":
    # Simulate a transient issue - e.g. a network/API call timing out.
    # In real jobs, retrying later often resolves this kind of error.
    raise TimeoutError("Connection to external service timed out after 30 seconds. The service may be temporarily unavailable.")

elif failure_type == "permanent_not_found":
    # Simulate a permanent issue - a referenced file/table does not exist.
    # Retrying will not help since the root cause won't resolve on its own.
    raise FileNotFoundError("Table 'sales.transactions_2023' not found. Please verify the table exists and the path is correct.")

elif failure_type == "permanent_bad_code":
    # Simulate a permanent issue - a code bug (undefined variable reference).
    # Retrying will not help since this requires a code fix.
    raise NameError("name 'undefined_variable' is not defined")

else:
    # Safety net: if an unexpected/unsupported failure_type value is passed,
    # fail loudly rather than silently doing nothing - helps catch config
    # mistakes early instead of masking them.
    raise ValueError(f"Unknown failure_type '{failure_type}' provided. Expected one of: success, transient_timeout, permanent_not_found, permanent_bad_code.")