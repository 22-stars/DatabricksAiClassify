# =============================================================================
# Test: test_ai_classifier.py
# Purpose: Manually validates classify_error() against known error scenarios
# covering all 3 classification categories (Transient, Permanent, Unknown),
# using the test cases already validated in PROJECT_STATE.md Step 5 log
# (7/7 correct as of last manual validation).
# =============================================================================

# WHY: When running in Databricks, the spark variable is auto-injected.
# When running locally, this test needs spark - run from a notebook or
# configure SparkSession manually.
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

from src.classification.ai_classifier import classify_error

# WHY: Same 7 test errors already manually validated - reused here as a 
# repeatable regression test rather than one-off notebook testing.
test_cases = [
    ("TimeoutError: Connection to external service timed out after 30 seconds. The service may be temporarily unavailable.", "Transient"),
    ("NameError: name 'undefined_variable' is not defined", "Permanent"),
    ("Error: Something went wrong.", "Unknown"),
    ("Job failed with exit code 1", "Unknown"),
    ("PermissionDenied: User does not have SELECT privilege on table 'finance.transactions'", "Permanent"),
    ("AnalysisException: cannot resolve column 'customer_id' due to data type mismatch", "Permanent"),
    ("429 Too Many Requests: Rate limit exceeded, please retry after some time", "Transient"),
]

for error_text, expected in test_cases:
    result = classify_error(error_text, spark)
    actual = result["classification"]
    match = "PASS" if actual == expected else "FAIL"
    print(f"[{match}] Expected: {expected} | Got: {actual} | Parse Status: {result['parse_status']}")
    print(f"       Reasoning: {result['reasoning']}")
    print("-----")