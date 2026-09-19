# =============================================================================
# Module: ai_classifier.py
# Purpose: Classifies a Databricks job failure error message as Transient, 
# Permanent, or Unknown using the Databricks Foundation Model API (ai_query).
#
# Used by: orchestrator (not yet built) - takes a raw error_text string and 
# returns a structured classification result.
# =============================================================================

import json

# -----------------------------------------------------------------------------
# WHY: Model name is defined as a constant (not hardcoded inline) so it can be 
# swapped later (e.g. to a larger model) without hunting through the function body.
# -----------------------------------------------------------------------------
MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

# -----------------------------------------------------------------------------
# WHY: Prompt is defined once at module level (not rebuilt per call) since it's 
# static except for the error_text appended at the end. Validated via manual 
# testing (7/7 correct across Transient/Permanent/Unknown categories) before 
# being locked in here - see PROJECT_STATE.md Step 5 log for validation history.
# -----------------------------------------------------------------------------
PROMPT_TEMPLATE = """You are an expert system that classifies Databricks job failure errors.

Classify the given error message into exactly one of these categories:
- "Transient": Temporary issues likely to resolve on their own if retried (e.g. network timeouts, temporary resource unavailability, rate limits, service throttling, temporary connection issues).
- "Permanent": Issues that will NOT resolve by simply retrying, and require a code fix, configuration change, or manual intervention (e.g. missing files/tables, invalid credentials, syntax errors, undefined variables, schema mismatches, permission errors).
- "Unknown": The error message does not contain enough SPECIFIC evidence to confidently determine the root cause. Use this whenever the message is generic, lacks a clear reason/cause, or could plausibly be explained by BOTH a transient and a permanent scenario. Do NOT guess - if you are inferring the cause rather than reading it directly from the error text, classify as Unknown instead.

Examples:
Error: "TimeoutError: Connection to external service timed out after 30 seconds."
Classification: Transient

Error: "FileNotFoundError: Table 'sales.transactions_2023' not found."
Classification: Permanent

Error: "NameError: name 'undefined_variable' is not defined"
Classification: Permanent

Error: "Job failed with exit code 1"
Classification: Unknown

Error: "Something went wrong."
Classification: Unknown

Now classify this error. Respond with ONLY valid JSON, no extra text, in exactly this format:
{"classification": "Transient|Permanent|Unknown", "reasoning": "one sentence explanation", "confidence": "High|Medium|Low"}

Error: """


def classify_error(error_text: str, spark) -> dict:
    """
    Classifies a single error message as Transient, Permanent, or Unknown.

    Args:
        error_text: The raw error message string to classify.
        spark: The active SparkSession (passed in rather than imported globally,
               so this function stays testable and doesn't assume a global 
               'spark' variable exists).

    Returns:
        A dict with keys: classification, reasoning, confidence, parse_status.
        parse_status is "OK" for a normal successful classification, or 
        "PARSE_FAILED" if the AI response could not be parsed as valid JSON 
        (in which case classification defaults to "Unknown" as a safe fallback).
    """

    # WHY: Build the full prompt by appending the actual error text to the 
    # static template - keeps the static instructions/examples separate from 
    # the dynamic input for clarity.
    full_prompt = PROMPT_TEMPLATE + error_text

    # WHY: Using parameterized args={"prompt": ...} instead of manual string 
    # formatting into the SQL query - avoids quote-escaping bugs when error 
    # text contains special characters (found during earlier prototyping).
    result = spark.sql(
        "SELECT ai_query(:model, :prompt) AS response",
        args={"model": MODEL_NAME, "prompt": full_prompt}
    ).collect()

    raw_response = result[0]["response"]

    # WHY: The AI is expected to return clean JSON, but we cannot fully trust 
    # an LLM's output format 100% of the time. Fallback ensures one malformed 
    # response never crashes the pipeline (Decision: Option C, see 
    # PROJECT_STATE.md Step 5 log for full reasoning).
    try:
        parsed = json.loads(raw_response)
        parsed["parse_status"] = "OK"
        return parsed

    except json.JSONDecodeError:
        return {
            "classification": "Unknown",
            "reasoning": "AI response could not be parsed",
            "confidence": "Low",
            "parse_status": "PARSE_FAILED"
        }


# -----------------------------------------------------------------------------
# WHY: Separate prompt for generating fix suggestions - keeps classification
# logic clean while allowing AI to provide actionable fix steps when needed.
# -----------------------------------------------------------------------------
FIX_SUGGESTION_PROMPT = """You are a senior Databricks engineer. A job just failed permanently.

JOB ID: {job_id}
ERROR MESSAGE: {error_message}
AI CLASSIFICATION REASONING: {ai_reasoning}

Please provide a fix suggestion in this exact JSON format:
{{
    "root_cause": "<one sentence explaining the root cause>",
    "fix_steps": ["step 1", "step 2", "step 3"],
    "prevention": "<one sentence on how to prevent this type of failure>"
}}

Be specific and actionable. Do not include any other text.
"""


def generate_fix_suggestion(error_text: str, spark, job_id=None, reasoning=None) -> dict:
    """
    Generate a structured fix suggestion for permanent errors.

    Args:
        error_text: The raw error message string.
        spark: The active SparkSession.
        job_id: Optional job ID for context.
        reasoning: Optional AI classification reasoning to include.

    Returns:
        A dict with keys: root_cause, fix_steps, prevention.
    """
    job_id_str = job_id or "unknown"
    reasoning_str = reasoning or ""

    full_prompt = FIX_SUGGESTION_PROMPT.format(
        job_id=job_id_str,
        error_message=error_text,
        ai_reasoning=reasoning_str
    )

    result = spark.sql(
        "SELECT ai_query(:model, :prompt) AS response",
        args={"model": MODEL_NAME, "prompt": full_prompt}
    ).collect()

    raw_response = result[0]["response"]

    try:
        parsed = json.loads(raw_response)
        return {
            "root_cause": parsed.get("root_cause", "No root cause provided"),
            "fix_steps": parsed.get("fix_steps", ["Review error message manually"]),
            "prevention": parsed.get("prevention", "N/A")
        }
    except json.JSONDecodeError:
        return {
            "root_cause": "Fix suggestion generation failed",
            "fix_steps": [reasoning_str if reasoning_str else "Review error message manually"],
            "prevention": "N/A"
        }