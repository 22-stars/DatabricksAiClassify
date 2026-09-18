# ---------------------------------------------------------------------------
# databricks_client.py
#
# Shared helper that provides an authenticated Databricks SDK client.
# Uses implicit authentication - when running inside a Databricks notebook
# or job, WorkspaceClient() automatically picks up the current context's
# credentials, so no manual PAT/secret scope setup is needed for this POC.
#
# Every other module (detection, retry, etc.) should get its client through
# this single function, so that if we ever need to change HOW we authenticate
# (e.g. switch to PAT-based auth for running outside Databricks), we only
# need to update this one file.
# ---------------------------------------------------------------------------

from databricks.sdk import WorkspaceClient


def get_workspace_client() -> WorkspaceClient:
    """
    Returns an authenticated WorkspaceClient instance.
    Currently uses implicit notebook/job context authentication.
    """
    return WorkspaceClient()