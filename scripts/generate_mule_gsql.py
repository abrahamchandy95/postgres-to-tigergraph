"""Regenerate the temporal mule loading jobs and load-validation query."""

from tf_gnn_loader.mule.contract import GSQL
from tf_gnn_loader.mule.gsql import loading_jobs, verify_query

if __name__ == "__main__":
    (GSQL / "loading_jobs.gsql").write_text(loading_jobs())
    (GSQL / "verify_load.gsql").write_text(verify_query())
