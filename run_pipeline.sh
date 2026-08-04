#!/usr/bin/env bash
#
# Nightly Ciena AS_Response pipeline: extract from SQL Server -> load Snowflake -> dbt build.
# This is what the scheduler (Windows Task Scheduler / cron) calls.
#
# Run manually to test:   bash run_pipeline.sh
#
set -euo pipefail

# Always operate from the repo root (this script lives there), so relative paths
# like ./landing and the .env files resolve correctly.
cd "$(dirname "$0")"

# Send all output to a dated log file so scheduled runs leave a trail.
mkdir -p logs
exec >> "logs/pipeline_$(date +%F).log" 2>&1

echo "==================================================================="
echo "Pipeline started: $(date)"

# Activate the project virtualenv (provides python, pyodbc, dbt, etc.).
source .venv/bin/activate

# Regenerate the mission-control dashboard on the way out -- success OR failure.
# A trap on EXIT runs even when `set -e` aborts the pipeline mid-step, so a failed
# night still shows up in dashboard.html.
trap 'python dashboard.py >/dev/null 2>&1 || true' EXIT

# Run as a module so the `etl` package imports resolve from the repo root.
python -m etl.run_ciena_as_response

echo "Pipeline finished OK: $(date)"
