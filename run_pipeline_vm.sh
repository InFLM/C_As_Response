#!/usr/bin/env bash
#
# VM entrypoint for the Ciena ETL on the CentOS 7.9 Azure VM.
#
# Differs from run_pipeline.sh in one way: the VM's Python comes from a Miniconda
# environment named "etl" (CentOS 7's system Python 3.6 is too old for dbt 1.8 / pandas 2.2),
# not from a local .venv. Everything else -- dated logging, dashboard refresh on exit -- is
# the same. cron calls THIS script on the VM.
#
# Test manually:   bash run_pipeline_vm.sh
set -euo pipefail

# Operate from the repo root (this script lives there) so ./landing, ./logs, .env resolve.
cd "$(dirname "$0")"

# Dated log file so every run leaves a trail (and dashboard.py can read them).
mkdir -p logs
exec >> "logs/pipeline_$(date +%F).log" 2>&1

echo "==================================================================="
echo "Pipeline started: $(date)"

# Activate the Miniconda env that has Python 3.11 + the project deps.
# Adjust this path if you installed Miniconda somewhere other than $HOME/miniconda3.
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate etl

# Regenerate the mission-control dashboard on the way out -- success OR failure.
trap 'python dashboard.py >/dev/null 2>&1 || true' EXIT

# Run the chunked extract -> Snowflake load -> dbt build.
python -m etl.run_ciena_as_response

echo "Pipeline finished OK: $(date)"

# --- OPTIONAL: self-deallocate when done (cheapest stop; see AZURE_VM_SETUP.md step 6) ---
# Requires: VM system-assigned managed identity with "Virtual Machine Contributor" on itself,
# and azure-cli installed in the conda env (pip install azure-cli). Uncomment to enable.
#
# az login --identity --output none
# RID=$(curl -s -H Metadata:true \
#   "http://169.254.169.254/metadata/instance/compute/resourceId?api-version=2021-02-01&format=text")
# echo "Deallocating self: $RID"
# az vm deallocate --ids "$RID" --no-wait
