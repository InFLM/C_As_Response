#!/usr/bin/env bash
#
# Quick health check of the most recent pipeline run.
#   bash check_status.sh            # latest log
#   bash check_status.sh 2026-06-17 # a specific day's log
#
cd "$(dirname "$0")"

if [ "${1:-}" ]; then
    log="logs/pipeline_${1}.log"
else
    log="$(ls -t logs/pipeline_*.log 2>/dev/null | head -1)"
fi

if [ -z "${log:-}" ] || [ ! -f "$log" ]; then
    echo "No pipeline log found (looked in ./logs)."
    exit 1
fi

echo "Log file:     $log"
grep -m1 "Pipeline started"  "$log" | sed 's/^/Started:      /' || true
echo    "Rows:         $(grep -m1 'Extract complete' "$log" | grep -oE '[0-9,]+ rows' || echo 'n/a')"
echo    "Snowflake:    $(grep -q 'Snowflake load complete' "$log" && echo 'loaded' || echo 'NOT loaded')"
# dbt's own summary line, e.g. "Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5"
echo    "dbt:          $(grep -m1 'Done. PASS=' "$log" | sed 's/^[0-9:]* *//' || echo 'did not run')"

echo "-------------------------------------------------------------------"
if grep -q "Pipeline finished OK" "$log"; then
    echo "RESULT:       SUCCESS"
else
    echo "RESULT:       FAILED  (showing last 15 lines below)"
    echo "-------------------------------------------------------------------"
    tail -15 "$log"
fi
