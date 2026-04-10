#!/usr/bin/env bash
set -euo pipefail

run_service() {
    local svc=$1
    echo "=== ${svc^^} ==="
    docker compose logs -f "$svc" &
    LOG_PID=$!
    docker wait "$svc" > /dev/null
    kill "$LOG_PID" 2>/dev/null || true
}

startTime=$(date +%s)
echo "Starting pipeline..."
docker compose up -d
run_service generator
run_service processor
run_service validator
endTime=$(date +%s)
duration=$((endTime - startTime))
echo "Pipeline completed in $duration seconds."
