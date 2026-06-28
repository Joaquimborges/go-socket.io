#!/usr/bin/env bash
set -euo pipefail

: "${SERVER_DIR:?Set SERVER_DIR to the Socket.IO server project directory}"

SERVER_URL="${SERVER_URL:-http://localhost:8083}"
PING_URL="${PING_URL:-${SERVER_URL%/}/v2/ping}"
LOG_FILE="${SOAK_SERVER_LOG:-${TMPDIR:-/tmp}/go-socket-io-server-churn.log}"
CHURN_INTERVAL="${CHURN_INTERVAL:-30}"
CHURN_DURATION="${CHURN_DURATION:-600}"
STABLE_DURATION="${STABLE_DURATION:-1800}"

SERVER_PID=""

log() {
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

stop_server() {
	if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
		log "SERVER STOPPED pid=$SERVER_PID"
	fi

	SERVER_PID=""

	if command -v lsof >/dev/null 2>&1; then
		lsof -ti "$(echo "$SERVER_URL" | sed -E 's#.*:([0-9]+).*#\1#')" 2>/dev/null | xargs kill 2>/dev/null || true
		sleep 1
	fi
}

start_server() {
	stop_server

	cd "$SERVER_DIR"
	npm run start >>"$LOG_FILE" 2>&1 &
	SERVER_PID=$!

	log "SERVER STARTED pid=$SERVER_PID"

	for _ in $(seq 1 30); do
		if curl -sf "$PING_URL" >/dev/null 2>&1; then
			log "SERVER READY at $SERVER_URL"
			return 0
		fi

		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			log "SERVER EXITED unexpectedly during startup"
			return 1
		fi

		sleep 1
	done

	log "SERVER START TIMEOUT ($PING_URL not ready)"
	return 1
}

: >"$LOG_FILE"
log "=== PHASE 1: churn every ${CHURN_INTERVAL}s for ${CHURN_DURATION}s ==="

phase1_end=$((SECONDS + CHURN_DURATION))
cycle=0

while (( SECONDS < phase1_end )); do
	cycle=$((cycle + 1))
	log "--- churn cycle $cycle: up for ${CHURN_INTERVAL}s ---"
	start_server || true
	sleep "$CHURN_INTERVAL"
	stop_server
done

log "=== PHASE 2: stable for ${STABLE_DURATION}s ==="
start_server || true
sleep "$STABLE_DURATION"
stop_server

log "=== DONE ==="
