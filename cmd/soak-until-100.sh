#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

: "${SERVER_DIR:?Set SERVER_DIR to the Socket.IO server project directory}"

SERVER_URL="${SERVER_URL:-http://localhost:8083}"
PING_URL="${PING_URL:-${SERVER_URL%/}/v2/ping}"
CLIENT_LOG="${SOAK_CLIENT_LOG:-${TMPDIR:-/tmp}/go-socket-io-testclient.log}"
SERVER_LOG="${SOAK_SERVER_LOG:-${TMPDIR:-/tmp}/go-socket-io-server-churn.log}"
REPORTS_FILE="${SOAK_REPORTS_FILE:-${TMPDIR:-/tmp}/go-socket-io-reports.txt}"
TARGET_RECONNECTS="${TARGET_RECONNECTS:-100}"
CHURN_INTERVAL="${CHURN_INTERVAL:-30}"

SERVER_PID=""
CLIENT_PID=""

log() {
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

get_reconnects() {
	grep "Reconnects:" "$CLIENT_LOG" 2>/dev/null | tail -1 | sed 's/.*Reconnects: //' | tr -d '[:space:]' || echo "0"
}

stop_server() {
	if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
	fi

	SERVER_PID=""

	if command -v lsof >/dev/null 2>&1; then
		lsof -ti "$(echo "$SERVER_URL" | sed -E 's#.*:([0-9]+).*#\1#')" 2>/dev/null | xargs kill 2>/dev/null || true
	fi
}

start_server() {
	stop_server
	sleep 1

	cd "$SERVER_DIR"
	npm run start >>"$SERVER_LOG" 2>&1 &
	SERVER_PID=$!

	for _ in $(seq 1 30); do
		if curl -sf "$PING_URL" >/dev/null 2>&1; then
			return 0
		fi

		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			return 1
		fi

		sleep 1
	done

	return 1
}

stop_client() {
	if [[ -n "${CLIENT_PID:-}" ]] && kill -0 "$CLIENT_PID" 2>/dev/null; then
		kill "$CLIENT_PID" 2>/dev/null || true
		wait "$CLIENT_PID" 2>/dev/null || true
	fi

	pkill -f "go run ./cmd/testclient" 2>/dev/null || true
	CLIENT_PID=""
}

start_client() {
	stop_client
	: >"$CLIENT_LOG"

	cd "$REPO_ROOT"
	go run ./cmd/testclient -url "$SERVER_URL" 2>&1 | tee "$CLIENT_LOG" &
	CLIENT_PID=$!

	sleep 3
}

print_report_samples() {
	awk '
		/^\[REPORT\]/ {
			if (block != "") {
				reports[++n] = block
			}
			block = $0 ORS
			sep = 0
			next
		}
		block != "" {
			block = block $0 ORS
			if ($0 == "========================================") {
				sep++
				if (sep == 2) {
					reports[++n] = block
					block = ""
					sep = 0
				}
			}
		}
		END {
			if (block != "") {
				reports[++n] = block
			}

			for (i = 1; i <= n; i++) {
				print reports[i]
			}
		}
	' "$CLIENT_LOG" >"$REPORTS_FILE"

	local total
	total=$(grep -c '^\[REPORT\]' "$REPORTS_FILE" || true)

	log "Total reports captured: $total"

	echo ""
	echo "========== PRIMEIROS 3 REPORTS (completos) =========="
	awk '
		/^\[REPORT\]/ { idx++ }
		idx >= 1 && idx <= 3 { print }
	' "$REPORTS_FILE"

	echo ""
	echo "========== ÚLTIMOS 3 REPORTS (completos) =========="
	if [[ "$total" -le 3 ]]; then
		cat "$REPORTS_FILE"
	else
		awk -v total="$total" '
			/^\[REPORT\]/ { idx++ }
			idx >= total - 2 { print }
		' "$REPORTS_FILE"
	fi
}

cleanup() {
	stop_client
	stop_server
}

trap cleanup EXIT

log "Starting client..."
start_client

log "Churning server until reconnects >= $TARGET_RECONNECTS..."

while true; do
	reconnects=$(get_reconnects)

	if [[ -n "$reconnects" && "$reconnects" -ge "$TARGET_RECONNECTS" ]]; then
		log "Target reached: reconnects=$reconnects"
		break
	fi

	start_server || true
	sleep "$CHURN_INTERVAL"
	stop_server

	reconnects=$(get_reconnects)
	log "reconnects=$reconnects"
done

stop_client
stop_server
trap - EXIT

log "Done."
print_report_samples
