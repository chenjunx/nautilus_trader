#!/usr/bin/env bash
set -euo pipefail

# === 工程目录：改成服务器上的 nautilus_trader 路径 ===
PROJECT_DIR="/home/admin/nautilus_trader"

# === RTDS 配置 ===
POLYMARKET_RTDS_WS_URL="wss://ws-live-data.polymarket.com"
POLYMARKET_RTDS_SYMBOLS="btcusd,ethusd,solusd"
POLYMARKET_RTDS_OUTPUT_DIR="/home/admin/data/polymarket/rtds"
POLYMARKET_RTDS_PING_INTERVAL_SECS="5"

# === 日志和 PID ===
LOG_DIR="/home/admin/data/polymarket/logs"
LOG_FILE="$LOG_DIR/rtds_btcusd.log"
PID_FILE="/home/admin/data/polymarket/run_polymarket_rtds_btcusd_collector.pid"

# 如果服务器需要代理，取消下面这行注释并修改地址。
# HTTPS_PROXY="http://127.0.0.1:7890"

mkdir -p "$LOG_DIR" "$POLYMARKET_RTDS_OUTPUT_DIR"

is_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

current_pid() {
  if [[ -f "$PID_FILE" ]]; then
    tr -d '[:space:]' < "$PID_FILE"
  fi
}

start() {
  local pid
  pid="$(current_pid)"
  if is_running "$pid"; then
    echo "Polymarket RTDS BTCUSD collector is already running."
    echo "  pid: $pid"
    echo "  log: $LOG_FILE"
    echo "  output: $POLYMARKET_RTDS_OUTPUT_DIR/crypto_prices"
    return 0
  fi

  cd "$PROJECT_DIR"

  export POLYMARKET_RTDS_WS_URL
  export POLYMARKET_RTDS_SYMBOLS
  export POLYMARKET_RTDS_OUTPUT_DIR
  export POLYMARKET_RTDS_PING_INTERVAL_SECS

  if [[ "${HTTPS_PROXY:-}" != "" ]]; then
    export HTTPS_PROXY
    export HTTP_PROXY="${HTTP_PROXY:-$HTTPS_PROXY}"
  fi

  echo "Starting Polymarket RTDS BTCUSD collector..."
  echo "  project: $PROJECT_DIR"
  echo "  ws_url: $POLYMARKET_RTDS_WS_URL"
  echo "  symbols: $POLYMARKET_RTDS_SYMBOLS"
  echo "  output: $POLYMARKET_RTDS_OUTPUT_DIR"
  echo "  log: $LOG_FILE"

  nohup uv run --no-sync python examples/live/polymarket/polymarket_rtds_crypto_price_collector.py \
    >> "$LOG_FILE" 2>&1 &

  pid="$!"
  echo "$pid" > "$PID_FILE"
  echo "Started."
  echo "  pid: $pid"
  echo "  tail log: tail -f $LOG_FILE"
}

stop() {
  local pid
  pid="$(current_pid)"
  if ! is_running "$pid"; then
    rm -f "$PID_FILE"
    echo "Polymarket RTDS BTCUSD collector is not running."
    return 0
  fi

  echo "Stopping Polymarket RTDS BTCUSD collector: $pid"
  kill "$pid" 2>/dev/null || true

  for _ in {1..30}; do
    if ! is_running "$pid"; then
      rm -f "$PID_FILE"
      echo "Stopped."
      return 0
    fi
    sleep 1
  done

  echo "Collector did not stop gracefully, killing: $pid"
  kill -KILL "$pid" 2>/dev/null || true
  rm -f "$PID_FILE"
}

status() {
  local pid
  pid="$(current_pid)"
  if is_running "$pid"; then
    echo "Polymarket RTDS BTCUSD collector is running."
    echo "  pid: $pid"
    echo "  log: $LOG_FILE"
    echo "  output: $POLYMARKET_RTDS_OUTPUT_DIR/crypto_prices"
  else
    echo "Polymarket RTDS BTCUSD collector is not running."
    [[ -f "$PID_FILE" ]] && echo "  stale pid file: $PID_FILE"
  fi
}

case "${1:-start}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac
