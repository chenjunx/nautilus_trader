#!/usr/bin/env bash
set -euo pipefail

# === 工程目录：改成你服务器上的 nautilus_trader 路径 ===
PROJECT_DIR="/home/admin/nautilus_trader"

# === 总输出目录 ===
DATA_ROOT="/home/admin/data/polymarket"
LOG_DIR="$DATA_ROOT/logs"
PID_FILE="$DATA_ROOT/run_polymarket_collectors.pid"
RUNNER_LOG="$LOG_DIR/runner.log"

# === 可选配置 ===
FLUSH_INTERVAL_SECS="5"
MAX_BUFFER_SIZE="10000"

# === 动态 ETH/SOL 15m 和 1h Up 市场滚动维护 ===
ENABLE_DYNAMIC_MARKETS="${ENABLE_DYNAMIC_MARKETS:-1}"
DYNAMIC_15M_MARKET_PREFIXES="${DYNAMIC_15M_MARKET_PREFIXES:-eth sol}"
DYNAMIC_15M_LOOKAHEAD_MARKETS="${DYNAMIC_15M_LOOKAHEAD_MARKETS:-4}"
DYNAMIC_1H_MARKET_PREFIXES="${DYNAMIC_1H_MARKET_PREFIXES:-eth sol}"
DYNAMIC_1H_MARKET_COUNT="${DYNAMIC_1H_MARKET_COUNT:-2}"
DYNAMIC_REFRESH_SECS="${DYNAMIC_REFRESH_SECS:-60}"
DYNAMIC_STOP_GRACE_SECS="${DYNAMIC_STOP_GRACE_SECS:-300}"
GAMMA_API_BASE_URL="${GAMMA_API_BASE_URL:-https://gamma-api.polymarket.com}"

# 如果服务器需要代理，取消下面这行注释并修改地址
# PROXY_URL="http://127.0.0.1:7890"

# === Polymarket 凭证配置 ===
# 默认从 ~/.polymarket.env 加载，也可以通过 POLYMARKET_SECRETS_FILE 指定其他路径。
# 文件内容示例：
#   POLYMARKET_PK="your_private_key"
#   POLYMARKET_FUNDER="your_funder_address"
#   POLYMARKET_API_KEY="your_api_key"
#   POLYMARKET_API_SECRET="your_api_secret"
#   POLYMARKET_PASSPHRASE="your_passphrase"
SECRETS_FILE="${POLYMARKET_SECRETS_FILE:-$HOME/.polymarket.env}"

# === 多市场配置 ===
# 格式：
# "name|condition_id|token_id"
#
# name 只用于目录名和日志名，建议只用英文、数字、下划线、短横线。
MARKETS=()
# 固定市场暂时停用，只保留动态 ETH 15m Up 市场。
# 如需恢复固定市场，把下面条目移回 MARKETS 数组。
# "market1|0xb106a3c9d1c59ed8117493dae6459a3ff79369a8f7cddaf62f4a05828b89195e|49084048476353771153083262278964615095037480324537117350127265918750912547378"
# "US_x_Iran_may_31|0x0e4a0c937b8934c2475613b6322b3f8edc8dedc24762e01e42b0e6f87424a089|72094069823942324362885404801938332659316240217382754851102758232469673300092"
# "Scotland_wcup|0xf950740bc71136155d6525cc0528a582c81f88812bff227803190c32ca25f54d|105252206997885252352889070218074909957179496257006510170583432513037465278006"
# "Congo_wcp|0xcd836ec4d94b8a4ddc5713d80fe9db245d2fb4796eaf12337974da1b4e96100d|87403333427856945144645806003352057704193778078820484282942507058200689996202"
# "Ivory_wcp|0x289568d555ec620ed6fa33c936c5f42649d3a2e30748a1daf7079f42453fbea4|58374167250364215964582274356498746399676421878376948523944979542572589542202"

mkdir -p "$LOG_DIR"

is_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

current_pid() {
  if [[ -f "$PID_FILE" ]]; then
    tr -d '[:space:]' < "$PID_FILE"
  fi
}

is_process_group_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 -- "-$pid" 2>/dev/null
}

terminate_process_group() {
  local pid="${1:-}"
  [[ -z "$pid" ]] && return 0

  if is_process_group_running "$pid"; then
    kill -- "-$pid" 2>/dev/null || true
  elif is_running "$pid"; then
    kill "$pid" 2>/dev/null || true
  fi
}

force_kill_process_group() {
  local pid="${1:-}"
  [[ -z "$pid" ]] && return 0

  if is_process_group_running "$pid"; then
    kill -KILL -- "-$pid" 2>/dev/null || true
  elif is_running "$pid"; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
}

cleanup_residual_collectors() {
  pkill -TERM -u "$(id -u)" -f "examples/live/polymarket/polymarket_orderbook_trade_collector.py" 2>/dev/null || true
  sleep 2
  pkill -KILL -u "$(id -u)" -f "examples/live/polymarket/polymarket_orderbook_trade_collector.py" 2>/dev/null || true
}

status() {
  local pid
  pid="$(current_pid)"

  if is_running "$pid"; then
    echo "Polymarket collectors are running."
    echo "  supervisor pid: $pid"
    echo "  runner log:     $RUNNER_LOG"
    echo "  market logs:    $LOG_DIR/<market_name>.log"
  else
    echo "Polymarket collectors are not running."
    [[ -f "$PID_FILE" ]] && echo "  stale pid file: $PID_FILE"
  fi
}

stop_existing() {
  local pid
  pid="$(current_pid)"

  if ! is_running "$pid"; then
    cleanup_residual_collectors
    rm -f "$PID_FILE"
    return 0
  fi

  echo "Stopping existing Polymarket collectors supervisor: $pid"
  terminate_process_group "$pid"

  for _ in {1..30}; do
    if ! is_running "$pid" && ! is_process_group_running "$pid"; then
      cleanup_residual_collectors
      rm -f "$PID_FILE"
      echo "Stopped."
      return 0
    fi
    sleep 1
  done

  echo "Supervisor did not stop gracefully, killing: $pid"
  force_kill_process_group "$pid"
  cleanup_residual_collectors
  rm -f "$PID_FILE"
}

start_background() {
  stop_existing

  echo "Starting Polymarket collectors in background..."
  nohup setsid "$0" foreground >> "$RUNNER_LOG" 2>&1 &
  local pid="$!"
  echo "$pid" > "$PID_FILE"

  echo "Started."
  echo "  supervisor pid: $pid"
  echo "  runner log:     $RUNNER_LOG"
  echo "  market logs:    $LOG_DIR/<market_name>.log"
  echo "  status:         $0 status"
  echo "  stop:           $0 stop"
}

load_secrets() {
  if [[ -f "$SECRETS_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
    set +a
    echo "Loaded Polymarket credentials from $SECRETS_FILE"
  else
    echo "Polymarket credentials file not found: $SECRETS_FILE"
    echo "Create it with POLYMARKET_PK, POLYMARKET_FUNDER, POLYMARKET_API_KEY, POLYMARKET_API_SECRET and POLYMARKET_PASSPHRASE if needed."
  fi
}

pids=()
declare -A dynamic_pids=()
declare -A dynamic_end_epochs=()

cleanup() {
  echo
  echo "Stopping collectors..."
  for pid in "${pids[@]}"; do
    terminate_process_group "$pid"
  done

  for _ in {1..10}; do
    local any_running=0
    for pid in "${pids[@]}"; do
      if is_running "$pid" || is_process_group_running "$pid"; then
        any_running=1
      fi
    done

    if (( any_running == 0 )); then
      break
    fi
    sleep 1
  done

  for pid in "${pids[@]}"; do
    force_kill_process_group "$pid"
  done

  cleanup_residual_collectors
  wait || true
  rm -f "$PID_FILE"
  echo "Stopped."
}

start_collector() {
  local name="$1"
  local condition_id="$2"
  local token_id="$3"

  local catalog_path="$DATA_ROOT/$name/catalog"
  local log_file="$LOG_DIR/$name.log"

  mkdir -p "$catalog_path"

  echo "Starting collector: $name"
  echo "  condition_id: $condition_id"
  echo "  token_id:      $token_id"
  echo "  catalog:       $catalog_path"
  echo "  log:           $log_file"

  (
    export POLYMARKET_CONDITION_ID="$condition_id"
    export POLYMARKET_TOKEN_ID="$token_id"
    export POLYMARKET_CATALOG_PATH="$catalog_path"
    export POLYMARKET_FLUSH_INTERVAL_SECS="$FLUSH_INTERVAL_SECS"
    export POLYMARKET_MAX_BUFFER_SIZE="$MAX_BUFFER_SIZE"

    if [[ "${PROXY_URL:-}" != "" ]]; then
      export POLYMARKET_PROXY_URL="$PROXY_URL"
    fi

    exec setsid uv run --no-sync python examples/live/polymarket/polymarket_orderbook_trade_collector.py
  ) > "$log_file" 2>&1 &

  last_collector_pid="$!"
  pids+=("$last_collector_pid")
  echo "  pid:           $last_collector_pid"
  echo
}

discover_dynamic_up_markets() {
  local timeframe="$1"
  local interval_secs="$2"
  local market_number="$3"
  local market_prefixes="$4"
  local mode="$5"

  GAMMA_API_BASE_URL="$GAMMA_API_BASE_URL" DYNAMIC_TIMEFRAME="$timeframe" DYNAMIC_INTERVAL_SECS="$interval_secs" DYNAMIC_MARKET_NUMBER="$market_number" DYNAMIC_MARKET_PREFIXES="$market_prefixes" DYNAMIC_MARKET_MODE="$mode" python - <<'PY'
import json
import os
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

api_base_url = os.environ.get("GAMMA_API_BASE_URL", "https://gamma-api.polymarket.com").rstrip("/")
timeframe = os.environ["DYNAMIC_TIMEFRAME"]
interval_secs = int(os.environ["DYNAMIC_INTERVAL_SECS"])
market_number = int(os.environ["DYNAMIC_MARKET_NUMBER"])
market_prefixes = os.environ["DYNAMIC_MARKET_PREFIXES"].split()
mode = os.environ["DYNAMIC_MARKET_MODE"]
now = int(time.time())
current_start = now - (now % interval_secs)
market_count = market_number + 1 if mode == "lookahead" else market_number

for prefix in market_prefixes:
    for offset in range(market_count):
        start_epoch = current_start + offset * interval_secs
        slug = f"{prefix}-updown-{timeframe}-{start_epoch}"
        url = f"{api_base_url}/markets?{urlencode({'slug': slug})}"
        req = Request(url, headers={"User-Agent": "nautilus-polymarket-collector/1.0"})
        try:
            with urlopen(req, timeout=20) as response:
                markets = json.load(response)
        except Exception as exc:
            print(f"WARN|{slug}|{type(exc).__name__}: {exc}")
            continue

        if isinstance(markets, dict):
            markets = markets.get("markets") or markets.get("data") or []

        for market in markets:
            if market.get("slug") != slug:
                continue
            if market.get("closed") is True:
                continue

            outcomes = market.get("outcomes") or []
            clob_token_ids = market.get("clobTokenIds") or []
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)

            try:
                up_index = outcomes.index("Up")
                token_id = clob_token_ids[up_index]
            except (ValueError, IndexError):
                continue

            condition_id = market.get("conditionId")
            end_date = market.get("endDate")
            if not condition_id or not token_id or not end_date:
                continue

            end_epoch = int(datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp())
            name = f"{prefix}_up_{timeframe}_{start_epoch}"
            print(f"MARKET|{slug}|{name}|{condition_id}|{token_id}|{end_epoch}")
PY
}

start_dynamic_collector_if_needed() {
  local slug="$1"
  local name="$2"
  local condition_id="$3"
  local token_id="$4"
  local end_epoch="$5"

  local pid="${dynamic_pids[$slug]:-}"
  if is_running "$pid"; then
    dynamic_end_epochs["$slug"]="$end_epoch"
    return 0
  fi

  if [[ -n "$pid" ]]; then
    unset 'dynamic_pids[$slug]'
  fi

  start_collector "$name" "$condition_id" "$token_id"
  dynamic_pids["$slug"]="$last_collector_pid"
  dynamic_end_epochs["$slug"]="$end_epoch"
}

stop_expired_dynamic_collectors() {
  local now_epoch
  now_epoch="$(date +%s)"

  for slug in "${!dynamic_pids[@]}"; do
    local pid="${dynamic_pids[$slug]}"
    local end_epoch="${dynamic_end_epochs[$slug]:-0}"
    local stop_epoch=$((end_epoch + DYNAMIC_STOP_GRACE_SECS))

    if (( now_epoch < stop_epoch )); then
      continue
    fi

    echo "Stopping expired dynamic collector: $slug"
    echo "  pid:        $pid"
    echo "  end_epoch:  $end_epoch"
    echo "  stop_epoch: $stop_epoch"

    if is_running "$pid"; then
      kill "$pid" 2>/dev/null || true
    fi

    unset 'dynamic_pids[$slug]'
    unset 'dynamic_end_epochs[$slug]'
  done
}

run_foreground() {
  cd "$PROJECT_DIR"
  load_secrets

  trap cleanup INT TERM

  for row in "${MARKETS[@]}"; do
    IFS="|" read -r name condition_id token_id <<< "$row"

    if [[ -z "$name" || -z "$condition_id" || -z "$token_id" ]]; then
      echo "Invalid market row: $row" >&2
      exit 1
    fi

    start_collector "$name" "$condition_id" "$token_id"
  done

  echo "Started ${#pids[@]} manually configured collectors."
  echo "Logs:"
  echo "  tail -f $LOG_DIR/<market_name>.log"
  echo
  echo "Use '$0 stop' to stop all collectors."

  if [[ "$ENABLE_DYNAMIC_MARKETS" != "1" ]]; then
    wait
    return 0
  fi

  echo "Dynamic ETH/SOL 15m and 1h Up rolling collectors enabled."
  echo "  15m market prefixes: $DYNAMIC_15M_MARKET_PREFIXES"
  echo "  15m lookahead:       $DYNAMIC_15M_LOOKAHEAD_MARKETS"
  echo "  1h market prefixes:  $DYNAMIC_1H_MARKET_PREFIXES"
  echo "  1h market count:     $DYNAMIC_1H_MARKET_COUNT"
  echo "  refresh secs:        $DYNAMIC_REFRESH_SECS"
  echo "  stop grace secs:     $DYNAMIC_STOP_GRACE_SECS"
  echo

  while true; do
    while IFS="|" read -r kind slug name condition_id token_id end_epoch; do
      case "$kind" in
        MARKET)
          start_dynamic_collector_if_needed "$slug" "$name" "$condition_id" "$token_id" "$end_epoch"
          ;;
        WARN)
          echo "Dynamic discovery warning for $slug: $name" >&2
          ;;
      esac
    done < <(
      discover_dynamic_up_markets "15m" "900" "$DYNAMIC_15M_LOOKAHEAD_MARKETS" "$DYNAMIC_15M_MARKET_PREFIXES" "lookahead"
      discover_dynamic_up_markets "1h" "3600" "$DYNAMIC_1H_MARKET_COUNT" "$DYNAMIC_1H_MARKET_PREFIXES" "count"
    )

    stop_expired_dynamic_collectors
    sleep "$DYNAMIC_REFRESH_SECS"
  done
}

case "${1:-restart}" in
  restart|start)
    start_background
    ;;
  stop)
    stop_existing
    ;;
  status)
    status
    ;;
  foreground)
    run_foreground
    ;;
  *)
    echo "Usage: $0 [restart|start|stop|status|foreground]" >&2
    exit 1
    ;;
esac
