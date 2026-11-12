#!/usr/bin/env bash
# miniKafka CLI 交互原型 —— 纯 bash 模拟，无真实网络/存储/C++。
# 在 Windows Git Bash 下直接运行：bash prototype/demo.sh
# 所有数据都是假数据，状态保存在 prototype/.demo-state/，每次运行重置。

set -u
exec 2>&1

ROOT="$(cd "$(dirname "$0")" && pwd)"
FIXTURES="$ROOT/fixtures"
STATE="$ROOT/.demo-state"
DATA_LABEL=".demo-state"

# ---------------------------------------------------------------- 展示辅助
say()    { printf '%s\n' "$*"; }
scene()  { printf '\n====================================================================\n[场景] %s\n' "$*"; }
doing()  { printf '[正在做什么] %s\n' "$*"; }
expect() { printf '[预期看到] %s\n' "$*"; }
code()   { printf '[退出码] %s\n' "$*"; }

err_json() {
  printf '{"error":"%s","message":"%s"}\n' "$1" "$2" >&2
}

# ---------------------------------------------------------------- 通用工具
json_field() {
  local line="$1" name="$2"
  printf '%s\n' "$line" | sed -n 's/.*"'"$name"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
}

json_has_field() {
  printf '%s' "$1" | grep -q "\"$2\"[[:space:]]*:"
}

ensure_state() {
  mkdir -p "$STATE/topics" "$STATE/groups" "$STATE/tmp"
  [ -f "$STATE/rr" ] || printf '0\n' > "$STATE/rr"
}

state_reset() {
  rm -rf "$STATE"
  mkdir -p "$STATE/topics" "$STATE/groups" "$STATE/tmp"
  printf '0\n' > "$STATE/rr"
}

next_offset() {
  local f="$1"
  if [ -f "$f" ]; then
    wc -l < "$f" | tr -d ' '
  else
    printf '0\n'
  fi
}

topic_parts() {
  cat "$STATE/topics/$1/meta" 2>/dev/null
}

hash_partition() {
  local key="$1" parts="$2" h=2166136261 i c
  for ((i = 0; i < ${#key}; i++)); do
    c=$(printf '%d' "'${key:i:1}")
    h=$(( (h ^ c) * 16777619 ))
    h=$(( h & 0xffffffff ))
  done
  printf '%s\n' "$(( h % parts ))"
}

round_robin() {
  local parts="$1" c p
  c=$(cat "$STATE/rr" 2>/dev/null || printf '0')
  case "$c" in ''|*[!0-9]*) c=0 ;; esac
  p=$(( c % parts ))
  printf '%s\n' "$(( c + 1 ))" > "$STATE/rr"
  printf '%s\n' "$p"
}

# 写失败模拟：返回 0 可写；返回 1 时 LAST_ERR 给出原因
LAST_ERR=""
can_write() {
  LAST_ERR=""
  if [ -f "$STATE/disk_full" ]; then
    LAST_ERR="disk_full"
    return 1
  fi
  if [ -f "$STATE/free_slots" ]; then
    local s
    s=$(cat "$STATE/free_slots" 2>/dev/null || printf '0')
    case "$s" in ''|*[!0-9]*) s=0 ;; esac
    if [ "$s" -le 0 ]; then
      LAST_ERR="disk_full"
      return 1
    fi
  fi
  return 0
}

consume_slot() {
  if [ -f "$STATE/free_slots" ]; then
    local s
    s=$(cat "$STATE/free_slots" 2>/dev/null || printf '0')
    case "$s" in ''|*[!0-9]*) s=0 ;; esac
    [ "$s" -gt 0 ] && printf '%s\n' "$(( s - 1 ))" > "$STATE/free_slots"
  fi
}

# 恢复：只保留连续、完整（3 个字段）、offset 连续的行
recover_log() {
  local t="$1" p="$2" f="$3"
  local total kept=0 expected=0 line off key val tmp
  [ -f "$f" ] || return 0
  total=$(awk 'END { print NR }' "$f" 2>/dev/null || printf '0')
  [ "$total" -eq 0 ] && return 0
  tmp="$STATE/tmp/recover.$$.$p"
  : > "$tmp"
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    if [[ "$line" == *$'\t'*$'\t'* ]]; then
      off=$(printf '%s\n' "$line" | cut -f1)
      if [[ "$off" =~ ^[0-9]+$ ]] && [ "$off" -eq "$expected" ]; then
        printf '%s\n' "$line" >> "$tmp"
        expected=$(( expected + 1 ))
        kept=$(( kept + 1 ))
        continue
      fi
    fi
    break
  done < "$f"
  if [ "$kept" -lt "$total" ]; then
    mv "$tmp" "$f"
    printf '{"event":"recovery","topic":"%s","partition":%s,"kept":%s,"dropped":%s}\n' \
      "$t" "$p" "$kept" "$(( total - kept ))" >&2
  else
    rm -f "$tmp"
  fi
}

# ---------------------------------------------------------------- mk 命令分发
mk() {
  local cmd="${1:-}"
  if [ $# -eq 0 ]; then
    err_json "usage" "missing subcommand"
    return 2
  fi
  shift
  case "$cmd" in
    broker)  cmd_broker "$@" ;;
    topic)
      local sub="${1:-}"
      if [ $# -eq 0 ]; then
        err_json "usage" "topic requires create|list"
        return 2
      fi
      shift
      case "$sub" in
        create) cmd_topic_create "$@" ;;
        list)   cmd_topic_list "$@" ;;
        *) err_json "usage" "unknown topic subcommand: $sub"; return 2 ;;
      esac ;;
    produce) cmd_produce "$@" ;;
    consume) cmd_consume "$@" ;;
    replay)  cmd_replay "$@" ;;
    *) err_json "usage" "unknown subcommand: $cmd"; return 2 ;;
  esac
}

# ---------------------------------------------------------------- broker
cmd_broker() {
  local port=9092 data_dir="" d t f p
  while [ $# -gt 0 ]; do
    case "$1" in
      --port)
        [ $# -ge 2 ] || { err_json "usage" "--port requires a value"; return 2; }
        port="$2"; shift 2 ;;
      --data-dir)
        [ $# -ge 2 ] || { err_json "usage" "--data-dir requires a value"; return 2; }
        data_dir="$2"; shift 2 ;;
      *) err_json "usage" "unknown broker option: $1"; return 2 ;;
    esac
  done
  if [ -n "$data_dir" ]; then
    STATE="$data_dir"
    DATA_LABEL="$data_dir"
  fi
  ensure_state
  for d in "$STATE"/topics/*; do
    [ -d "$d" ] || continue
    t=$(basename "$d")
    for f in "$d"/p*.log; do
      [ -f "$f" ] || continue
      p=${f##*/p}
      p=${p%.log}
      recover_log "$t" "$p" "$f"
    done
  done
  : > "$STATE/broker.started"
  printf '{"event":"broker_started","port":%s,"data_dir":"%s","status":"running"}\n' "$port" "$DATA_LABEL"
  printf '{"event":"ready"}\n'
  return 0
}

# ---------------------------------------------------------------- topic
cmd_topic_create() {
  local topic="" parts=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --topic)
        [ $# -ge 2 ] || { err_json "usage" "--topic requires a value"; return 2; }
        topic="$2"; shift 2 ;;
      --partitions)
        [ $# -ge 2 ] || { err_json "usage" "--partitions requires a value"; return 2; }
        parts="$2"; shift 2 ;;
      *) err_json "usage" "unknown topic create option: $1"; return 2 ;;
    esac
  done
  [ -n "$topic" ] || { err_json "usage" "--topic is required"; return 2; }
  if ! [[ "$topic" =~ ^[A-Za-z0-9_-]+$ ]]; then
    err_json "invalid_topic_name" "topic name must match [A-Za-z0-9_-]+"
    return 3
  fi
  case "$parts" in ''|*[!0-9]*) err_json "invalid_partitions" "--partitions must be an integer"; return 3 ;; esac
  if [ "$parts" -lt 1 ] || [ "$parts" -gt 16 ]; then
    err_json "invalid_partitions" "partitions must be between 1 and 16"
    return 3
  fi
  if [ -d "$STATE/topics/$topic" ]; then
    err_json "topic_exists" "topic '$topic' already exists"
    return 3
  fi
  mkdir -p "$STATE/topics/$topic"
  printf '%s\n' "$parts" > "$STATE/topics/$topic/meta"
  local i
  for ((i = 0; i < parts; i++)); do
    : > "$STATE/topics/$topic/p$i.log"
  done
  printf '{"topic":"%s","partitions":%s,"status":"created"}\n' "$topic" "$parts"
  return 0
}

cmd_topic_list() {
  local d t parts ids i found=0
  for d in "$STATE"/topics/*; do
    [ -d "$d" ] || continue
    found=1
    t=$(basename "$d")
    parts=$(cat "$d/meta")
    ids=""
    for ((i = 0; i < parts; i++)); do
      [ -n "$ids" ] && ids="$ids,"
      ids="$ids$i"
    done
    printf '{"topic":"%s","partitions":%s,"partition_ids":[%s]}\n' "$t" "$parts" "$ids"
  done
  if [ "$found" -eq 0 ]; then
    printf '{"topics":[]}\n'
  fi
  return 0
}

# ---------------------------------------------------------------- produce
cmd_produce() {
  local topic="" partition="" file="" message="" retries=3
  local input="" line="" key="" val="" p="" off="" index=0
  local blocked_list=" " succeeded=0 failed_count=0 retry_exhausted=0
  local attempt=0 ok=0 err="" total_lines=0
  local success_file failed_file succ_json="" fail_json="" parts="" d
  local pp parts_list offs rc=0

  while [ $# -gt 0 ]; do
    case "$1" in
      --topic)       [ $# -ge 2 ] || { err_json "usage" "--topic requires a value"; return 2; }; topic="$2"; shift 2 ;;
      --partition)   [ $# -ge 2 ] || { err_json "usage" "--partition requires a value"; return 2; }; partition="$2"; shift 2 ;;
      --file)        [ $# -ge 2 ] || { err_json "usage" "--file requires a value"; return 2; }; file="$2"; shift 2 ;;
      --message)     [ $# -ge 2 ] || { err_json "usage" "--message requires a value"; return 2; }; message="$2"; shift 2 ;;
      --retries)     [ $# -ge 2 ] || { err_json "usage" "--retries requires a value"; return 2; }; retries="$2"; shift 2 ;;
      *) err_json "usage" "unknown produce option: $1"; return 2 ;;
    esac
  done

  [ -n "$topic" ] || { err_json "usage" "--topic is required"; return 2; }
  [ -f "$STATE/topics/$topic/meta" ] || { err_json "topic_not_found" "topic '$topic' does not exist"; return 3; }
  parts=$(topic_parts "$topic")
  case "$retries" in ''|*[!0-9]*) err_json "usage" "--retries must be an integer"; return 2 ;; esac

  if [ -n "$partition" ]; then
    case "$partition" in ''|*[!0-9]*) err_json "invalid_partition" "--partition must be an integer"; return 3 ;; esac
    if [ "$partition" -lt 0 ] || [ "$partition" -ge "$parts" ]; then
      err_json "invalid_partition" "partition must be between 0 and $(( parts - 1 ))"
      return 3
    fi
  fi

  if [ -n "$message" ]; then
    input="$message"
  elif [ -n "$file" ]; then
    if [ ! -f "$file" ]; then
      err_json "file_not_found" "input file '$file' does not exist"
      return 3
    fi
    input=$(cat "$file")
  elif [ ! -t 0 ]; then
    input=$(cat)
  else
    err_json "usage" "no input: use --message, --file, or pipe JSON lines to stdin"
    return 2
  fi
  if [ -z "$input" ]; then
    err_json "usage" "input is empty"
    return 2
  fi

  success_file="$STATE/tmp/success.$$"
  failed_file="$STATE/tmp/failed.$$"
  : > "$success_file"
  : > "$failed_file"

  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    [ -z "$line" ] && continue
    key=$(json_field "$line" key)

    if [ -n "$partition" ]; then
      p="$partition"
    elif [ -n "$key" ]; then
      p=$(hash_partition "$key" "$parts")
    else
      p=$(round_robin "$parts")
    fi

    if ! json_has_field "$line" value; then
      printf '{"index":%s,"partition":%s,"status":"error","error":"invalid_json","detail":"missing value field"}\n' "$index" "$p"
      failed_count=$(( failed_count + 1 ))
      printf '%s\n' "$index" >> "$failed_file"
      index=$(( index + 1 ))
      continue
    fi
    val=$(json_field "$line" value)

    if [ "$val" = "OVERSIZE_DEMO" ]; then
      printf '{"index":%s,"partition":%s,"status":"error","error":"message_too_large","detail":"1048577 > 1048576"}\n' "$index" "$p"
      failed_count=$(( failed_count + 1 ))
      printf '%s\n' "$index" >> "$failed_file"
      index=$(( index + 1 ))
      continue
    fi

    if [[ "$blocked_list" == *" $p "* ]]; then
      printf '{"index":%s,"partition":%s,"status":"error","error":"partition_blocked"}\n' "$index" "$p"
      failed_count=$(( failed_count + 1 ))
      printf '%s\n' "$index" >> "$failed_file"
      index=$(( index + 1 ))
      continue
    fi

    attempt=0
    ok=0
    err=""
    while :; do
      if can_write; then
        ok=1
        break
      fi
      err="$LAST_ERR"
      if [ "$attempt" -ge "$retries" ]; then
        break
      fi
      attempt=$(( attempt + 1 ))
      printf '{"event":"retry","index":%s,"partition":%s,"attempt":%s,"error":"%s"}\n' "$index" "$p" "$attempt" "$err" >&2
    done

    if [ "$ok" -eq 1 ]; then
      off=$(next_offset "$STATE/topics/$topic/p$p.log")
      printf '%s\t%s\t%s\n' "$off" "$key" "$val" >> "$STATE/topics/$topic/p$p.log"
      consume_slot
      printf '{"index":%s,"partition":%s,"offset":%s,"status":"ok"}\n' "$index" "$p" "$off"
      printf '%s\t%s\n' "$p" "$off" >> "$success_file"
      succeeded=$(( succeeded + 1 ))
    else
      printf '{"index":%s,"partition":%s,"status":"error","error":"%s"}\n' "$index" "$p" "$err"
      failed_count=$(( failed_count + 1 ))
      printf '%s\n' "$index" >> "$failed_file"
      blocked_list="$blocked_list$p "
      retry_exhausted=1
    fi
    index=$(( index + 1 ))
  done <<< "$input"

  if [ "$failed_count" -gt 0 ]; then
    succ_json=""
    if [ -s "$success_file" ]; then
      parts_list=$(cut -f1 "$success_file" | sort -n | uniq)
      for pp in $parts_list; do
        offs=$(awk -F'\t' -v p="$pp" '$1 == p { printf "%s%s", sep, $2; sep="," }' "$success_file")
        [ -n "$succ_json" ] && succ_json="$succ_json,"
        succ_json="$succ_json{\"partition\":$pp,\"offsets\":[$offs]}"
      done
    fi
    fail_json=$(tr '\n' ',' < "$failed_file" | sed 's/,$//')
    if [ "$retry_exhausted" -eq 1 ]; then
      local summary_status="partial_failure"
      [ "$succeeded" -eq 0 ] && summary_status="failed"
      printf '{"event":"produce_summary","status":"%s","succeeded":%s,"failed":%s,"success_offsets":[%s],"failed_indices":[%s],"exit_reason":"retry_exhausted"}\n' \
        "$summary_status" "$succeeded" "$failed_count" "$succ_json" "$fail_json"
      rc=4
    else
      printf '{"event":"produce_summary","status":"failed","succeeded":%s,"failed":%s,"success_offsets":[%s],"failed_indices":[%s],"exit_reason":"validation_error"}\n' \
        "$succeeded" "$failed_count" "$succ_json" "$fail_json"
      rc=3
    fi
  fi
  rm -f "$success_file" "$failed_file"
  return "$rc"
}

# ---------------------------------------------------------------- consume
cmd_consume() {
  local topic="" group="" from="earliest" interval="5s" exit_at_end=0
  local parts="" slot_idx=0 slot_n=1 p assigned="" cf log start=0 src="" off key val
  local total_processed=0 elapsed=0 last_commit=0 interval_num=0 crash_after=0
  local -a prog=()
  local -a touched=()

  while [ $# -gt 0 ]; do
    case "$1" in
      --topic)                [ $# -ge 2 ] || { err_json "usage" "--topic requires a value"; return 2; }; topic="$2"; shift 2 ;;
      --group)                [ $# -ge 2 ] || { err_json "usage" "--group requires a value"; return 2; }; group="$2"; shift 2 ;;
      --from)                 [ $# -ge 2 ] || { err_json "usage" "--from requires a value"; return 2; }; from="$2"; shift 2 ;;
      --auto-commit-interval) [ $# -ge 2 ] || { err_json "usage" "--auto-commit-interval requires a value"; return 2; }; interval="$2"; shift 2 ;;
      --exit-at-end)          exit_at_end=1; shift ;;
      *) err_json "usage" "unknown consume option: $1"; return 2 ;;
    esac
  done

  [ -n "$topic" ] || { err_json "usage" "--topic is required"; return 2; }
  [ -n "$group" ] || { err_json "usage" "--group is required"; return 2; }
  if ! [[ "$group" =~ ^[A-Za-z0-9_-]+$ ]]; then
    err_json "invalid_group_name" "group name must match [A-Za-z0-9_-]+"
    return 3
  fi
  [ -f "$STATE/topics/$topic/meta" ] || { err_json "topic_not_found" "topic '$topic' does not exist"; return 3; }
  parts=$(topic_parts "$topic")

  interval_num="${interval%[sS]}"
  case "$interval_num" in ''|*[!0-9]*|0) err_json "invalid_interval" "--auto-commit-interval must be positive seconds (e.g. 1s)"; return 2 ;; esac

  case "$from" in
    earliest|latest) ;;
    ''|*[!0-9]*) err_json "invalid_from" "--from must be earliest, latest, or a non-negative offset"; return 3 ;;
  esac

  if [ -n "${MK_DEMO_SLOT:-}" ]; then
    slot_idx="${MK_DEMO_SLOT%%/*}"
    slot_n="${MK_DEMO_SLOT##*/}"
    case "$slot_idx" in ''|*[!0-9]*) err_json "invalid_slot" "MK_DEMO_SLOT must look like 0/2"; return 2 ;; esac
    case "$slot_n" in ''|*[!0-9]*|0) err_json "invalid_slot" "MK_DEMO_SLOT must look like 0/2"; return 2 ;; esac
    [ "$slot_n" -gt 0 ] || { err_json "invalid_slot" "slot count must be > 0"; return 2; }
    [ "$slot_idx" -lt "$slot_n" ] || { err_json "invalid_slot" "slot index must be < slot count"; return 2; }
  fi

  case "${MK_DEMO_CRASH_AFTER:-}" in
    ''|*[!0-9]*) crash_after=0 ;;
    *) crash_after="$MK_DEMO_CRASH_AFTER" ;;
  esac

  for ((p = 0; p < parts; p++)); do
    if [ $(( p % slot_n )) -eq "$slot_idx" ]; then
      assigned="$assigned $p"
    fi
  done
  printf '{"event":"assigned","group":"%s","topic":"%s","partitions":[%s]}\n' \
    "$group" "$topic" "$(printf '%s' "$assigned" | tr ' ' ',' | sed 's/^,//')" >&2

  for p in $assigned; do
    cf="$STATE/groups/$group/$topic/offsets/$p"
    log="$STATE/topics/$topic/p$p.log"
    [ -f "$log" ] || : > "$log"

    if [ -f "$cf" ]; then
      start=$(cat "$cf")
      src="committed"
    else
      case "$from" in
        earliest) start=0; src="earliest" ;;
        latest)   start=$(next_offset "$log"); src="latest" ;;
        *)        start="$from"; src="offset" ;;
      esac
    fi
    printf '{"event":"start","partition":%s,"offset":%s,"source":"%s"}\n' "$p" "$start" "$src" >&2

    while IFS= read -r line || [ -n "$line" ]; do
      line=${line%$'\r'}
      [ -z "$line" ] && continue
      off=$(printf '%s\n' "$line" | cut -f1)
      key=$(printf '%s\n' "$line" | cut -f2)
      val=$(printf '%s\n' "$line" | cut -f3-)
      printf '{"partition":%s,"offset":%s,"key":"%s","value":"%s"}\n' "$p" "$off" "$key" "$val"
      total_processed=$(( total_processed + 1 ))
      elapsed=$(( elapsed + 1 ))
      prog[$p]=$(( off + 1 ))
      touched[$p]=1

      if [ $(( elapsed - last_commit )) -ge "$interval_num" ]; then
        local cp
        for cp in "${!touched[@]}"; do
          if [ "${touched[$cp]}" = "1" ]; then
            mkdir -p "$STATE/groups/$group/$topic/offsets"
            printf '%s\n' "${prog[$cp]}" > "$STATE/groups/$group/$topic/offsets/$cp"
            printf '{"event":"commit","partition":%s,"offset":%s}\n' "$cp" "${prog[$cp]}" >&2
            touched[$cp]=0
          fi
        done
        last_commit=$elapsed
      fi

      if [ "$crash_after" -gt 0 ] && [ "$total_processed" -ge "$crash_after" ]; then
        printf '{"event":"consumer_crashed","processed":%s,"note":"simulated kill -9; uncommitted progress lost"}\n' "$total_processed" >&2
        return 137
      fi
    done < <(tail -n +"$(( start + 1 ))" "$log")
  done

  if [ "$exit_at_end" -eq 0 ]; then
    printf '{"event":"follow","status":"waiting_for_new_messages","note":"prototype returns immediately instead of blocking"}\n' >&2
  fi
  return 0
}

# ---------------------------------------------------------------- replay
cmd_replay() {
  local topic="" partition="0" offset="0" count="" total=0 printed=0 line off key val
  while [ $# -gt 0 ]; do
    case "$1" in
      --topic)    [ $# -ge 2 ] || { err_json "usage" "--topic requires a value"; return 2; }; topic="$2"; shift 2 ;;
      --partition)[ $# -ge 2 ] || { err_json "usage" "--partition requires a value"; return 2; }; partition="$2"; shift 2 ;;
      --offset)   [ $# -ge 2 ] || { err_json "usage" "--offset requires a value"; return 2; }; offset="$2"; shift 2 ;;
      --count)    [ $# -ge 2 ] || { err_json "usage" "--count requires a value"; return 2; }; count="$2"; shift 2 ;;
      *) err_json "usage" "unknown replay option: $1"; return 2 ;;
    esac
  done
  [ -n "$topic" ] || { err_json "usage" "--topic is required"; return 2; }
  [ -f "$STATE/topics/$topic/meta" ] || { err_json "topic_not_found" "topic '$topic' does not exist"; return 3; }
  case "$partition" in ''|*[!0-9]*) err_json "invalid_partition" "--partition must be an integer"; return 3 ;; esac
  case "$offset" in ''|*[!0-9]*) err_json "invalid_offset" "--offset must be a non-negative integer"; return 3 ;; esac
  if [ -n "$count" ]; then
    case "$count" in ''|*[!0-9]*|0) err_json "invalid_count" "--count must be a positive integer"; return 2 ;; esac
  fi
  local parts
  parts=$(topic_parts "$topic")
  if [ "$partition" -ge "$parts" ]; then
    err_json "invalid_partition" "partition must be between 0 and $(( parts - 1 ))"
    return 3
  fi
  total=$(next_offset "$STATE/topics/$topic/p$partition.log")
  if [ "$offset" -gt "$total" ]; then
    err_json "invalid_offset" "offset $offset is beyond the end ($total)"
    return 3
  fi

  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    [ -z "$line" ] && continue
    off=$(printf '%s\n' "$line" | cut -f1)
    key=$(printf '%s\n' "$line" | cut -f2)
    val=$(printf '%s\n' "$line" | cut -f3-)
    printf '{"partition":%s,"offset":%s,"key":"%s","value":"%s"}\n' "$partition" "$off" "$key" "$val"
    printed=$(( printed + 1 ))
    if [ -n "$count" ] && [ "$printed" -ge "$count" ]; then
      break
    fi
  done < <(tail -n +"$(( offset + 1 ))" "$STATE/topics/$topic/p$partition.log")
  return 0
}

# ================================================================ 演示剧本
state_reset

scene "0. 启动（假）Broker"
doing "重置 prototype/.demo-state 假数据；启动模拟 Broker（不建立真实 TCP，不启动真实进程）"
expect "输出 broker_started 与 ready；后续命令共享同一份假状态；每次运行都从空状态开始"
mk broker --port 9092
code $?

scene "1. Topic 创建与列出"
doing "创建 orders（4 个分区）；重复创建同名 Topic；再尝试创建 17 个分区的非法 Topic；最后列出 Topic"
expect "orders 创建成功；重复创建返回 topic_exists、退出码 3；17 分区返回 invalid_partitions、退出码 3；list 显示 4 个分区 id"
mk topic create --topic orders --partitions 4
code $?
mk topic create --topic orders --partitions 4
code $?
mk topic create --topic bad --partitions 17
code $?
mk topic list

scene "2.1 Producer：单条消息 + 指定分区（R22）"
doing "发送 1 条 key=k0 的消息，显式指定 partition 0"
expect "返回 status=ok、partition=0、offset=0"
mk produce --topic orders --partition 0 --message '{"key":"k0","value":"hello-0"}'
code $?

scene "2.2 Producer：批量 + 带 key 走哈希（R22/R23）"
doing "批量发送 2 条 key=user-1 的消息，不指定分区"
expect "两条都返回 ok；partition 相同（同一 key 哈希稳定）；offset 在该分区内连续递增"
mk produce --topic orders --file "$FIXTURES/keyed.jsonl"
code $?

scene "2.3 Producer：批量 + 无 key 走轮询（R22/R23）"
doing "批量发送 6 条无 key 消息"
expect "6 条按轮询依次落到分区 0,1,2,3,0,1；每条逐行返回 ok 与各自 offset"
mk produce --topic orders --file "$FIXTURES/nokey.jsonl"
code $?

scene "2.4 Producer：准备位点演示数据"
doing "创建单分区 offset-demo，写入 5 条消息（供指定位点与重启续消费演示）"
expect "5 条都返回 ok，offset 依次为 0..4"
mk topic create --topic offset-demo --partitions 1
mk produce --topic offset-demo --file "$FIXTURES/offset5.jsonl"
code $?

scene "3.1 Consumer：首次消费 earliest + 自动提交 + 读完即退（R6/R11/R18/R19）"
doing "group=g1，从 earliest 消费 orders，自动提交间隔 1s（原型中 1 条消息=1 秒假时钟），--exit-at-end"
expect "逐条输出 JSON（含 partition/offset/key/value）；每处理一条就出现 commit 事件；读到末尾后退出码 0"
mk consume --topic orders --group g1 --from earliest --auto-commit-interval 1s --exit-at-end
code $?

scene "3.2 Consumer：从指定位点开始（新 group，无已提交位点，R11）"
doing "新 group=g-offset，从 offset-demo 的 offset 2 开始消费，--exit-at-end"
expect "因为没有已提交位点，start 事件 source=offset、offset=2；输出 offset 2、3、4"
mk consume --topic offset-demo --group g-offset --from 2 --auto-commit-interval 1s --exit-at-end
code $?

scene "3.3 Consumer：latest（新 group，无已提交位点，R11）"
doing "新 group=g-latest，用 --from latest 消费 orders，--exit-at-end"
expect "没有已提交位点，按 latest 从各分区末尾开始：start 事件 source=latest；没有历史消息输出，退出码 0"
mk consume --topic orders --group g-latest --from latest --auto-commit-interval 1s --exit-at-end
code $?

scene "3.4 Consumer：已提交位点优先（R11）"
doing "同一个 group=g1，用 --from latest 再次启动"
expect "忽略 latest：start 事件的 source=committed，从上次已提交位点继续；因为没有新消息，只打印 start 事件后退出码 0"
mk consume --topic orders --group g1 --from latest --auto-commit-interval 1s --exit-at-end
code $?

scene "3.5 Consumer：两个消费者同一 group 的静态分区分工（R10/R13）"
doing "group=g2 下模拟两个消费者：slot 0/2 与 slot 1/2；两者都从 earliest 读取 orders"
expect "第一个消费者分配 [0,2]，第二个分配 [1,3]；分区互不重叠；各自只输出自己分区的消息"
MK_DEMO_SLOT="0/2"
mk consume --topic orders --group g2 --from earliest --auto-commit-interval 1s --exit-at-end
code $?
MK_DEMO_SLOT="1/2"
mk consume --topic orders --group g2 --from earliest --auto-commit-interval 1s --exit-at-end
code $?
unset MK_DEMO_SLOT

scene "3.6 Consumer：持续跟随与 --exit-at-end（R18）"
doing "创建 2 分区的 follow-demo，发送 1 条消息；不带 --exit-at-end 消费"
expect "先输出该消息；随后输出 follow 事件（waiting_for_new_messages）；原型不会真的阻塞，立即返回退出码 0"
mk topic create --topic follow-demo --partitions 2
mk produce --topic follow-demo --message '{"value":"follow-me"}'
mk consume --topic follow-demo --group gf --from earliest --auto-commit-interval 1s
code $?

scene "4. Replay：按 offset 只读回放（R12）"
doing "replay orders 的 partition 0，从 offset 0 读 2 条"
expect "输出 2 条历史消息；没有 commit 事件"
mk replay --topic orders --partition 0 --offset 0 --count 2
code $?
doing "replay 之后，用 group=g1 再消费一次（--from earliest 应被已提交位点覆盖）"
expect "g1 仍从已提交位点开始，没有新消息；说明 replay 不改变任何 group 进度"
mk consume --topic orders --group g1 --from earliest --auto-commit-interval 1s --exit-at-end
code $?

scene "5. 位点提交与中断重启：展示最多重复区间（R6/R11/R28）"
doing "复用 2.4 写入的 offset-demo（5 条）；group=g-crash 以提交间隔 3s 消费，并在处理完第 5 条后模拟 kill -9"
expect "输出 offset 0..4；commit 事件出现在处理第 3 条之后（offset=3）；随后 consumer_crashed，退出码 137"
MK_DEMO_CRASH_AFTER=5
mk consume --topic offset-demo --group g-crash --from earliest --auto-commit-interval 3s --exit-at-end
code $?
unset MK_DEMO_CRASH_AFTER
doing "同一个 group=g-crash 重启，仍然传 --from earliest"
expect "start 事件显示 source=committed、offset=3；输出 offset 3、4（即最多重复 2 条），随后继续提交并退出码 0"
mk consume --topic offset-demo --group g-crash --from earliest --auto-commit-interval 1s --exit-at-end
code $?

scene "6.1 错误：磁盘满拒写（R16）"
doing "模拟磁盘满（设置假磁盘满标记），发送 1 条消息，重试次数 3"
expect "每次重试前输出 retry 事件；最终返回 disk_full 错误与退出码 4；消息未写入"
touch "$STATE/disk_full"
mk produce --topic orders --message '{"key":"df","value":"should-fail"}' --retries 3
code $?
doing "磁盘满时读取已有数据"
expect "replay 仍能正常读出历史消息；读路径不受影响"
mk replay --topic orders --partition 0 --offset 0 --count 1
code $?
rm -f "$STATE/disk_full"

scene "6.2 错误：单条消息超限（R17）"
doing "发送 value=OVERSIZE_DEMO 的消息（原型用它模拟 1MB+1 字节）"
expect "直接拒绝为 message_too_large，不重试、不截断；输出 produce_summary（validation_error）；退出码 3"
mk produce --topic orders --message '{"key":"big","value":"OVERSIZE_DEMO"}'
code $?

scene "6.3 错误：批量部分失败、分区阻塞、重试耗尽（R3/R23/R24/R29）"
doing "模拟只剩 2 条写入空间；向 partition 2 批量发送 4 条；重试次数 2"
expect "前 2 条 ok（offset 连续）；第 3 条 disk_full，重试 2 次后该分区阻塞；第 4 条得到 partition_blocked；最后输出 produce_summary（含成功条目 offset 汇总），退出码 4"
printf '2\n' > "$STATE/free_slots"
mk produce --topic orders --partition 2 --file "$FIXTURES/batch4.jsonl" --retries 2
code $?
rm -f "$STATE/free_slots"

scene "6.4 错误恢复：Broker 崩溃后自动截断残缺尾部（R7/R27）"
doing "向 orders/p0 尾部写入半条记录（无结尾换行），模拟 kill -9 后重启 Broker"
expect "Broker 启动时输出 recovery 事件（dropped=1），丢弃残缺尾部；无需人工修复；完整消息仍可 replay"
printf '%s\t%s' "999" "PARTIAL-NO-NEWLINE" >> "$STATE/topics/orders/p0.log"
mk broker --port 9092
code $?
doing "恢复后回放 orders/p0 的前 3 条"
expect "只输出完整记录，offset 连续；残缺记录不出现"
mk replay --topic orders --partition 0 --offset 0 --count 3
code $?

scene "7. 演示结束"
say "已覆盖：Topic 创建/列出、三种分区选择、单条/批量、逐条结果、earliest/latest/位点、自动提交、"
say "静态分区分工、持续跟随、只读重放、中断续消费、磁盘满、消息超限、批量失败、崩溃恢复。"
say "全部为假数据模拟；真实网络、存储、C++ 均未涉及。"

exit 0
