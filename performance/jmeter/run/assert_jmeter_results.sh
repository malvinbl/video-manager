#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <result_jtl>"
  exit 1
fi

JTL_FILE="$1"
if [[ ! -f "${JTL_FILE}" ]]; then
  echo "JTL file not found: ${JTL_FILE}"
  exit 1
fi

MAX_ERROR_RATE="${MAX_ERROR_RATE:-0.01}"
MAX_P95_MS="${MAX_P95_MS:-800}"
MIN_THROUGHPUT_RPS="${MIN_THROUGHPUT_RPS:-5}"
MIN_SAMPLES="${MIN_SAMPLES:-10}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

ELAPSED_FILE="${TMP_DIR}/elapsed.txt"

awk -F',' 'NR>1 {print $2 > f; total++; if ($8=="true") ok++; else ko++; if (NR==2 || $1<min) min=$1; if ($1>max) max=$1} END {printf "%d %d %d %d %d\n", total, ok, ko, min, max}' \
  f="${ELAPSED_FILE}" "${JTL_FILE}" > "${TMP_DIR}/summary.txt"

read -r TOTAL OK KO MIN_TS MAX_TS < "${TMP_DIR}/summary.txt"

if [[ "${TOTAL}" -eq 0 ]]; then
  echo "No samples found in ${JTL_FILE}"
  exit 1
fi

ERROR_RATE="$(awk -v ko="${KO}" -v t="${TOTAL}" 'BEGIN {printf "%.6f", ko / t}')"
WINDOW_SEC="$(awk -v min="${MIN_TS}" -v max="${MAX_TS}" 'BEGIN {w=(max-min)/1000.0; if (w<=0) w=1; printf "%.6f", w}')"
THROUGHPUT_RPS="$(awk -v t="${TOTAL}" -v w="${WINDOW_SEC}" 'BEGIN {printf "%.6f", t / w}')"

SORTED_FILE="${TMP_DIR}/elapsed.sorted.txt"
sort -n "${ELAPSED_FILE}" > "${SORTED_FILE}"
P95_MS="$(awk -v n="${TOTAL}" 'BEGIN {k=int((0.95*n)+0.999999); if (k<1) k=1} NR==k {print; exit}' "${SORTED_FILE}")"
if [[ -z "${P95_MS}" ]]; then
  P95_MS=0
fi

echo "JMeter assertions for: ${JTL_FILE}"
echo "  total=${TOTAL} ok=${OK} ko=${KO}"
echo "  error_rate=${ERROR_RATE} (max=${MAX_ERROR_RATE})"
echo "  p95_ms=${P95_MS} (max=${MAX_P95_MS})"
echo "  throughput_rps=${THROUGHPUT_RPS} (min=${MIN_THROUGHPUT_RPS})"

FAIL=0

if [[ "${TOTAL}" -lt "${MIN_SAMPLES}" ]]; then
  echo "ASSERT FAIL: total samples ${TOTAL} is lower than MIN_SAMPLES=${MIN_SAMPLES}"
  FAIL=1
fi

if awk -v v="${ERROR_RATE}" -v max="${MAX_ERROR_RATE}" 'BEGIN {exit !(v>max)}'; then
  echo "ASSERT FAIL: error_rate ${ERROR_RATE} > ${MAX_ERROR_RATE}"
  FAIL=1
fi

if awk -v v="${P95_MS}" -v max="${MAX_P95_MS}" 'BEGIN {exit !(v>max)}'; then
  echo "ASSERT FAIL: p95_ms ${P95_MS} > ${MAX_P95_MS}"
  FAIL=1
fi

if awk -v v="${THROUGHPUT_RPS}" -v min="${MIN_THROUGHPUT_RPS}" 'BEGIN {exit !(v<min)}'; then
  echo "ASSERT FAIL: throughput_rps ${THROUGHPUT_RPS} < ${MIN_THROUGHPUT_RPS}"
  FAIL=1
fi

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi

echo "ASSERT OK"
