#!/usr/bin/env bash
# Numbered MLB model-family stages driver — run all stages in order, or a subset.
#
# Usage:
#   scripts/mlb_models.sh                  # all stages in numbered order
#   scripts/mlb_models.sh 01 05            # by stage number
#   scripts/mlb_models.sh ep xyac          # by model name
#   scripts/mlb_models.sh --force 06       # pass --force through to the stages
#   MLB_MODELS_ARGS="--nrounds 5" scripts/mlb_models.sh 06   # extra stage args
#
# Stage list = python/mlb_model_NN_<family>.py (single home:
# models/manifest.yaml; tests/test_model_manifest.py keeps them in lockstep).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PY="${MLB_DATA_PYTHON:-}"
if [ -z "${PY}" ]; then
  for cand in .venv/Scripts/python.exe .venv/bin/python; do
    if [ -x "${cand}" ]; then PY="${cand}"; break; fi
  done
fi
if [ -z "${PY}" ]; then
  echo "FATAL: no venv python found (uv sync first, or set MLB_DATA_PYTHON)" >&2
  exit 1
fi

FORCE=""
if [ "${1:-}" = "--force" ]; then FORCE="--force"; shift; fi

mapfile -t ALL < <(ls python/mlb_model_[0-9][0-9]_*.py | sort)

SELECTED=()
if [ "$#" -eq 0 ]; then
  SELECTED=("${ALL[@]}")
else
  for want in "$@"; do
    hit=""
    for f in "${ALL[@]}"; do
      base="$(basename "${f}" .py)"
      num="${base:10:2}"
      model="${base:13}"
      if [ "${want}" = "${num}" ] || [ "${want}" = "${model}" ] || [ "${want}" = "${base}" ]; then
        SELECTED+=("${f}")
        hit=1
        break
      fi
    done
    if [ -z "${hit}" ]; then
      echo "FATAL: unknown stage '${want}' (numbers 01-04 or model names)" >&2
      exit 1
    fi
  done
fi

rc=0
for f in "${SELECTED[@]}"; do
  mod="$(basename "${f}" .py)"
  echo "== ${mod}"
  # shellcheck disable=SC2086
  PYTHONPATH=python "${PY}" -m "${mod}" ${FORCE} ${MLB_MODELS_ARGS:-} || {
    rc=$?
    echo "== ${mod} FAILED (rc=${rc})"
    break
  }
done
exit "${rc}"
