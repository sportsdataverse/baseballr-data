#!/usr/bin/env bash
# Render every docs/models/*.qmd to committed GFM + figures (reproducible model writeups).
# Uses this repo's .venv via QUARTO_PYTHON; docs deps: `uv sync --group docs`.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export QUARTO_PYTHON="$ROOT/.venv/Scripts/python.exe"
QUARTO_BIN="${QUARTO_BIN:-quarto}"
command -v "$QUARTO_BIN" >/dev/null 2>&1 || QUARTO_BIN="$LOCALAPPDATA/Programs/Quarto/bin/quarto.cmd"
for q in "$ROOT"/docs/models/*.qmd; do
  echo "== rendering $q"
  "$QUARTO_BIN" render "$q" --to gfm
done
