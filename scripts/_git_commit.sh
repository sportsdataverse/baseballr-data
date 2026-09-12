#!/usr/bin/env bash
# Shared commit+push helper -- `source` it, never run it.
#
# One implementation, four callers. It was already duplicated verbatim in the
# two daily scrapers, and the other two drivers had their own weaker versions
# that reported success for work that never left the box:
#
#   git push > /dev/null || true          # R processor: a rejected push = green
#   git commit ... || echo "No changes"   # a FAILED commit reported as "nothing to do"
#   git add ... 2>/dev/null               # a failed add reported as "nothing to commit"
#
# That is the same defect that cost odds-data 11 hours of snapshots and left
# nfl-raw diverged for days: the failure is real, the exit code is 0.
#
#   source "$(dirname "$0")/_git_commit.sh"
#   sdv_commit_push "NCAA Schedules update (Start: 2026 End: 2026)" ncaa/ logs/
#
# Returns 0 only when the work is on origin (or there was genuinely nothing to
# commit). Callers must propagate that: `|| PUSH_RC=1` and exit non-zero.

sdv_commit_push() {
  local msg="$1"; shift

  # Keep only pathspecs that exist. `git add` rejects the WHOLE pathspec if one
  # element matches nothing, staging NOTHING -- and an unmatched glob arrives
  # here as a literal (no nullglob). Silently staging nothing and reporting
  # "nothing to commit" is precisely the failure this helper exists to stop.
  local p paths=()
  for p in "$@"; do
    if [ -e "$p" ]; then paths+=("$p"); else echo "  skip (absent): $p"; fi
  done
  if [ "${#paths[@]}" -eq 0 ]; then
    echo "nothing to stage for: $msg"
    return 0
  fi

  # NOT swallowed: a failed add means the commit below sees an empty index and
  # would report "nothing to commit" for work that is sitting right there.
  if ! git add -- "${paths[@]}"; then
    echo "::error ::git add failed for: $msg"
    return 1
  fi

  if git diff --cached --quiet; then
    echo "nothing to commit for: $msg"
    return 0
  fi

  git commit -m "$msg" >/dev/null || { echo "::error ::commit failed: $msg"; return 1; }

  # Survive a remote that moved while the build was running. rebase --merge,
  # never plain `pull --rebase`: the am backend base64-encodes binary and
  # stalls on a parquet-heavy tree.
  local attempt
  for attempt in 1 2 3; do
    if git push -q origin HEAD; then
      echo "pushed: $msg (attempt $attempt)"
      return 0
    fi
    echo "push rejected (attempt $attempt); syncing with origin"
    git fetch --quiet origin main || true
    # Stash tracked modifications before rebasing. A caller's own driver writes
    # its closing lines to logs/ AFTER this function commits -- `tee -a` and a
    # final `echo` both land post-commit -- so the tree is reliably dirty by the
    # time a rejected push needs a rebase, and git refuses with "cannot rebase:
    # You have unstaged changes". Observed live 2026-09-12: every stage green,
    # parse clean, and the run still exited 1 because the MLB Models GH Action
    # had pushed in the meantime.
    #
    # BOUNDED, not --autostash: autostash takes the whole tree unconditionally,
    # and an in-flight parse in a sibling process can leave tens of thousands of
    # modified parquets, where it dies with "patch too large". Refuse loudly
    # above the bound instead of stashing a data tree out from under another job.
    local _dirty _stashed=0
    _dirty=$(git diff --name-only | wc -l | tr -d ' ')
    if [ "$_dirty" -gt "${SDV_MAX_STASH_FILES:-50}" ]; then
      echo "::error ::${_dirty} modified files -- refusing to stash for a rebase" >&2
      echo "       (another job may be mid-write; commit is safe locally)" >&2
      return 1
    fi
    if [ "$_dirty" -gt 0 ]; then
      git stash push --quiet --include-untracked=false 2>/dev/null && _stashed=1
    fi
    if ! git rebase --merge origin/main >/dev/null 2>&1; then
      git rebase --abort >/dev/null 2>&1 || true
      [ "$_stashed" = 1 ] && git stash pop --quiet 2>/dev/null
      echo "::error ::cannot rebase onto origin/main for: $msg"
      return 1
    fi
    [ "$_stashed" = 1 ] && git stash pop --quiet 2>/dev/null
  done
  echo "::error ::push still rejected after 3 attempts: $msg"
  return 1
}
