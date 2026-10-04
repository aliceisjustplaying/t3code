#!/usr/bin/env bash
# Rebuilds the `stack` branch: upstream/main, then each PR in prs.txt, then `ours`.
# Reads prs.txt from `ours`. Needs a clean worktree. Conflict resolutions recorded in
# rr-cache/ (copied from .git/rr-cache after resolving) are replayed by rerere.
set -euo pipefail

merge() {
  if ! git merge --no-ff --no-edit -m "$2" "$1"; then
    if [ -n "$(git diff --name-only --diff-filter=U)" ]; then
      echo "build-stack: conflict in '$2'; resolve and commit, or drop it from prs.txt" >&2
      exit 1
    fi
    git commit --no-edit
  fi
}

main() {
  cd "$(git rev-parse --show-toplevel)"
  git diff --quiet && git diff --cached --quiet || { echo "build-stack: worktree not clean" >&2; exit 1; }
  git fetch upstream main
  git config rerere.enabled true
  git config rerere.autoupdate true
  local rr
  rr=$(git rev-parse --git-path rr-cache)
  mkdir -p "$rr"
  git archive ours scripts/fork/rr-cache | tar -x --strip-components=3 -C "$rr"
  local prs
  prs=$(git show ours:scripts/fork/prs.txt | sed -e 's/#.*//' | awk 'NF { print $1 }')
  git checkout -B stack upstream/main
  for pr in $prs; do
    git fetch upstream "pull/$pr/head"
    merge FETCH_HEAD "Merge upstream PR #$pr"
  done
  merge ours "Merge ours"
}

main "$@"
