#!/usr/bin/env bash
# Build away from the running checkout. Only reviewed PR commits enter the stack.
set -euo pipefail

main() {
  source_root=$(git rev-parse --show-toplevel)
  cd "$source_root"
  if git worktree list --porcelain | grep -qx 'branch refs/heads/fork-stack'; then
    echo 'build-stack: fork-stack is checked out; switch that checkout before rebuilding it' >&2
    return 1
  fi
  ours=$(git rev-parse ours)
  git fetch upstream main
  base=$(git rev-parse upstream/main)
  scratch=$(mktemp -d)
  worktree="$scratch/stack"
  trap 'git -C "$source_root" worktree remove --force "$worktree" 2>/dev/null || true; rm -rf "$scratch"' EXIT
  git worktree add --detach "$worktree" "$base"
  manifest="$scratch/prs.txt"
  git show "$ours:scripts/fork/prs.txt" > "$manifest"
  cd "$worktree"
  rr=$(git rev-parse --git-path rr-cache)
  mkdir -p "$rr"
  git archive "$ours" scripts/fork/rr-cache | tar -x --strip-components=3 -C "$rr"
  while read -r pr sha note; do
    [[ -z "$pr" || "$pr" == \#* ]] && continue
    [[ "$pr" =~ ^[0-9]+$ && "$sha" =~ ^[0-9a-f]{40}$ ]] || {
      echo "build-stack: expected a PR number and full reviewed SHA: $pr" >&2
      return 1
    }
    git cat-file -e "$sha^{commit}" 2>/dev/null || git fetch upstream "$sha"
    if ! git -c rerere.enabled=true -c rerere.autoupdate=true merge --no-ff --no-edit -m "Merge upstream PR #$pr" "$sha"; then
      if [[ -n "$(git diff --name-only --diff-filter=U)" ]]; then
        echo "build-stack: unresolved conflict in PR #$pr; fork-stack was not changed" >&2
        return 1
      fi
      git commit --no-edit
    fi
  done < "$manifest"
  while read -r pr sha note; do
    [[ -z "$pr" || "$pr" == \#* ]] && continue
    if git cat-file -e "$ours:scripts/fork/fixups/$pr.patch" 2>/dev/null; then
      git show "$ours:scripts/fork/fixups/$pr.patch" | git apply --index
      git commit -m "Fix up upstream PR #$pr"
    fi
  done < "$manifest"
  if ! git -c rerere.enabled=true -c rerere.autoupdate=true merge --no-ff --no-edit -m 'Merge ours' "$ours"; then
    if [[ -n "$(git diff --name-only --diff-filter=U)" ]]; then
      echo "build-stack: unresolved conflict in ours; fork-stack was not changed" >&2
      return 1
    fi
    git commit --no-edit
  fi
  git branch -f fork-stack HEAD
  cd "$source_root"
}

main "$@"
