import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

NodeTest.test("stack builds use reviewed SHAs and leave the active checkout intact", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-stack-"));
  const upstream = NodePath.join(root, "upstream"),
    fork = NodePath.join(root, "fork");
  const git = (cwd, ...args) =>
    NodeChildProcess.execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  const commit = (cwd, message) => {
    git(cwd, "add", ".");
    git(
      cwd,
      "-c",
      "user.name=Stack Test",
      "-c",
      "user.email=stack@example.invalid",
      "commit",
      "-m",
      message,
    );
  };
  try {
    NodeFS.mkdirSync(upstream);
    git(upstream, "init", "-b", "main");
    NodeFS.writeFileSync(NodePath.join(upstream, "base"), "base");
    commit(upstream, "base");
    git(upstream, "checkout", "-b", "pr");
    NodeFS.writeFileSync(NodePath.join(upstream, "reviewed"), "reviewed");
    commit(upstream, "reviewed");
    const reviewed = git(upstream, "rev-parse", "HEAD");
    NodeFS.writeFileSync(NodePath.join(upstream, "unreviewed"), "unreviewed");
    commit(upstream, "unreviewed");
    git(upstream, "update-ref", "refs/pull/1/head", "HEAD");
    git(root, "clone", "-b", "main", upstream, fork);
    git(fork, "remote", "rename", "origin", "upstream");
    git(fork, "checkout", "-b", "ours");
    NodeFS.mkdirSync(NodePath.join(fork, "scripts/fork/rr-cache"), { recursive: true });
    NodeFS.writeFileSync(NodePath.join(fork, "scripts/fork/rr-cache/.keep"), "");
    NodeFS.writeFileSync(NodePath.join(fork, "scripts/fork/prs.txt"), `1 ${reviewed} reviewed\n`);
    NodeFS.copyFileSync(
      NodeURL.fileURLToPath(new URL("./build-stack.sh", import.meta.url)),
      NodePath.join(fork, "scripts/fork/build-stack.sh"),
    );
    commit(fork, "fork tooling");
    const before = git(fork, "rev-parse", "HEAD");
    NodeFS.writeFileSync(NodePath.join(fork, "local-work"), "keep me");
    NodeChildProcess.execFileSync("bash", ["scripts/fork/build-stack.sh"], {
      cwd: fork,
      stdio: "pipe",
    });
    NodeAssert.equal(git(fork, "rev-parse", "HEAD"), before);
    NodeAssert.equal(git(fork, "branch", "--show-current"), "ours");
    NodeAssert.equal(git(fork, "show", "fork-stack:reviewed"), "reviewed");
    NodeAssert.equal(
      NodeChildProcess.spawnSync("git", ["cat-file", "-e", "fork-stack:unreviewed"], { cwd: fork })
        .status,
      128,
    );
    NodeAssert.equal(git(fork, "worktree", "list", "--porcelain").match(/^worktree /gm).length, 1);
    git(fork, "checkout", "fork-stack");
    NodeAssert.notEqual(
      NodeChildProcess.spawnSync("bash", ["scripts/fork/build-stack.sh"], { cwd: fork }).status,
      0,
    );
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
});
