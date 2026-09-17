"""scripts/_git_commit.sh -- a rejected push must rebase past a dirty tracked log.

Live shape (2026-09-12..17): the MLB capture commits, a GH Action has pushed in
the meantime, and the driver has already appended its closing lines to a
tracked log. The helper has to stash that log, rebase, push, and restore it.
"""

import os
import subprocess
from pathlib import Path

HELPER = Path(__file__).resolve().parents[1] / "scripts" / "_git_commit.sh"


def _git(cwd: Path, *args: str, env: dict) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
    ).stdout


def test_rejected_push_rebases_past_dirty_tracked_log(tmp_path: Path):
    env = {
        **os.environ,
        "HOME": str(tmp_path),  # no global hooks/config from the host
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    origin, box, other = tmp_path / "origin.git", tmp_path / "box", tmp_path / "other"
    _git(tmp_path, "init", "-q", "--bare", str(origin), env=env)
    _git(tmp_path, "clone", "-q", str(origin), str(box), env=env)
    _git(box, "checkout", "-qb", "main", env=env)  # git<2.28 clones empty as master
    (box / "logs").mkdir()
    (box / "logs" / "run.log").write_text("start\n")
    _git(box, "add", ".", env=env)
    _git(box, "commit", "-qm", "chore: init", env=env)
    _git(box, "push", "-q", "origin", "HEAD:main", env=env)

    # Remote moves (the MLB Models GH Action).
    _git(tmp_path, "clone", "-q", "-b", "main", str(origin), str(other), env=env)
    (other / "models.txt").write_text("m\n")
    _git(other, "add", ".", env=env)
    _git(other, "commit", "-qm", "MLB Models update (Start: 2026 End: 2026)", env=env)
    _git(other, "push", "-q", "origin", "HEAD:main", env=env)

    # The capture: new data, plus the driver's log tail already on disk.
    (box / "mlb").mkdir()
    (box / "mlb" / "games.txt").write_text("g\n")
    (box / "logs" / "run.log").write_text("start\npost-commit tail\n")

    rc = subprocess.run(
        ["bash", "-c", f'source "{HELPER}" && sdv_commit_push "feat(mlb): capture" mlb/'],
        cwd=box,
        env=env,
        capture_output=True,
        text=True,
    )

    assert rc.returncode == 0, rc.stdout + rc.stderr
    log = _git(origin, "log", "--format=%s", "main", env=env)
    assert "feat(mlb): capture" in log and "MLB Models update" in log
    assert (box / "logs" / "run.log").read_text() == "start\npost-commit tail\n"
