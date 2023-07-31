"""A module providing wrap over the git command.
"""
from pathlib import Path
import subprocess as sp


class Repo:
    """A local Git repository."""

    def __init__(self, root: str | Path):
        self._root = str(root)
        self._remote = self.remote()

    def remote(self) -> str:
        """Run the "git remote" command."""
        proc = sp.run(
            ["git", "-C", self._root, "remote"], check=True, capture_output=True
        )
        return proc.stdout.strip().decode()

    def pull(self, branch: str) -> None:
        """Run the "git pull" command."""
        sp.run(["git", "-C", self._root, "pull", self._remote, branch], check=True)

    def push(self, branch: str = "") -> None:
        """Run the "git push" command."""
        if not branch:
            branch = self.active_branch()
        sp.run(["git", "-C", self._root, "push", self._remote, branch], check=True)

    def _branch(self) -> list[str]:
        proc = sp.run(
            ["git", "-C", self._root, "branch"], check=True, capture_output=True
        )
        return proc.stdout.strip().decode().split("\n")

    def active_branch(self) -> str:
        """Get the active branch of the repository."""
        lines = self._branch()
        return next(line[2:] for line in lines if line.startswith("* "))

    def branch(self):
        """Run the "git branch" command to get all branches of the repository."""
        return [b[2:] for b in self._branch()]

    def add(self, pattern: str = "."):
        """Run the "git add" command."""
        sp.run(["git", "-C", self._root, "add", pattern], check=True)

    def commit(self, message: str):
        """Run the "git commit" command."""
        sp.run(["git", "-C", self._root, "commit", "-m", message], check=True)

    def status(self):
        """Run the "git status" command."""
        sp.run(["git", "-C", self._root, "status"], check=True)

    def tag(self, tag="") -> sp.CompletedProcess | list[str]:
        """Run the "git tag" command."""
        if tag:
            return sp.run(["git", "tag", tag], check=True)
        proc = sp.run(["git", "tag"], check=True, capture_output=True)
        return proc.stdout.strip().decode().split("\n")

    def checkout(self, branch: str = "", new: str = ""):
        """Run the "git checkout" command."""
        if new:
            return sp.run(["git", "checkout", "-b", new], check=True)
        if not branch:
            raise ValueError("Either new or branch is required!")
        return sp.run(["git", "checkout", branch], check=True)
