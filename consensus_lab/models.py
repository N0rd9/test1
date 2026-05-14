"""Core data models for the consensus simulator."""

from __future__ import annotations

import shlex
from dataclasses import asdict, dataclass, field
from enum import Enum


class ClusterError(Exception):
    """Raised when a simulation action cannot be applied."""


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True, slots=True)
class LogEntry:
    index: int
    term: int
    command: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class Node:
    id: str
    role: Role = Role.FOLLOWER
    current_term: int = 0
    voted_for: str | None = None
    online: bool = True
    commit_index: int = 0
    last_applied: int = 0
    log: list[LogEntry] = field(default_factory=list)
    state_machine: dict[str, str] = field(default_factory=dict)

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def transition_to_follower(self, term: int) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = Role.FOLLOWER

    def append_entry(self, entry: LogEntry) -> None:
        if entry.index <= len(self.log):
            existing = self.log[entry.index - 1]
            if existing.term == entry.term and existing.command == entry.command:
                return
            del self.log[entry.index - 1 :]
        self.log.append(entry)

    def apply_commits(self) -> None:
        while self.last_applied < self.commit_index:
            entry = self.log[self.last_applied]
            _apply_command(self.state_machine, entry.command)
            self.last_applied += 1

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "role": self.role.value,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "online": self.online,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log": [entry.as_dict() for entry in self.log],
            "state_machine": dict(self.state_machine),
        }


@dataclass(frozen=True, slots=True)
class ElectionResult:
    term: int
    candidate_id: str
    votes: tuple[str, ...]
    won: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AppendResult:
    leader_id: str
    entry: LogEntry
    acknowledgements: tuple[str, ...]
    committed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "leader_id": self.leader_id,
            "entry": self.entry.as_dict(),
            "acknowledgements": list(self.acknowledgements),
            "committed": self.committed,
        }


def validate_command(command: str) -> None:
    parts = shlex.split(command)
    if len(parts) >= 3 and parts[0] == "set":
        return
    if len(parts) == 2 and parts[0] == "delete":
        return
    raise ClusterError("commands must be 'set <key> <value>' or 'delete <key>'")


def _apply_command(state: dict[str, str], command: str) -> None:
    validate_command(command)
    parts = shlex.split(command)
    if parts[0] == "set":
        state[parts[1]] = " ".join(parts[2:])
    elif parts[0] == "delete":
        state.pop(parts[1], None)
