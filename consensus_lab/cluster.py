"""Deterministic Raft-style cluster simulation."""

from __future__ import annotations

from collections.abc import Iterable

from consensus_lab.models import (
    AppendResult,
    ClusterError,
    ElectionResult,
    LogEntry,
    Node,
    Role,
    validate_command,
)


class Cluster:
    """A deterministic consensus simulator with elections, partitions, and replication."""

    def __init__(self, node_ids: Iterable[str]) -> None:
        ids = tuple(node_ids)
        if len(ids) < 1:
            raise ClusterError("a cluster needs at least one node")
        if len(set(ids)) != len(ids):
            raise ClusterError("node ids must be unique")
        self.nodes: dict[str, Node] = {node_id: Node(node_id) for node_id in ids}
        self.blocked_links: set[tuple[str, str]] = set()
        self.leader_id: str | None = None

    @classmethod
    def with_size(cls, size: int) -> "Cluster":
        if size < 1:
            raise ClusterError("cluster size must be at least one")
        return cls(f"node-{index}" for index in range(1, size + 1))

    @property
    def majority(self) -> int:
        return (len(self.nodes) // 2) + 1

    def elect_leader(self, candidate_id: str) -> ElectionResult:
        candidate = self._node(candidate_id)
        self._require_online(candidate)
        term = max(node.current_term for node in self.nodes.values()) + 1
        candidate.current_term = term
        candidate.role = Role.CANDIDATE
        candidate.voted_for = candidate.id
        votes = {candidate.id}

        for voter in self.nodes.values():
            if voter.id == candidate.id:
                continue
            if self._request_vote(candidate, voter, term):
                votes.add(voter.id)

        won = len(votes) >= self.majority
        if won:
            self._clear_leaders(term)
            candidate.role = Role.LEADER
            candidate.current_term = term
            self.leader_id = candidate.id
        return ElectionResult(term=term, candidate_id=candidate.id, votes=tuple(sorted(votes)), won=won)

    def propose(self, command: str) -> AppendResult:
        validate_command(command)
        leader = self._current_leader()
        self._require_online(leader)
        entry = LogEntry(index=leader.last_log_index + 1, term=leader.current_term, command=command)
        leader.append_entry(entry)
        acknowledgements = {leader.id}

        for follower in self.nodes.values():
            if follower.id == leader.id:
                continue
            if self._replicate_to_follower(leader, follower):
                acknowledgements.add(follower.id)

        committed = len(acknowledgements) >= self.majority
        if committed:
            self._commit(entry.index, acknowledgements)
        return AppendResult(
            leader_id=leader.id,
            entry=entry,
            acknowledgements=tuple(sorted(acknowledgements)),
            committed=committed,
        )

    def sync_leader(self) -> tuple[str, ...]:
        leader = self._current_leader()
        self._require_online(leader)
        synced = {leader.id}
        for follower in self.nodes.values():
            if follower.id == leader.id:
                continue
            if self._replicate_to_follower(leader, follower):
                follower.commit_index = min(leader.commit_index, follower.last_log_index)
                follower.apply_commits()
                synced.add(follower.id)
        return tuple(sorted(synced))

    def crash(self, node_id: str) -> None:
        node = self._node(node_id)
        node.online = False
        if self.leader_id == node_id:
            self.leader_id = None
        node.role = Role.FOLLOWER

    def recover(self, node_id: str) -> None:
        node = self._node(node_id)
        node.online = True
        node.role = Role.FOLLOWER

    def partition(self, group_a: Iterable[str], group_b: Iterable[str]) -> None:
        left = tuple(group_a)
        right = tuple(group_b)
        for node_id in (*left, *right):
            self._node(node_id)
        for source in left:
            for target in right:
                self.blocked_links.add((source, target))
                self.blocked_links.add((target, source))

    def heal(self) -> None:
        self.blocked_links.clear()

    def status(self) -> dict[str, object]:
        return {
            "leader_id": self.leader_id,
            "majority": self.majority,
            "blocked_links": sorted(f"{source}->{target}" for source, target in self.blocked_links),
            "nodes": {node_id: node.as_dict() for node_id, node in sorted(self.nodes.items())},
        }

    def value(self, node_id: str, key: str) -> str | None:
        return self._node(node_id).state_machine.get(key)

    def _request_vote(self, candidate: Node, voter: Node, term: int) -> bool:
        if not self._can_communicate(candidate.id, voter.id):
            return False
        if term < voter.current_term:
            return False
        if term > voter.current_term:
            voter.transition_to_follower(term)
        if voter.voted_for not in (None, candidate.id):
            return False
        if not self._candidate_log_is_up_to_date(candidate, voter):
            return False
        voter.voted_for = candidate.id
        return True

    def _replicate_to_follower(self, leader: Node, follower: Node) -> bool:
        if not self._can_communicate(leader.id, follower.id):
            return False
        if follower.current_term > leader.current_term:
            leader.transition_to_follower(follower.current_term)
            self.leader_id = None
            return False
        follower.transition_to_follower(leader.current_term)
        for entry in leader.log:
            follower.append_entry(entry)
        return True

    def _commit(self, index: int, acknowledgements: set[str]) -> None:
        leader = self._current_leader()
        leader.commit_index = max(leader.commit_index, index)
        leader.apply_commits()
        for node_id in acknowledgements:
            follower = self.nodes[node_id]
            follower.commit_index = min(leader.commit_index, follower.last_log_index)
            follower.apply_commits()

    def _clear_leaders(self, term: int) -> None:
        for node in self.nodes.values():
            if node.current_term <= term and node.role == Role.LEADER:
                node.transition_to_follower(term)

    def _candidate_log_is_up_to_date(self, candidate: Node, voter: Node) -> bool:
        if candidate.last_log_term != voter.last_log_term:
            return candidate.last_log_term > voter.last_log_term
        return candidate.last_log_index >= voter.last_log_index

    def _can_communicate(self, source: str, target: str) -> bool:
        source_node = self._node(source)
        target_node = self._node(target)
        if not source_node.online or not target_node.online:
            return False
        return (source, target) not in self.blocked_links

    def _current_leader(self) -> Node:
        if self.leader_id is None:
            raise ClusterError("no leader has been elected")
        leader = self._node(self.leader_id)
        if leader.role != Role.LEADER:
            raise ClusterError("recorded leader is not in leader role")
        return leader

    def _require_online(self, node: Node) -> None:
        if not node.online:
            raise ClusterError(f"{node.id} is offline")

    def _node(self, node_id: str) -> Node:
        try:
            return self.nodes[node_id]
        except KeyError as error:
            raise ClusterError(f"unknown node: {node_id}") from error
