import pytest

from consensus_lab import Cluster, ClusterError, Role


def test_leader_election_requires_majority() -> None:
    cluster = Cluster.with_size(5)
    cluster.partition(["node-1"], ["node-2", "node-3", "node-4", "node-5"])

    result = cluster.elect_leader("node-1")

    assert not result.won
    assert result.votes == ("node-1",)
    assert cluster.status()["leader_id"] is None


def test_candidate_with_majority_becomes_leader() -> None:
    cluster = Cluster.with_size(5)

    result = cluster.elect_leader("node-3")

    assert result.won
    assert set(result.votes) == {"node-1", "node-2", "node-3", "node-4", "node-5"}
    assert cluster.status()["leader_id"] == "node-3"
    assert cluster.nodes["node-3"].role is Role.LEADER


def test_command_commits_to_majority_and_applies_state() -> None:
    cluster = Cluster.with_size(3)
    cluster.elect_leader("node-1")

    result = cluster.propose("set topic consensus")

    assert result.committed
    assert set(result.acknowledgements) == {"node-1", "node-2", "node-3"}
    assert cluster.value("node-1", "topic") == "consensus"
    assert cluster.value("node-2", "topic") == "consensus"
    assert cluster.value("node-3", "topic") == "consensus"


def test_isolated_leader_cannot_commit_new_entries() -> None:
    cluster = Cluster.with_size(5)
    cluster.elect_leader("node-1")
    cluster.partition(["node-1"], ["node-2", "node-3", "node-4", "node-5"])

    result = cluster.propose("set risk high")

    assert not result.committed
    assert result.acknowledgements == ("node-1",)
    assert cluster.value("node-1", "risk") is None


def test_healed_cluster_syncs_committed_entries() -> None:
    cluster = Cluster.with_size(5)
    cluster.elect_leader("node-1")
    cluster.partition(["node-5"], ["node-1", "node-2", "node-3", "node-4"])

    result = cluster.propose("set feature replicated-log")
    assert result.committed
    assert cluster.value("node-5", "feature") is None

    cluster.heal()
    synced = cluster.sync_leader()

    assert "node-5" in synced
    assert cluster.value("node-5", "feature") == "replicated-log"


def test_crashed_leader_must_be_replaced() -> None:
    cluster = Cluster.with_size(3)
    cluster.elect_leader("node-1")

    cluster.crash("node-1")
    with pytest.raises(ClusterError):
        cluster.propose("set unavailable true")

    result = cluster.elect_leader("node-2")

    assert result.won
    assert cluster.status()["leader_id"] == "node-2"


def test_invalid_command_is_rejected() -> None:
    cluster = Cluster.with_size(3)
    cluster.elect_leader("node-1")

    with pytest.raises(ClusterError):
        cluster.propose("increment counter")
