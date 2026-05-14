"""Consensus Lab public interface."""

from consensus_lab.cluster import Cluster
from consensus_lab.models import (
    AppendResult,
    ClusterError,
    ElectionResult,
    LogEntry,
    Node,
    Role,
)

__all__ = [
    "AppendResult",
    "Cluster",
    "ClusterError",
    "ElectionResult",
    "LogEntry",
    "Node",
    "Role",
]
