"""Command line interface for Consensus Lab."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from consensus_lab.cluster import Cluster
from consensus_lab.models import ClusterError

app = typer.Typer(help="Consensus Lab: deterministic Raft-style cluster simulator.")


@app.command()
def demo(
    nodes: int = typer.Option(5, "--nodes", "-n", help="Number of nodes in the demo cluster."),
    pretty: bool = typer.Option(True, "--pretty/--compact", help="Pretty-print JSON output."),
) -> None:
    """Run a built-in election, partition, recovery, and replication scenario."""
    cluster = Cluster.with_size(nodes)
    timeline: list[dict[str, Any]] = []

    election = cluster.elect_leader("node-1")
    timeline.append({"action": "elect node-1", "result": election.as_dict()})

    first_append = cluster.propose("set course distributed-systems")
    timeline.append({"action": "propose course", "result": first_append.as_dict()})

    minority = ["node-1"]
    majority = [f"node-{index}" for index in range(2, nodes + 1)]
    cluster.partition(minority, majority)
    timeline.append({"action": "partition leader away from majority", "status": cluster.status()})

    second_append = cluster.propose("set unsafe should-not-commit")
    timeline.append({"action": "propose without quorum", "result": second_append.as_dict()})

    cluster.heal()
    synced = cluster.sync_leader()
    timeline.append({"action": "heal and sync", "synced": list(synced), "status": cluster.status()})

    _echo_json(timeline, pretty)


@app.command()
def simulate(
    script: Path = typer.Argument(..., exists=True, dir_okay=False, help="JSON scenario file."),
    pretty: bool = typer.Option(True, "--pretty/--compact", help="Pretty-print JSON output."),
) -> None:
    """Run a JSON scenario file and print the final cluster state."""
    try:
        payload = json.loads(script.read_text(encoding="utf-8"))
        cluster = Cluster.with_size(int(payload.get("nodes", 5)))
        timeline = _run_actions(cluster, payload.get("actions", []))
    except (json.JSONDecodeError, OSError, ValueError, ClusterError) as error:
        raise typer.BadParameter(str(error)) from error
    _echo_json({"timeline": timeline, "status": cluster.status()}, pretty)


def _run_actions(cluster: Cluster, actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    for action in actions:
        name = action.get("action")
        if name == "elect":
            result = cluster.elect_leader(str(action["node"])).as_dict()
        elif name == "propose":
            result = cluster.propose(str(action["command"])).as_dict()
        elif name == "partition":
            cluster.partition(action["group_a"], action["group_b"])
            result = cluster.status()
        elif name == "heal":
            cluster.heal()
            result = cluster.status()
        elif name == "crash":
            cluster.crash(str(action["node"]))
            result = cluster.status()
        elif name == "recover":
            cluster.recover(str(action["node"]))
            result = cluster.status()
        elif name == "sync":
            result = {"synced": list(cluster.sync_leader())}
        else:
            raise ClusterError(f"unknown action: {name}")
        timeline.append({"action": name, "result": result})
    return timeline


def _echo_json(payload: object, pretty: bool) -> None:
    typer.echo(json.dumps(payload, indent=2 if pretty else None, sort_keys=True))
