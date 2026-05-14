# Consensus Lab

Consensus Lab is a deterministic Raft-style simulator for distributed systems coursework. It models leader elections, majority quorum, replicated logs, node crashes, network partitions, recovery, and a small replicated key-value state machine.

The project is intentionally deterministic: there are no random timeouts or background threads. Every state transition is triggered by a clear simulation command, which makes the system easier to test, debug, and explain in a university setting.

## Features

- Raft-style roles: follower, candidate, leader
- Majority-based leader election
- RequestVote-style log freshness checks
- Replicated append-only logs
- Majority commit rules
- Key-value state machine commands
- Node crash and recovery simulation
- Network partition and healing simulation
- JSON scenario runner
- Test suite for election, quorum, commit, partition, sync, and crash behavior
- GitHub Actions workflow for linting and tests

## Why This Project Matters

Consensus protocols are one of the harder topics in distributed systems because correctness depends on what happens during failure. Consensus Lab makes those failure modes visible and repeatable.

It demonstrates:

- distributed systems reasoning
- quorum-based fault tolerance
- replicated log design
- deterministic simulation
- state machine replication
- testable architecture

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

## Run the Demo

```powershell
consensus-lab demo
```

The demo:

1. creates a five-node cluster
2. elects `node-1` as leader
3. commits a replicated command
4. partitions the leader away from the majority
5. shows that the isolated leader cannot commit safely
6. heals the network and syncs reachable nodes

## Run a Scenario File

Create `scenario.json`:

```json
{
  "nodes": 5,
  "actions": [
    {"action": "elect", "node": "node-1"},
    {"action": "propose", "command": "set course distributed-systems"},
    {"action": "partition", "group_a": ["node-5"], "group_b": ["node-1", "node-2", "node-3", "node-4"]},
    {"action": "propose", "command": "set project consensus-lab"},
    {"action": "heal"},
    {"action": "sync"}
  ]
}
```

Run it:

```powershell
consensus-lab simulate .\scenario.json
```

## Supported Commands

State machine commands:

- `set <key> <value>`
- `delete <key>`

Scenario actions:

- `elect`
- `propose`
- `partition`
- `heal`
- `crash`
- `recover`
- `sync`

## Development

Run tests:

```powershell
pytest
```

Run linting:

```powershell
ruff check .
```

## Project Structure

```text
consensus_lab/
  cluster.py   deterministic cluster simulator
  models.py    node, log, role, and result models
  cli.py       demo and JSON scenario runner
tests/
  test_cluster.py
```

## Possible Extensions

- randomized election timeouts
- persistent logs stored on disk
- visual timeline export
- safety invariant checker
- web dashboard for partitions and replicated logs
- more faithful AppendEntries conflict handling
