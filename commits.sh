#!/usr/bin/env bash
# Commit history for the Raft-Pyro implementation, in order.
# Stage the corresponding changes for each step, then run the matching commit line.

set -e

git add server.py
git commit -m "add RaftNode class with FOLLOWER/CANDIDATE/LEADER states and Pyro5 exposure"

git add server.py
git commit -m "add request_vote RPC handler with term comparison and single-vote-per-term rule"

git add server.py
git commit -m "add append_entries RPC handler with leader recognition and step-down on higher term"

git add server.py
git commit -m "add run loop driving election timeout and periodic heartbeats"

git add server.py
git commit -m "add _start_election with randomized timeout, self-vote and majority count"

git add server.py
git commit -m "add _send_heartbeats and leader step-down when discovering a higher term"

git add server.py
git commit -m "add _register_as_leader to publish raft.leader in the Pyro name server"

git add server.py
git commit -m "add get_status returning state, term, leader_id, voted_for and timing fields"

git add main.py
git commit -m "add main entry point with Pyro daemon, name server registration and threaded run loop"

git add run.sh
git commit -m "add run.sh launching the four nodes in parallel"

git add client.py
git commit -m "add client.py interacting with the cluster via raft.leader"

git add server.py
git commit -m "fix peers list to exclude self, preventing leader from demoting itself via self-heartbeat"

git add server.py
git commit -m "add threading.Lock to serialize state mutations across concurrent RPC handlers"

git add server.py
git commit -m "fix HEARTBEAT_INTERVAL and RPC_TIMEOUT to stay below ELECTION_TIMEOUT_MIN"

git add server.py main.py
git commit -m "add shutdown method and loopCondition for clean node termination via Pyro RPC"

git add server.py
git commit -m "add safe=False to raft.leader registration to overwrite stale entries from prior runs"
