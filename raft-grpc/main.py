import collections
import json
import os
import threading
import time
from datetime import datetime
from random import randint

from concurrent import futures

import grpc
import client_pb2
import client_pb2_grpc
import server_pb2
import server_pb2_grpc

NODE_ID = int(os.environ["NODE_ID"]) if "NODE_ID" in os.environ else 0
CLUSTER_SIZE = int(os.environ.get("CLUSTER_SIZE", "4"))
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
PEER_HOST_PATTERN = os.environ.get("PEER_HOST_PATTERN", "raft-{i}")
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
DATA_DIR = os.environ.get("DATA_DIR", f"data/n{NODE_ID}")

RPC_TIMEOUT = 1.0
COMMIT_TIMEOUT = 5.0


class EventLog:
    """Thread-safe append-only ring buffer of node events."""

    def __init__(self, maxlen: int = 4000):
        self._events: collections.deque = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._counter = 0

    def append(self, node_id: int, msg: str) -> None:
        with self._lock:
            self._counter += 1
            self._events.append((self._counter, datetime.now(), node_id, msg))
        print(f"n{node_id}: {msg}", flush=True)

    def since(self, last_id: int) -> list[tuple]:
        with self._lock:
            return [e for e in self._events if e[0] > last_id]


EVENTS = EventLog()


class Follower:
    def __init__(self):
        self.timeout_ms = randint(150, 300)
        self.last_heartbeat = time.monotonic()


class Candidate:
    def __init__(self, votes):
        self.timeout_ms = randint(150, 300)
        self.election_start = time.monotonic()
        self.votes = votes


class LogEntry:
    def __init__(self, value, term: int):
        self.value = value
        self.term = term


class Leader:
    def __init__(self, peer_ids, log_size):
        self.next_index: dict = {pid: log_size for pid in peer_ids}
        self.match_index: dict = {pid: 0 for pid in peer_ids}
        self.heartbeat_frequency = 0.1
        self.last_sent_heartbeat = time.monotonic()


State = Follower | Candidate | Leader


class Server(server_pb2_grpc.ServerServicer, client_pb2_grpc.ClientAPIServicer):
    def __init__(self, id):
        self.id = id
        self.state: State = Follower()
        self.term = 0
        # id of the node this one voted for in the current term (None = no vote)
        self.voted_for: int | None = None
        self.logs: list[LogEntry] = []
        self.leader_id = id
        self.lock = threading.Lock()
        # signaled whenever commit_id advances, so pending client writes
        # blocked in Publish can re-check whether their entry committed
        self.commit_cv = threading.Condition(self.lock)
        self.time_scale: float = 1.0
        self.peers: dict[int, server_pb2_grpc.ServerStub] = {}
        self.crashed: bool = False
        self.partition_id: int = 0
        self.partition_map: dict[int, int] = {}
        self.commit_id: int = -1
        self._load()

    def _persist(self) -> None:
        data = {
            "term": self.term,
            "voted_for": self.voted_for,
            "commit_id": self.commit_id,
            "logs": [{"value": e.value, "term": e.term} for e in self.logs],
        }
        tmp = os.path.join(DATA_DIR, "state.json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, os.path.join(DATA_DIR, "state.json"))

    def _load(self) -> None:
        try:
            with open(os.path.join(DATA_DIR, "state.json")) as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return
        self.term = data["term"]
        self.voted_for = data["voted_for"]
        self.commit_id = data["commit_id"]
        self.logs = [LogEntry(e["value"], e["term"]) for e in data["logs"]]
        EVENTS.append(
            self.id,
            f"RESTORED from disk: term={self.term} log_size={len(self.logs)} "
            f"commit={self.commit_id} voted_for={self.voted_for}",
        )

    def _clear_state(self) -> None:
        self.term = 0
        self.voted_for = None
        self.commit_id = -1
        self.logs = []
        
    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _peer_partition(self, peer_id: int) -> int:
        return self.partition_map.get(peer_id, 0)

    def _can_reach(self, peer_id: int) -> bool:
        return self._peer_partition(peer_id) == self.partition_id

    # -- gRPC server-side RPCs (called by peers) -- #

    def AppendEntries(self, request, context):
        with self.lock:
            if self.crashed:
                context.abort(grpc.StatusCode.UNAVAILABLE, f"server {self.id} is down")
            if not self._can_reach(request.leader_id):
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    f"server {self.id} unreachable from n{request.leader_id} (partitioned)",
                )
            if self.term > request.term:
                EVENTS.append(
                    self.id,
                    f"reject ae from n{request.leader_id} t{request.term} (mine={self.term})",
                )
                return server_pb2.AppendEntriesResponse(
                    term=self.term, success=False, last_log_index=len(self.logs)
                )
            was_leader_or_candidate = not isinstance(self.state, Follower)
            if was_leader_or_candidate:
                EVENTS.append(self.id, f"step down on ae from n{request.leader_id} t{request.term}")
                self.state = Follower()
                self.commit_cv.notify_all()  # wake pending client writes
            self.state.last_heartbeat = time.monotonic()
            if self.term < request.term:
                self.term = request.term
                self.voted_for = None
                self._persist()
            self.leader_id = request.leader_id

            if request.prev_log_index >= len(self.logs):
                if request.entries:
                    EVENTS.append(
                        self.id,
                        f"reject ae from n{request.leader_id}: gap (have {len(self.logs)}, need prev={request.prev_log_index})",
                    )
                return server_pb2.AppendEntriesResponse(
                    term=self.term, success=False, last_log_index=len(self.logs)
                )
            if (
                request.prev_log_index >= 0
                and self.logs[request.prev_log_index].term != request.prev_log_term
            ):
                EVENTS.append(
                    self.id,
                    f"reject ae from n{request.leader_id}: term mismatch at idx {request.prev_log_index}",
                )
                return server_pb2.AppendEntriesResponse(
                    term=self.term, success=False, last_log_index=len(self.logs)
                )

            new_entries = [LogEntry(e.value, e.term) for e in request.entries]
            old_len = len(self.logs)
            old_commit = self.commit_id
            self.logs = self.logs[: request.prev_log_index + 1] + new_entries
            # never commit past our own log end (leader_commit is global)
            self.commit_id = max(
                self.commit_id, min(request.leader_commit, len(self.logs) - 1)
            )
            if new_entries or len(self.logs) != old_len or self.commit_id != old_commit:
                self._persist()
            if request.entries:
                EVENTS.append(
                    self.id,
                    f"<- ae from n{request.leader_id} t{request.term}: applied {len(new_entries)} entries -> log_size={len(self.logs)}",
                )
            return server_pb2.AppendEntriesResponse(
                term=self.term, success=True, last_log_index=len(self.logs)
            )

    def RequestVote(self, request, context):
        with self.lock:
            cand_id = request.candidate_id
            if self.crashed:
                context.abort(grpc.StatusCode.UNAVAILABLE, f"server {self.id} is down")
            if not self._can_reach(cand_id):
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    f"server {self.id} unreachable from n{cand_id} (partitioned)",
                )
            if self.term > request.term:
                EVENTS.append(
                    self.id, f"deny vote n{cand_id} t{request.term} (mine={self.term})"
                )
                return server_pb2.RequestVoteResponse(term=self.term, vote_granted=False)
            if self.term < request.term and not isinstance(self.state, Follower):
                EVENTS.append(
                    self.id, f"step down on vote request n{cand_id} t{request.term}"
                )
                self.state = Follower()
                self.commit_cv.notify_all()  # wake pending client writes
            if self.term < request.term:
                self.term = request.term
                self.voted_for = None
                self._persist()
            if not isinstance(self.state, Follower):
                EVENTS.append(
                    self.id, f"deny vote n{cand_id} t{request.term} (not follower)"
                )
                return server_pb2.RequestVoteResponse(term=self.term, vote_granted=False)
            if self.voted_for is not None and self.voted_for != cand_id:
                EVENTS.append(
                    self.id,
                    f"deny vote n{cand_id} t{request.term} (already voted n{self.voted_for})",
                )
                return server_pb2.RequestVoteResponse(term=self.term, vote_granted=False)

            my_last_log_term = self.logs[-1].term if len(self.logs) > 0 else -1
            my_log_size = len(self.logs)
            log_ok = (request.last_log_term > my_last_log_term) or (
                request.last_log_term == my_last_log_term
                and request.last_log_index >= my_log_size
            )
            if log_ok:
                self.state.last_heartbeat = time.monotonic()
                self.voted_for = cand_id
                self._persist()
                EVENTS.append(self.id, f"GRANT vote n{cand_id} t{request.term}")
                return server_pb2.RequestVoteResponse(term=self.term, vote_granted=True)
            EVENTS.append(
                self.id, f"deny vote n{cand_id} t{request.term} (log not up-to-date)"
            )
            return server_pb2.RequestVoteResponse(term=self.term, vote_granted=False)

    # -- ClientAPI (application operations, defined in client.proto) -- #

    def Publish(self, request, context):
        with self.lock:
            if self.crashed:
                context.abort(grpc.StatusCode.UNAVAILABLE, f"server {self.id} is down")
            if not isinstance(self.state, Leader):
                EVENTS.append(
                    self.id,
                    f"publish {request.value!r} REJECTED (not leader, hint n{self.leader_id})",
                )
                return client_pb2.PublishResponse(success=False, leader_id=self.leader_id)

            self.logs.append(LogEntry(request.value, self.term))
            self._persist()  # uncommitted entry must survive a crash
            index = len(self.logs) - 1
            term = self.term
            EVENTS.append(
                self.id, f"publish {request.value!r} @t{term} -> log_size={len(self.logs)}"
            )

            deadline = time.monotonic() + COMMIT_TIMEOUT * self.time_scale
            while self.commit_id < index:
                # lost leadership or the entry got overwritten by a new leader
                if (
                    not isinstance(self.state, Leader)
                    or len(self.logs) <= index
                    or self.logs[index].term != term
                ):
                    EVENTS.append(
                        self.id, f"publish {request.value!r} FAILED (lost leadership)"
                    )
                    return client_pb2.PublishResponse(
                        success=False, leader_id=self.leader_id
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # no quorum reached in time: reject the write
                    EVENTS.append(
                        self.id, f"publish {request.value!r} REJECTED (no quorum)"
                    )
                    return client_pb2.PublishResponse(success=False, leader_id=self.id)
                self.commit_cv.wait(remaining)

            # commit_id reached index, but wait() released the lock along the
            # way: confirm the committed entry at `index` is still OURS (same
            # term). A new leader may have overwritten that slot and committed
            # its own entry there, which also advances commit_id.
            if len(self.logs) <= index or self.logs[index].term != term:
                EVENTS.append(
                    self.id,
                    f"publish {request.value!r} FAILED (overwritten before commit)",
                )
                return client_pb2.PublishResponse(
                    success=False, leader_id=self.leader_id
                )
            EVENTS.append(self.id, f"publish {request.value!r} COMMITTED (idx {index})")
            return client_pb2.PublishResponse(success=True, leader_id=self.leader_id)

    def Consume(self, request, context):
        with self.lock:
            if self.crashed:
                context.abort(grpc.StatusCode.UNAVAILABLE, f"server {self.id} is down")
            # never expose uncommitted entries: only up to commit_id
            committed = self.logs[: self.commit_id + 1]
            return client_pb2.ConsumeResponse(
                entries=[
                    client_pb2.ConsumedEntry(value=e.value, term=e.term)
                    for e in committed
                ],
                leader_id=self.leader_id,
            )

    def GetState(self, request, context):
        with self.lock:
            now = time.monotonic()
            scale = self.time_scale
            resp = server_pb2.StateResponse(
                id=self.id,
                term=self.term,
                voted=self.voted_for is not None,
                leader_id=self.leader_id,
                log_size=len(self.logs),
                time_scale=self.time_scale,
                crashed=self.crashed,
                partition_id=self.partition_id,
                logs=[
                    server_pb2.LogView(
                        value=e.value, term=e.term, commited=self.commit_id >= i
                    )
                    for i, e in enumerate(self.logs)
                ],
            )
            if isinstance(self.state, Follower):
                resp.role = "follower"
                resp.timeout_ms = self.state.timeout_ms
                resp.ms_since_heartbeat = int(
                    (now - self.state.last_heartbeat) * 1000 / scale
                )
            elif isinstance(self.state, Candidate):
                resp.role = "candidate"
                resp.timeout_ms = self.state.timeout_ms
                resp.votes = self.state.votes
                resp.ms_since_election = int(
                    (now - self.state.election_start) * 1000 / scale
                )
            else:
                resp.role = "leader"
                for pid, idx in self.state.next_index.items():
                    resp.next_index[pid] = idx
            return resp

    def GetEvents(self, request, context):
        for ev_id, ts, nid, msg in EVENTS.since(request.since_id):
            yield server_pb2.Event(
                id=ev_id, ts=ts.isoformat(), node_id=nid, msg=msg
            )

    def SetTimeScale(self, request, context):
        with self.lock:
            self.time_scale = max(0.1, float(request.scale))
            return server_pb2.SetTimeScaleResponse(scale=self.time_scale)

    def SetPartition(self, request, context):
        with self.lock:
            old = self.partition_id
            self.partition_id = int(request.partition_id)
        if old != self.partition_id:
            EVENTS.append(self.id, f"moved to partition P{self.partition_id} (was P{old})")
        return server_pb2.SetPartitionResponse(partition_id=self.partition_id)

    def SetPartitionMap(self, request, context):
        with self.lock:
            self.partition_map = {int(k): int(v) for k, v in request.partition_map.items()}
            self.partition_id = self.partition_map.get(self.id, self.partition_id)
            return server_pb2.SetPartitionAllResponse(partition_map=self.partition_map)

    def Kill(self, request, context):
        with self.lock:
            if not self.crashed:
                self.crashed = True
                self._clear_state()
                EVENTS.append(self.id, "*** CRASHED (sim) ***")
        return server_pb2.KillResponse()

    def Restore(self, request, context):
        with self.lock:
            if self.crashed:
                self.crashed = False
                self.state = Follower()
                self._load()  
                EVENTS.append(self.id, "*** RESTORED (sim) ***")
        return server_pb2.KillResponse()

    def GetLeader(self, request, context):
        with self.lock:
            return server_pb2.LeaderResponse(leader_id=self.leader_id)

    # -- outgoing RPCs (this node acting as candidate / leader) -- #

    def broadcast(self, rpc_name, requests: dict, on_result: dict | None = None):
        def call(peer_id, req):
            stub = self.peers[peer_id]
            try:
                resp = getattr(stub, rpc_name)(req, timeout=RPC_TIMEOUT)
            except grpc.RpcError:
                return
            if on_result is not None:
                on_result[peer_id](resp)

        for peer_id, req in requests.items():
            threading.Thread(target=call, args=(peer_id, req), daemon=True).start()

    def _vote_callback(self, peer_id):
        def callback(resp):
            with self.lock:
                if resp.term > self.term:
                    EVENTS.append(
                        self.id, f"step down: n{peer_id} replied with higher term {resp.term}"
                    )
                    self.state = Follower()
                    self.term = resp.term
                    self.voted_for = None
                    self._persist()
                    self.commit_cv.notify_all()  # wake pending client writes
                    return
                if resp.vote_granted and isinstance(self.state, Candidate):
                    self.state.votes += 1
                    EVENTS.append(
                        self.id,
                        f"vote received from n{peer_id}: {self.state.votes}/{self._majority()}",
                    )
                elif not resp.vote_granted:
                    EVENTS.append(self.id, f"vote denied by n{peer_id}")

        return callback

    def request_votes(self):
        reachable = [pid for pid in self.peers if self._can_reach(pid)]
        if not reachable:
            return
        last_log_term = self.logs[-1].term if len(self.logs) > 0 else -1
        requests = {
            pid: server_pb2.RequestVoteRequest(
                term=self.term,
                candidate_id=self.id,
                last_log_index=len(self.logs),
                last_log_term=last_log_term,
            )
            for pid in reachable
        }
        peer_ids = ",".join(f"n{pid}" for pid in reachable)
        EVENTS.append(self.id, f"-> request_vote to {peer_ids} t{self.term}")
        self.broadcast(
            "RequestVote",
            requests,
            on_result={pid: self._vote_callback(pid) for pid in reachable},
        )

    def append_entries_callback_factory(self, peer_id):
        def callback(resp):
            with self.lock:
                if resp.term < self.term:
                    return
                if resp.term > self.term:
                    EVENTS.append(
                        self.id, f"step down: n{peer_id} replied with higher term {resp.term}"
                    )
                    self.state = Follower()
                    self.term = resp.term
                    self.voted_for = None
                    self._persist()
                    self.commit_cv.notify_all()  # wake pending client writes
                    return
                if not isinstance(self.state, Leader):
                    return

                prev = self.state.next_index[peer_id]
                if resp.success:
                    if resp.last_log_index != prev:
                        EVENTS.append(
                            self.id,
                            f"ack from n{peer_id}: next_index {prev} -> {resp.last_log_index}",
                        )
                    self.state.next_index[peer_id] = resp.last_log_index
                    self.state.match_index[peer_id] = resp.last_log_index - 1
                    old_commit = self.commit_id
                    for i in range(self.commit_id + 1, len(self.logs)):
                        if self.logs[i].term != self.term:
                            continue
                        amount = sum(
                            1 for p in self.peers if self.state.match_index[p] >= i
                        )
                        if amount + 1 >= self._majority():
                            self.commit_id = i
                        else:
                            break
                    if self.commit_id != old_commit:
                        self._persist()
                        EVENTS.append(self.id, f"commit advanced -> {self.commit_id}")
                        # wake up client writes waiting in Publish
                        self.commit_cv.notify_all()
                else:
                    # guided backtracking: the follower reported its log size in
                    # last_log_index, so jump straight to the end of its log
                    # instead of walking back one entry at a time
                    self.state.next_index[peer_id] = max(
                        0, min(prev - 1, resp.last_log_index)
                    )
                    EVENTS.append(
                        self.id,
                        f"nack from n{peer_id}: backing off next_index -> {self.state.next_index[peer_id]}",
                    )

        return callback

    def loop(self):
        with self.lock:
            if self.crashed:
                return
            scale = self.time_scale
            match self.state:
                case Follower(last_heartbeat=lh, timeout_ms=t):
                    if time.monotonic() - lh > (t * scale) / 1000.0:
                        self.state = Candidate(votes=1)
                        self.term += 1
                        self.voted_for = self.id  # a candidate votes for itself
                        self._persist()
                        EVENTS.append(self.id, f"election timeout -> CANDIDATE t{self.term}")
                        self.request_votes()
                case Candidate(votes=v, election_start=es, timeout_ms=t):
                    if v >= self._majority():
                        EVENTS.append(self.id, f"WON election -> LEADER t{self.term}")
                        self.state = Leader(list(self.peers.keys()), len(self.logs))
                        self.leader_id = self.id  # keep redirect hints accurate
                    elif time.monotonic() - es > (t * scale) / 1000.0:
                        EVENTS.append(self.id, "candidate timeout -> FOLLOWER")
                        self.state = Follower()
                case Leader(last_sent_heartbeat=lsh, heartbeat_frequency=hf):
                    if time.monotonic() - lsh < hf * scale:
                        return
                    requests = {}
                    on_result = {}
                    for pid in self.peers:
                        if not self._can_reach(pid):
                            continue
                        ni = self.state.next_index[pid]
                        entries = self.logs[ni:]
                        prev_log_index = ni - 1
                        prev_log_term = self.logs[ni - 1].term if ni >= 1 else 0
                        if entries:
                            EVENTS.append(
                                self.id,
                                f"-> ae to n{pid} prev=({prev_log_index},t{prev_log_term}) entries={len(entries)}",
                            )
                        requests[pid] = server_pb2.AppendEntriesRequest(
                            term=self.term,
                            leader_id=self.id,
                            prev_log_index=prev_log_index,
                            prev_log_term=prev_log_term,
                            entries=[
                                server_pb2.Entry(value=e.value, term=e.term) for e in entries
                            ],
                            leader_commit=self.commit_id,
                        )
                        on_result[pid] = self.append_entries_callback_factory(pid)
                    if requests:
                        self.broadcast("AppendEntries", requests, on_result=on_result)
                        self.state.last_sent_heartbeat = time.monotonic()


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    node = Server(NODE_ID)

    for i in range(CLUSTER_SIZE):
        if i == NODE_ID:
            continue
        channel = grpc.insecure_channel(f"{PEER_HOST_PATTERN.format(i=i)}:{GRPC_PORT}")
        node.peers[i] = server_pb2_grpc.ServerStub(channel)

    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server_pb2_grpc.add_ServerServicer_to_server(node, srv)
    client_pb2_grpc.add_ClientAPIServicer_to_server(node, srv)
    srv.add_insecure_port(f"{BIND_HOST}:{GRPC_PORT}")
    srv.start()
    print(f"node {NODE_ID} listening (gRPC) on {BIND_HOST}:{GRPC_PORT}", flush=True)

    def tick():
        while True:
            node.loop()
            time.sleep(0.01 * node.time_scale)

    threading.Thread(target=tick, daemon=True).start()
    srv.wait_for_termination()


if __name__ == "__main__":
    main()
