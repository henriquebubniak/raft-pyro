import collections
import os
import threading
import time
from datetime import datetime
from random import randint

import Pyro5
import Pyro5.api
import Pyro5.errors

NODE_ID = int(os.environ["NODE_ID"]) if "NODE_ID" in os.environ else 0
CLUSTER_SIZE = int(os.environ.get("CLUSTER_SIZE", "5"))
PYRO_PORT = int(os.environ.get("PYRO_PORT", "9091"))
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
PEER_HOST_PATTERN = os.environ.get("PEER_HOST_PATTERN", "raft-{i}")


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

    def since(self, last_id: int) -> list[tuple]:
        with self._lock:
            return [e for e in self._events if e[0] > last_id]


EVENTS = EventLog()


def _peer_id(peer) -> int:
    s = str(peer._pyroUri)
    return int(s.split("server.")[1].split("@")[0])


class Follower:
    def __init__(self):
        self.timeout_ms = randint(1000, 3000)
        self.last_heartbeat = time.monotonic()


class Candidate:
    def __init__(self, votes):
        self.timeout_ms = randint(1000, 3000)
        self.election_start = time.monotonic()
        self.votes = votes


class LogEntry:
    def __init__(self, value, term: int):
        self.value = value
        self.term = term


Pyro5.api.register_class_to_dict(
    LogEntry,
    lambda obj: {"__class__": "LogEntry", "value": obj.value, "term": obj.term},
)
Pyro5.api.register_dict_to_class(
    "LogEntry",
    lambda classname, d: LogEntry(d["value"], d["term"]),
)


class Leader:
    def __init__(self, peers, log_size):
        self.next_index: dict = {p: log_size for p in peers}
        self.heartbeat_frequency = 0.1
        self.last_sent_heartbeat = time.monotonic()


State = Follower | Candidate | Leader


class Server:
    def __init__(self, id):
        self.id = id
        self.state: State = Follower()
        self.term = 0
        self.voted = False
        self.logs: list[LogEntry] = []
        self.leader_id = id
        self.lock = threading.Lock()
        self.time_scale: float = 1.0
        self.peers: list[Pyro5.api.Proxy] = []
        self.crashed: bool = False
        self.partition_id: int = 0
        self.partition_map: dict[int, int] = {}

    def _peer_partition(self, peer_id: int) -> int:
        return self.partition_map.get(peer_id, 0)

    def _can_reach(self, peer_id: int) -> bool:
        return self._peer_partition(peer_id) == self.partition_id

    @Pyro5.api.expose
    def append_entries(
        self,
        leader_id: int,
        term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: list,
    ):
        with self.lock:
            if self.crashed:
                raise RuntimeError(f"server {self.id} is down")
            if not self._can_reach(leader_id):
                raise RuntimeError(
                    f"server {self.id} unreachable from n{leader_id} (partitioned)"
                )
            if self.term > term:
                EVENTS.append(
                    self.id,
                    f"reject ae from n{leader_id} t{term} (mine={self.term})",
                )
                return (self.term, False, len(self.logs))
            was_leader_or_candidate = not isinstance(self.state, Follower)
            if was_leader_or_candidate:
                EVENTS.append(self.id, f"step down on ae from n{leader_id} t{term}")
                self.state = Follower()
            self.state.last_heartbeat = time.monotonic()
            if self.term < term:
                self.term = term
                self.voted = False
            self.leader_id = leader_id
            if prev_log_index >= len(self.logs):
                if entries:
                    EVENTS.append(
                        self.id,
                        f"reject ae from n{leader_id}: gap (have {len(self.logs)}, need prev={prev_log_index})",
                    )
                return (self.term, False, len(self.logs))
            if prev_log_index >= 0 and self.logs[prev_log_index].term != prev_log_term:
                EVENTS.append(
                    self.id,
                    f"reject ae from n{leader_id}: term mismatch at idx {prev_log_index}",
                )
                return (self.term, False, len(self.logs))
            self.logs = self.logs[: prev_log_index + 1] + entries
            if entries:
                EVENTS.append(
                    self.id,
                    f"<- ae from n{leader_id} t{term}: applied {len(entries)} entries -> log_size={len(self.logs)}",
                )
            return (self.term, True, len(self.logs))

    @Pyro5.api.expose
    def request_vote(self, term, candidate_id, log_size, last_log_term):
        with self.lock:
            if self.crashed:
                raise RuntimeError(f"server {self.id} is down")
            if not self._can_reach(candidate_id):
                raise RuntimeError(
                    f"server {self.id} unreachable from n{candidate_id} (partitioned)"
                )
            if self.term > term:
                EVENTS.append(
                    self.id, f"deny vote n{candidate_id} t{term} (mine={self.term})"
                )
                return (self.term, False)
            if self.term < term and not isinstance(self.state, Follower):
                EVENTS.append(
                    self.id, f"step down on vote request n{candidate_id} t{term}"
                )
                self.state = Follower()
            if self.term < term:
                self.term = term
                self.voted = False
            if not isinstance(self.state, Follower):
                EVENTS.append(
                    self.id, f"deny vote n{candidate_id} t{term} (not follower)"
                )
                return (self.term, False)

            if self.voted:
                EVENTS.append(
                    self.id, f"deny vote n{candidate_id} t{term} (already voted)"
                )
                return (self.term, False)

            my_last_log_term = self.logs[-1].term if len(self.logs) > 0 else -1
            my_log_size = len(self.logs)
            log_ok = (last_log_term > my_last_log_term) or (
                last_log_term == my_last_log_term and log_size >= my_log_size
            )
            if log_ok:
                self.state.last_heartbeat = time.monotonic()
                self.voted = True
                EVENTS.append(self.id, f"GRANT vote n{candidate_id} t{term}")
                return (self.term, True)
            EVENTS.append(
                self.id, f"deny vote n{candidate_id} t{term} (log not up-to-date)"
            )
            return (self.term, False)

    @Pyro5.api.expose
    def add_log(self, x):
        with self.lock:
            if self.crashed:
                raise RuntimeError(f"server {self.id} is down")
            if not isinstance(self.state, Leader):
                EVENTS.append(self.id, f"add_log {x!r} REJECTED (not leader)")
                return False
            self.logs.append(LogEntry(x, self.term))
            EVENTS.append(
                self.id, f"add_log {x!r} @t{self.term} -> log_size={len(self.logs)}"
            )
            return True

    @Pyro5.api.expose
    def get_state(self):
        with self.lock:
            now = time.monotonic()
            scale = self.time_scale
            if isinstance(self.state, Follower):
                role = "follower"
                extra = {
                    "timeout_ms": self.state.timeout_ms,
                    "ms_since_heartbeat": int(
                        (now - self.state.last_heartbeat) * 1000 / scale
                    ),
                }
            elif isinstance(self.state, Candidate):
                role = "candidate"
                extra = {
                    "timeout_ms": self.state.timeout_ms,
                    "votes": self.state.votes,
                    "ms_since_election": int(
                        (now - self.state.election_start) * 1000 / scale
                    ),
                }
            else:
                role = "leader"
                extra = {
                    "next_index": {
                        _peer_id(p): idx for p, idx in self.state.next_index.items()
                    }
                }
            return {
                "id": self.id,
                "role": role,
                "term": self.term,
                "voted": self.voted,
                "leader_id": self.leader_id,
                "log_size": len(self.logs),
                "logs": [{"value": e.value, "term": e.term} for e in self.logs],
                "time_scale": self.time_scale,
                "crashed": self.crashed,
                "partition_id": self.partition_id,
                "extra": extra,
            }

    @Pyro5.api.expose
    def get_events(self, since_id: int):
        return [
            (ev_id, ts.isoformat(), nid, msg)
            for ev_id, ts, nid, msg in EVENTS.since(since_id)
        ]

    @Pyro5.api.expose
    def set_time_scale(self, scale: float):
        with self.lock:
            self.time_scale = max(0.1, float(scale))
            return self.time_scale

    @Pyro5.api.expose
    def set_partition(self, partition_id: int):
        with self.lock:
            old = self.partition_id
            self.partition_id = int(partition_id)
        if old != self.partition_id:
            EVENTS.append(
                self.id, f"moved to partition P{self.partition_id} (was P{old})"
            )

    @Pyro5.api.expose
    def set_partition_map(self, partition_map: dict):
        with self.lock:
            self.partition_map = {int(k): int(v) for k, v in partition_map.items()}
            self.partition_id = self.partition_map.get(self.id, self.partition_id)

    @Pyro5.api.expose
    def kill(self):
        with self.lock:
            if not self.crashed:
                self.crashed = True
                EVENTS.append(self.id, "*** CRASHED (sim) ***")

    @Pyro5.api.expose
    def restore(self):
        with self.lock:
            if self.crashed:
                self.crashed = False
                self.state = Follower()
                EVENTS.append(self.id, "*** RESTORED (sim) ***")

    def request_votes(self):
        def make_callback(peer):
            pid = _peer_id(peer)

            def callback(response):
                with self.lock:
                    term, granted = response
                    if term < self.term:
                        return
                    if term > self.term:
                        EVENTS.append(
                            self.id,
                            f"step down: n{pid} replied with higher term {term}",
                        )
                        self.state = Follower()
                        self.term = term
                        self.voted = False
                        return
                    if granted and isinstance(self.state, Candidate):
                        self.state.votes += 1
                        threshold = len(self.peers) // 2 + 1
                        EVENTS.append(
                            self.id,
                            f"vote received from n{pid}: {self.state.votes}/{threshold}",
                        )
                    elif not granted:
                        EVENTS.append(self.id, f"vote denied by n{pid}")

            return callback

        reachable = [p for p in self.peers if self._can_reach(_peer_id(p))]
        if not reachable:
            return
        peer_ids = ",".join(f"n{_peer_id(p)}" for p in reachable)
        EVENTS.append(self.id, f"-> request_vote to {peer_ids} t{self.term}")
        broadcast(
            "request_vote",
            {
                p: [
                    self.term,
                    self.id,
                    len(self.logs),
                    self.logs[-1].term if len(self.logs) > 0 else -1,
                ]
                for p in reachable
            },
            on_result={p: make_callback(p) for p in reachable},
        )

    def append_entries_callback_factory(self, peer):
        pid = _peer_id(peer)

        def callback(response):
            with self.lock:
                term, granted, peer_logs_len = response
                if term < self.term:
                    return
                if term > self.term:
                    EVENTS.append(
                        self.id,
                        f"step down: n{pid} replied with higher term {term}",
                    )
                    self.state = Follower()
                    self.term = term
                    self.voted = False
                    return

                if not isinstance(self.state, Leader):
                    return

                prev = self.state.next_index[peer]
                if granted:
                    if peer_logs_len != prev:
                        EVENTS.append(
                            self.id,
                            f"ack from n{pid}: next_index {prev} -> {peer_logs_len}",
                        )
                    self.state.next_index[peer] = peer_logs_len
                else:
                    self.state.next_index[peer] -= 1
                    EVENTS.append(
                        self.id,
                        f"nack from n{pid}: backing off next_index -> {self.state.next_index[peer]}",
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
                        EVENTS.append(
                            self.id, f"election timeout -> CANDIDATE t{self.term}"
                        )
                        self.request_votes()
                case Candidate(votes=v, election_start=es, timeout_ms=t):
                    if v > len(self.peers) / 2:
                        EVENTS.append(self.id, f"WON election -> LEADER t{self.term}")
                        self.state = Leader(self.peers, len(self.logs))
                        try:
                            ns = Pyro5.api.locate_ns()
                            ns.register(
                                "leader",
                                f"PYRO:server.{self.id}@{PEER_HOST_PATTERN.format(i=self.id)}:{PYRO_PORT}",
                            )
                        except Exception:
                            pass
                    elif time.monotonic() - es > (t * scale) / 1000.0:
                        EVENTS.append(self.id, "candidate timeout -> FOLLOWER")
                        self.state = Follower()
                case Leader(last_sent_heartbeat=lsh, heartbeat_frequency=hf):
                    if time.monotonic() - lsh < hf * scale:
                        return
                    args_map = {}
                    on_result = {}
                    for p in self.peers:
                        pid = _peer_id(p)
                        if not self._can_reach(pid):
                            continue
                        ni = self.state.next_index[p]
                        entries = self.logs[ni:]
                        prev_log_index = ni - 1
                        prev_log_term = self.logs[ni - 1].term if ni >= 1 else 0
                        if entries:
                            EVENTS.append(
                                self.id,
                                f"-> ae to n{pid} prev=({prev_log_index},t{prev_log_term}) entries={len(entries)}",
                            )
                        args_map[p] = [
                            self.id,
                            self.term,
                            prev_log_index,
                            prev_log_term,
                            entries,
                        ]
                        on_result[p] = self.append_entries_callback_factory(p)
                    if args_map:
                        broadcast("append_entries", args_map, on_result=on_result)
                        self.state.last_sent_heartbeat = time.monotonic()


def broadcast(method_name, args_map: dict, on_result: dict | None = None):
    def call(peer, args):
        peer._pyroClaimOwnership()
        try:
            result = getattr(peer, method_name)(*args)
        except Exception:
            return
        if on_result is not None:
            on_result[peer](result)

    for peer, args in args_map.items():
        threading.Thread(target=call, args=(peer, args), daemon=True).start()


def _own_uri() -> str:
    return f"PYRO:server.{NODE_ID}@{PEER_HOST_PATTERN.format(i=NODE_ID)}:{PYRO_PORT}"


def _register_in_ns(uri: str, retries: int = 60, delay: float = 1.0) -> None:
    last_err = None
    for _ in range(retries):
        try:
            ns = Pyro5.api.locate_ns()
            ns.register(f"server.{NODE_ID}", uri)
            return
        except Exception as e:
            last_err = e
            time.sleep(delay)
    print(f"node {NODE_ID}: failed to register in NS: {last_err}", flush=True)


def main():
    daemon = Pyro5.api.Daemon(host=BIND_HOST, port=PYRO_PORT)
    server = Server(NODE_ID)
    daemon.register(server, objectId=f"server.{NODE_ID}")

    threading.Thread(target=_register_in_ns, args=(_own_uri(),), daemon=True).start()

    for i in range(CLUSTER_SIZE):
        if i == NODE_ID:
            continue
        peer_uri = f"PYRO:server.{i}@{PEER_HOST_PATTERN.format(i=i)}:{PYRO_PORT}"
        server.peers.append(Pyro5.api.Proxy(peer_uri))

    def tick():
        while True:
            server.loop()
            time.sleep(0.01 * server.time_scale)

    threading.Thread(target=tick, daemon=True).start()

    print(f"node {NODE_ID} listening on {BIND_HOST}:{PYRO_PORT}", flush=True)
    daemon.requestLoop()


if __name__ == "__main__":
    main()
