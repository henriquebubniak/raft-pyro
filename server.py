import time
import random
import threading
import Pyro5.api
import sys

HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
RPC_TIMEOUT = 0.5

NODE_IDS = [1, 2, 3, 4]

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


@Pyro5.api.expose
class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.state = FOLLOWER
        self.current_term = 0
        self.peers = [p for p in NODE_IDS if p != node_id]
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_timeout()
        self.running = True
        self.uri = None
        self.lock = threading.Lock()

    def _random_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _peer_proxy(self, peer_id):
        proxy = Pyro5.api.Proxy(f"PYRONAME:raft.node{peer_id}")
        proxy._pyroTimeout = RPC_TIMEOUT
        return proxy

    def _log(self, msg):
        print(f"[node{self.node_id} term={self.current_term} {self.state}] {msg}", flush=True)

    def request_vote(self, term, candidate_id):
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = FOLLOWER
                self.leader_id = None

            vote_granted = False
            if term == self.current_term and self.voted_for in (None, candidate_id):
                self.voted_for = candidate_id
                vote_granted = True
                self.last_heartbeat = time.time()
                self._log(f"granted vote to node{candidate_id}")

            return self.current_term, vote_granted

    def append_entries(self, term, leader_id):
        with self.lock:
            if term < self.current_term:
                return self.current_term, False

            if term > self.current_term:
                self.current_term = term
                self.voted_for = None

            if self.state != FOLLOWER:
                self._log(f"stepping down, leader is node{leader_id}")
            self.state = FOLLOWER
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            return self.current_term, True

    def get_status(self):
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "voted_for": self.voted_for,
                "since_heartbeat": round(time.time() - self.last_heartbeat, 2),
                "election_timeout": round(self.election_timeout, 2),
            }

    def shutdown(self):
        self._log("shutdown requested")
        self.running = False

    def run(self):
        while self.running:
            time.sleep(0.05)

            with self.lock:
                state = self.state
                elapsed = time.time() - self.last_heartbeat
                timeout = self.election_timeout

            if state == LEADER:
                self._send_heartbeats()
                time.sleep(HEARTBEAT_INTERVAL)
                continue

            if elapsed >= timeout:
                self._start_election()

    def _start_election(self):
        with self.lock:
            self.state = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_timeout()
            term = self.current_term
            self._log("starting election")

        votes = 1
        majority = (len(NODE_IDS) // 2) + 1

        for peer_id in self.peers:
            try:
                with self._peer_proxy(peer_id) as proxy:
                    peer_term, granted = proxy.request_vote(term, self.node_id)
            except Exception:
                continue

            with self.lock:
                if peer_term > self.current_term:
                    self.current_term = peer_term
                    self.voted_for = None
                    self.state = FOLLOWER
                    self.leader_id = None
                    self._log(f"discovered higher term {peer_term} from node{peer_id}, stepping down")
                    return
                if self.state != CANDIDATE or self.current_term != term:
                    return

            if granted:
                votes += 1

        with self.lock:
            if self.state != CANDIDATE or self.current_term != term:
                return
            if votes < majority:
                self._log(f"election failed with {votes}/{len(NODE_IDS)} votes")
                return

            self.state = LEADER
            self.leader_id = self.node_id
            won_term = self.current_term
            print(
                f"[node{self.node_id}] became LEADER for term {won_term} "
                f"with {votes}/{len(NODE_IDS)} votes",
                flush=True,
            )

        self._register_as_leader()

    def _register_as_leader(self):
        try:
            ns = Pyro5.api.locate_ns()
            ns.register("raft.leader", self.uri, safe=False)
        except Exception as e:
            print(f"[node{self.node_id}] failed to register raft.leader: {e}", flush=True)

    def _send_heartbeats(self):
        with self.lock:
            term = self.current_term

        for peer_id in self.peers:
            try:
                with self._peer_proxy(peer_id) as proxy:
                    peer_term, _ = proxy.append_entries(term, self.node_id)
            except Exception:
                continue

            with self.lock:
                if peer_term > self.current_term:
                    self.current_term = peer_term
                    self.voted_for = None
                    self.state = FOLLOWER
                    self.leader_id = None
                    self._log("discovered higher term, stepping down")
                    return


def main():
    if len(sys.argv) != 2:
        print("Usage: python server.py <node_id>  (1..4)")
        sys.exit(1)
    node_id = int(sys.argv[1])
    if node_id not in NODE_IDS:
        print(f"node_id must be one of {NODE_IDS}")
        sys.exit(1)

    daemon = Pyro5.api.Daemon()
    node = RaftNode(node_id)
    uri = daemon.register(node)
    node.uri = uri

    ns = Pyro5.api.locate_ns()
    ns.register(f"raft.node{node_id}", uri)

    threading.Thread(target=node.run, daemon=True).start()

    print(f"[node{node_id}] ready at {uri}", flush=True)
    daemon.requestLoop(loopCondition=lambda: node.running)


if __name__ == "__main__":
    main()
