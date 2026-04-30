import sys
import threading
import Pyro5.api
import Pyro5
from server import RaftNode


FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"

NODE_IDS = [1, 2, 3, 4]
HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
RPC_TIMEOUT = 0.5

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

    print(f"[node{node_id}] ready at {uri}")
    daemon.requestLoop()


if __name__ == "__main__":
    main()

