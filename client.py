import time
import Pyro5.api
import Pyro5.errors


def main():
    while True:
        try:
            with Pyro5.api.Proxy("PYRONAME:raft.leader") as leader:
                leader._pyroTimeout = 1.0
                status = leader.get_status()
            print(
                f"leader=node{status['node_id']} "
                f"term={status['term']} state={status['state']}"
            )
        except Pyro5.errors.NamingError:
            print("no leader registered yet")
        except Pyro5.errors.CommunicationError:
            print("leader registered but unreachable (likely stale)")
        except Exception as e:
            print(f"error: {e}")
        time.sleep(2)


if __name__ == "__main__":
    main()
