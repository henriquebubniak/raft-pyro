import argparse
import time
import Pyro5.api
import Pyro5.errors


def get_leader():
    ns = Pyro5.api.locate_ns()
    uri = ns.lookup("leader")
    return Pyro5.api.Proxy(uri)


def send(value, retries=5, backoff=0.5):
    for attempt in range(retries):
        try:
            leader = get_leader()
            leader.add_log(value)
            print(f"sent {value!r} to {leader._pyroUri}")
            return True
        except Pyro5.errors.NamingError:
            print(f"no leader registered (attempt {attempt + 1}/{retries})")
        except Pyro5.errors.CommunicationError as e:
            print(f"leader unreachable: {e} (attempt {attempt + 1}/{retries})")
        time.sleep(backoff)
    return False


def main():
    parser = argparse.ArgumentParser(description="Raft client")
    parser.add_argument(
        "values",
        nargs="*",
        help="values to append to the log; if omitted, runs in interactive mode",
    )
    args = parser.parse_args()

    if args.values:
        for v in args.values:
            if not send(v):
                print(f"failed to send {v!r}")
                return
        return

    print("interactive mode — type a value and press enter (ctrl-d to exit)")
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            print()
            return
        if not line:
            continue
        if not send(line):
            print(f"failed to send {line!r}")


if __name__ == "__main__":
    main()
