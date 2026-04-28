import typing
import Pyro5.api
import Pyro5
from datetime import datetime, timedelta
from random import randint

PYROPORT = 9090


class Follower:
    def __init__(self):
        self.timeout_ms = randint(100, 300)
        self.last_heartbeat = datetime.now()
        self.voted = []


class Candidate:
    def __init__(self, votes):
        self.timeout_ms = randint(100, 300)
        self.election_start = datetime.now()
        self.votes = votes

    def request_votes(self):
        pass


class Leader:
    pass


State = Follower | Candidate | Leader


class Server:
    def __init__(self, id):
        self.id = id
        self.state: State = Follower()
        self.term = 0

    def heartbeat(self):
        if isinstance(self.state, Follower):
            self.state.last_heartbeat = datetime.now()

    def request_votes(self):
        if isinstance(self.state, Candidate):
            for peer in peers:
                self.state.votes += peer.request_vote(self.term + 1)

    def request_vote(self, term) -> typing.Literal[1, 0]:
        if not isinstance(self.state, Follower):
            return 0

        self.state.last_heartbeat = datetime.now()

        if term in self.state.voted:
            self.state.voted.append(term)
            return 1

        return 0

    def loop(self):
        match type(self.state):
            case Follower(last_heartbeat=lh, timeout_ms=t):
                if datetime.now() - lh > timedelta(milliseconds=t):
                    self.state = Candidate(votes=1)
                    self.state.request_votes()
            case Candidate(votes=v, election_start=es, timeout_ms=t):
                if v > len(peers) / 2:
                    self.state = Leader()
                if datetime.now() - es > timedelta(milliseconds=t):
                    self.state = Follower()
            case Leader():
                pass


peers: list[Server] = []


def main():
    id = 1
    daemon = Pyro5.api.Daemon(port=PYROPORT)
    uri = daemon.register(Server(id), objectId=f"server.{id}")


if __name__ == "__main__":
    main()
