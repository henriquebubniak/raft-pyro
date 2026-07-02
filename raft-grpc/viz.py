import os
import queue
import threading
import time

import grpc
import client_pb2
import client_pb2_grpc
import server_pb2
import server_pb2_grpc
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Input, RichLog, Static

CLUSTER_SIZE = int(os.environ.get("CLUSTER_SIZE", "4"))
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
PEER_HOST_PATTERN = os.environ.get("PEER_HOST_PATTERN", "raft-{i}")
RPC_TIMEOUT = 0.5
SPEED_STEP = 1.5
MIN_SCALE = 0.1
MAX_SCALE = 200.0
POLL_INTERVAL_S = 0.1
PARTITION_KEYS = ["z", "x", "c", "v", "b"]

NODE_COLORS = ["cyan", "magenta", "yellow", "green", "blue"]
PARTITION_COLORS = ["white", "red", "green3", "yellow", "blue"]


def role_color(role: str) -> str:
    return {
        "leader": "green",
        "candidate": "yellow",
        "follower": "cyan",
    }.get(role, "white")


def _snap_to_dict(s) -> dict:
    """Convert a StateResponse protobuf into the dict shape the panels expect."""
    d = {
        "id": s.id,
        "role": s.role,
        "term": s.term,
        "voted": s.voted,
        "leader_id": s.leader_id,
        "log_size": s.log_size,
        "time_scale": s.time_scale,
        "crashed": s.crashed,
        "partition_id": s.partition_id,
        "logs": [
            {"value": l.value, "term": l.term, "commited": l.commited} for l in s.logs
        ],
    }
    if s.role == "follower":
        d["extra"] = {
            "timeout_ms": s.timeout_ms,
            "ms_since_heartbeat": s.ms_since_heartbeat,
        }
    elif s.role == "candidate":
        d["extra"] = {
            "timeout_ms": s.timeout_ms,
            "votes": s.votes,
            "ms_since_election": s.ms_since_election,
        }
    else:
        d["extra"] = {"next_index": dict(s.next_index)}
    return d


class Controller:
    """Owns gRPC stubs and runs all RPCs in a single background thread."""

    def __init__(self, n_nodes: int):
        self.n = n_nodes
        self.stubs: list[server_pb2_grpc.ServerStub | None] = [None] * n_nodes
        self.api_stubs: list[client_pb2_grpc.ClientAPIStub | None] = [None] * n_nodes
        self.snapshots: list[dict | None] = [None] * n_nodes
        self.last_event_ids = [0] * n_nodes
        self.event_buffer: list[tuple] = []
        self.command_queue: queue.Queue = queue.Queue()
        self.lock = threading.Lock()
        self.partition_map: dict[int, int] = {i: 0 for i in range(n_nodes)}
        self._running = True
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False

    def submit(self, fn) -> None:
        self.command_queue.put(fn)

    def get_snapshot(self, i: int) -> dict | None:
        with self.lock:
            return self.snapshots[i]

    def drain_events(self) -> list[tuple]:
        with self.lock:
            evs = self.event_buffer
            self.event_buffer = []
        return evs

    def _connect(self) -> None:
        for i in range(self.n):
            host = PEER_HOST_PATTERN.format(i=i)
            channel = grpc.insecure_channel(f"{host}:{GRPC_PORT}")
            self.stubs[i] = server_pb2_grpc.ServerStub(channel)
            self.api_stubs[i] = client_pb2_grpc.ClientAPIStub(channel)
            self._buffer_event("viz", f"connected n{i} -> {host}:{GRPC_PORT}")

    def _buffer_event(self, src, msg: str) -> None:
        with self.lock:
            self.event_buffer.append(("", src, msg))

    def _loop(self) -> None:
        self._connect()
        while self._running:
            self._drain_commands()
            for i in range(self.n):
                stub = self.stubs[i]
                if stub is None:
                    continue
                try:
                    snap = _snap_to_dict(
                        stub.GetState(server_pb2.Empty(), timeout=RPC_TIMEOUT)
                    )
                    new_events = list(
                        stub.GetEvents(
                            server_pb2.GetEventsRequest(since_id=self.last_event_ids[i]),
                            timeout=RPC_TIMEOUT,
                        )
                    )
                except grpc.RpcError:
                    snap = None
                    new_events = []
                with self.lock:
                    self.snapshots[i] = snap
                    for ev in new_events:
                        self.event_buffer.append((ev.ts, ev.node_id, ev.msg))
                        if ev.id > self.last_event_ids[i]:
                            self.last_event_ids[i] = ev.id
            time.sleep(POLL_INTERVAL_S)

    def _drain_commands(self) -> None:
        while True:
            try:
                fn = self.command_queue.get_nowait()
            except queue.Empty:
                return
            try:
                fn(self.stubs)
            except Exception:
                pass


class NodePanel(Static):
    DEFAULT_CSS = """
    NodePanel {
        border: round $primary;
        padding: 1;
        margin: 0 1;
        width: 1fr;
        height: 100%;
    }
    NodePanel.crashed {
        border: round red;
    }
    NodePanel.offline {
        border: round $surface;
    }
    """

    def __init__(self, node_id: int, **kwargs):
        super().__init__(**kwargs)
        self.node_id = node_id

    def update_view(self, snap: dict | None) -> None:
        if snap is None:
            self.add_class("offline")
            self.remove_class("crashed")
            self.update(
                f"[bold dim]Node {self.node_id}  OFFLINE[/]\n\n"
                f"[dim](no response from container)[/]"
            )
            return

        self.remove_class("offline")
        role = snap["role"]
        crashed = snap["crashed"]
        partition = snap.get("partition_id", 0)
        part_color = PARTITION_COLORS[partition % len(PARTITION_COLORS)]

        if crashed:
            self.add_class("crashed")
            header = (
                f"[bold red]Node {self.node_id}  DOWN[/]  "
                f"[{part_color}]P{partition}[/]"
            )
        else:
            self.remove_class("crashed")
            color = role_color(role)
            header = (
                f"[bold {color}]Node {self.node_id}  {role.upper()}[/]  "
                f"[{part_color}]P{partition}[/]"
            )

        lines = [header, ""]
        lines.append(f"term:      {snap['term']}")
        lines.append(f"voted:     {snap['voted']}")
        lines.append(f"leader_id: {snap['leader_id']}")
        lines.append(f"log_size:  {snap['log_size']}")

        extra = snap["extra"]
        if role == "follower":
            lines.append(
                f"hb_age:    {extra['ms_since_heartbeat']}ms / {extra['timeout_ms']}ms"
            )
        elif role == "candidate":
            lines.append(f"votes:     {extra['votes']}")
            lines.append(
                f"election:  {extra['ms_since_election']}ms / {extra['timeout_ms']}ms"
            )
        else:
            lines.append("next_index:")
            for pid, idx in extra.get("next_index", {}).items():
                lines.append(f"  n{pid}: {idx}")

        lines.append("")
        lines.append("[dim]log:[/]")
        if snap["logs"]:
            tail = snap["logs"][-8:]
            offset = snap["log_size"] - len(tail)
            for i, e in enumerate(tail):
                entry = f"[{offset + i}] {e['value']!r} @t{e['term']}"
                if e.get("commited"):
                    lines.append(f"  [green]{entry}[/]")
                else:
                    lines.append(f"  {entry}")
            if snap["log_size"] > 8:
                lines.append(f"  [dim]...({snap['log_size'] - 8} more above)[/]")
        else:
            lines.append("  [dim](empty)[/]")

        self.update("\n".join(lines))


class RaftViz(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #cluster {
        height: 24;
    }
    #events {
        height: 1fr;
        border: round $secondary;
        padding: 0 1;
    }
    #status {
        height: 3;
        padding: 0 2;
        background: $boost;
    }
    Input {
        dock: bottom;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("1", "toggle_node('0')", "Crash 0"),
        Binding("2", "toggle_node('1')", "Crash 1"),
        Binding("3", "toggle_node('2')", "Crash 2"),
        Binding("4", "toggle_node('3')", "Crash 3"),
        Binding("5", "toggle_node('4')", "Crash 4"),
        Binding("z", "cycle_partition('0')", "Part 0"),
        Binding("x", "cycle_partition('1')", "Part 1"),
        Binding("c", "cycle_partition('2')", "Part 2"),
        Binding("v", "cycle_partition('3')", "Part 3"),
        Binding("b", "cycle_partition('4')", "Part 4"),
        Binding("plus,equals_sign", "speed_up", "Faster"),
        Binding("minus", "speed_down", "Slower"),
        Binding("m", "focus_input", "Send msg"),
        Binding("escape", "blur_input", "Defocus", show=False),
    ]

    def __init__(self):
        super().__init__()
        self.controller = Controller(CLUSTER_SIZE)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="cluster"):
            for i in range(CLUSTER_SIZE):
                yield NodePanel(i, id=f"node-{i}")
        yield RichLog(id="events", highlight=False, markup=True, wrap=False)
        yield Static("", id="status")
        msg = Input(
            placeholder="Type message and press Enter to send to leader (Esc to cancel)…",
            id="msg",
        )
        msg.disabled = True
        yield msg
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Raft Visualization"
        self.controller.start()
        self.set_interval(POLL_INTERVAL_S, self._tick)

    def on_unmount(self) -> None:
        self.controller.stop()

    def _status_text(self) -> str:
        snaps = [self.controller.get_snapshot(i) for i in range(CLUSTER_SIZE)]
        scale = next((s["time_scale"] for s in snaps if s is not None), 1.0)
        leader_id = next(
            (
                s["id"]
                for s in snaps
                if s is not None and s["role"] == "leader" and not s["crashed"]
            ),
            None,
        )
        leader_str = f"node {leader_id}" if leader_id is not None else "[red]none[/]"
        partitions = " ".join(
            f"n{i}=P{(s['partition_id'] if s else self.controller.partition_map[i])}"
            for i, s in enumerate(snaps)
        )
        return (
            f"[b]time_scale:[/] {scale:.2f}x   "
            f"[b]leader:[/] {leader_str}   "
            f"[b]partitions:[/] {partitions}\n"
            f"[dim]1-5 crash · z x c v b partition · m send · +/- speed · q quit[/]"
        )

    def _tick(self) -> None:
        for i in range(CLUSTER_SIZE):
            snap = self.controller.get_snapshot(i)
            self.query_one(f"#node-{i}", NodePanel).update_view(snap)
        self.query_one("#status", Static).update(self._status_text())
        self._drain_events()

    def _drain_events(self) -> None:
        new = self.controller.drain_events()
        if not new:
            return
        log = self.query_one("#events", RichLog)
        for ts_iso, nid, msg in new:
            stamp = ts_iso.split("T")[-1][:12] if ts_iso else "        "
            color = (
                NODE_COLORS[nid % len(NODE_COLORS)] if isinstance(nid, int) else "white"
            )
            tag = f"n{nid}" if isinstance(nid, int) else str(nid)
            log.write(f"[dim]{stamp}[/]  [bold {color}]{tag:>3}[/]  {msg}")

    def action_toggle_node(self, idx: str) -> None:
        i = int(idx)
        if i >= CLUSTER_SIZE:
            return
        snap = self.controller.get_snapshot(i)
        currently_crashed = bool(snap and snap["crashed"])

        def cmd(stubs):
            stub = stubs[i]
            if stub is None:
                return
            if currently_crashed:
                stub.Restore(server_pb2.KillRequest(), timeout=RPC_TIMEOUT)
            else:
                stub.Kill(server_pb2.KillRequest(), timeout=RPC_TIMEOUT)

        self.controller.submit(cmd)

    def action_cycle_partition(self, idx: str) -> None:
        i = int(idx)
        if i >= CLUSTER_SIZE:
            return
        self.controller.partition_map[i] = (
            self.controller.partition_map[i] + 1
        ) % CLUSTER_SIZE
        pmap = dict(self.controller.partition_map)

        def cmd(stubs):
            for stub in stubs:
                if stub is None:
                    continue
                try:
                    stub.SetPartitionMap(
                        server_pb2.SetPartitionAllRequest(partition_map=pmap),
                        timeout=RPC_TIMEOUT,
                    )
                except grpc.RpcError:
                    pass

        self.controller.submit(cmd)

    def action_speed_up(self) -> None:
        self._scale_speed(1.0 / SPEED_STEP)

    def action_speed_down(self) -> None:
        self._scale_speed(SPEED_STEP)

    def _scale_speed(self, factor: float) -> None:
        snap = next(
            (
                self.controller.get_snapshot(i)
                for i in range(CLUSTER_SIZE)
                if self.controller.get_snapshot(i) is not None
            ),
            None,
        )
        current = snap["time_scale"] if snap else 1.0
        new_scale = max(MIN_SCALE, min(MAX_SCALE, current * factor))

        def cmd(stubs):
            for stub in stubs:
                if stub is None:
                    continue
                try:
                    stub.SetTimeScale(
                        server_pb2.SetTimeScaleRequest(scale=new_scale),
                        timeout=RPC_TIMEOUT,
                    )
                except grpc.RpcError:
                    pass

        self.controller.submit(cmd)

    def action_focus_input(self) -> None:
        msg = self.query_one("#msg", Input)
        msg.disabled = False
        msg.focus()

    def action_blur_input(self) -> None:
        msg = self.query_one("#msg", Input)
        msg.value = ""
        msg.disabled = True
        self.set_focus(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        value = event.value.strip()
        event.input.value = ""
        event.input.disabled = True
        self.set_focus(None)
        if not value:
            return
        leader_id = next(
            (
                self.controller.get_snapshot(i)["id"]
                for i in range(CLUSTER_SIZE)
                if (s := self.controller.get_snapshot(i)) is not None
                and s["role"] == "leader"
                and not s["crashed"]
            ),
            None,
        )
        if leader_id is None:
            self.bell()
            return

        def cmd(stubs):
            stub = self.controller.api_stubs[leader_id]
            if stub is None:
                return

            # Publish blocks until the entry commits (or times out), so run
            # it in its own thread to keep the polling loop responsive
            def send():
                try:
                    resp = stub.Publish(
                        client_pb2.PublishRequest(value=value), timeout=10.0
                    )
                    verdict = "committed" if resp.success else "rejected"
                    self.controller._buffer_event("viz", f"write {value!r}: {verdict}")
                except grpc.RpcError:
                    self.controller._buffer_event("viz", f"write {value!r}: error")

            threading.Thread(target=send, daemon=True).start()

        self.controller.submit(cmd)


def main():
    app = RaftViz()
    app.run()


if __name__ == "__main__":
    main()
