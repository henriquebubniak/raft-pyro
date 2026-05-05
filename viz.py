import os
import queue
import threading
import time

import Pyro5.api
import Pyro5.errors
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Input, RichLog, Static

CLUSTER_SIZE = int(os.environ.get("CLUSTER_SIZE", "5"))
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


class Controller:
    """Owns Pyro5 proxies and runs all RPCs in a single background thread."""

    def __init__(self, n_nodes: int):
        self.n = n_nodes
        self.proxies: list[Pyro5.api.Proxy | None] = [None] * n_nodes
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

    def _resolve_proxies(self) -> None:
        ns_uri = None
        for _ in range(120):
            try:
                ns = Pyro5.api.locate_ns()
                ns_uri = ns
                break
            except Exception:
                time.sleep(0.5)
        if ns_uri is None:
            self._buffer_event("viz", "could not locate Pyro5 nameserver")
            return
        for i in range(self.n):
            for _ in range(120):
                try:
                    uri = ns_uri.lookup(f"server.{i}")
                    self.proxies[i] = Pyro5.api.Proxy(uri)
                    self._buffer_event("viz", f"resolved n{i} -> {uri}")
                    break
                except Pyro5.errors.NamingError:
                    time.sleep(0.5)
                except Exception:
                    time.sleep(0.5)

    def _buffer_event(self, src, msg: str) -> None:
        with self.lock:
            self.event_buffer.append(("", src, msg))

    def _loop(self) -> None:
        self._resolve_proxies()
        while self._running:
            self._drain_commands()
            for i in range(self.n):
                p = self.proxies[i]
                if p is None:
                    continue
                try:
                    p._pyroClaimOwnership()
                    snap = p.get_state()
                    new_events = p.get_events(self.last_event_ids[i])
                except Exception:
                    snap = None
                    new_events = []
                with self.lock:
                    self.snapshots[i] = snap
                    for ev in new_events:
                        ev_id, ts_iso, nid, msg = ev
                        self.event_buffer.append((ts_iso, nid, msg))
                        if ev_id > self.last_event_ids[i]:
                            self.last_event_ids[i] = ev_id
            time.sleep(POLL_INTERVAL_S)

    def _drain_commands(self) -> None:
        while True:
            try:
                fn = self.command_queue.get_nowait()
            except queue.Empty:
                return
            try:
                fn(self.proxies)
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
        scale = next(
            (s["time_scale"] for s in snaps if s is not None), 1.0
        )
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
        snap = self.controller.get_snapshot(i)
        currently_crashed = bool(snap and snap["crashed"])

        def cmd(proxies):
            p = proxies[i]
            if p is None:
                return
            p._pyroClaimOwnership()
            if currently_crashed:
                p.restore()
            else:
                p.kill()

        self.controller.submit(cmd)

    def action_cycle_partition(self, idx: str) -> None:
        i = int(idx)
        self.controller.partition_map[i] = (
            self.controller.partition_map[i] + 1
        ) % CLUSTER_SIZE
        pmap = dict(self.controller.partition_map)

        def cmd(proxies):
            for p in proxies:
                if p is None:
                    continue
                try:
                    p._pyroClaimOwnership()
                    p.set_partition_map(pmap)
                except Exception:
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

        def cmd(proxies):
            for p in proxies:
                if p is None:
                    continue
                try:
                    p._pyroClaimOwnership()
                    p.set_time_scale(new_scale)
                except Exception:
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

        def cmd(proxies):
            p = proxies[leader_id]
            if p is None:
                return
            try:
                p._pyroClaimOwnership()
                p.add_log(value)
            except Exception:
                pass

        self.controller.submit(cmd)


def main():
    app = RaftViz()
    app.run()


if __name__ == "__main__":
    main()
