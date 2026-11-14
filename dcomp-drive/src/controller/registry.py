""" Descoberta e monitoramento dos nós de armazenamento """

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from typing import Callable

from src.wrapper import wrapperKazoo


@dataclass
class StorageNode:
    node_id: str
    disks: list
    last_seen: float
    state: str = "ALIVE"
    addr: str | None = None
    host: str | None = None
    draining: bool = False


class NodeRegistry:
    def __init__(
        self,
        cfg,
        db,
        on_change: Callable | None = None,
    ):
        self.cfg = cfg
        self.db = db
        self.on_change = on_change

        self._nodes: dict[str, StorageNode] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._watcher = None
        self._monitor_thread = None
        self._closed = False

    def start(self):
        """Inicia a descoberta e o monitoramento dos nós."""

        if self._closed:
            raise RuntimeError("Um registro encerrado não pode ser reiniciado.")


        if self._monitor_thread and self._monitor_thread.is_alive():
            return

        wrapperKazoo.init()
        self._stop_event.clear()

        if self._watcher is None:
            self._watcher = wrapperKazoo.set_storage_watch(
                remove_func=self._remove_node,
                add_func=self._add_node,
            )

        self._monitor_thread = threading.Thread(
            target=self._monitor,
            name="storage-node-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        logging.info("Monitoramento dos nós iniciado")

    def stop(self):
        """Encerra definitivamente o monitoramento."""
        if self._closed:
            return

        self._closed = True
        self._stop_event.set()

        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)
            self._monitor_thread = None

        logging.info("Monitoramento dos nós encerrado")

    def _add_node(self, node_id: str):
        if self._stop_event.is_set():
            return

        result = wrapperKazoo.get_node(f"/storage/nodes/{node_id}")
        if result is None:
            return

        data, _ = result

        try:
            disks = json.loads(data.decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
            logging.exception("Dados inválidos para o nó %s", node_id)
            return

        draining = self.is_draining(node_id)

        with self._lock:
            previous = self._nodes.get(node_id)

            node = StorageNode(
                node_id=node_id,
                disks=disks,
                last_seen=time.monotonic(),
                state="ALIVE",
                addr=previous.addr if previous else None,
                host=previous.host if previous else None,
                draining=draining,
            )

            self._nodes[node_id] = node

        self._notify(node_id, "ALIVE")
        logging.info("Nó %s descoberto", node_id)

    def _remove_node(self, node_id: str):
        if self._stop_event.is_set():
            return

        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return

            node.state = "DEAD"

        self._notify(node_id, "DEAD")
        logging.warning("Nó %s removido do ZooKeeper", node_id)

    def heartbeat(
        self,
        node_id: str,
        addr: str | None = None,
        host: str | None = None,
        disks: list | None = None,
    ):
        """Atualiza a última comunicação recebida de um nó."""
        with self._lock:
            node = self._nodes.get(node_id)

            if node is None:
                return False

            previous_state = node.state
            node.last_seen = time.monotonic()
            node.state = "ALIVE"

            if addr is not None:
                node.addr = addr

            if host is not None:
                node.host = host

            if disks is not None:
                node.disks = disks

        if previous_state != "ALIVE":
            self._notify(node_id, "ALIVE")

        return True

    def _monitor(self):
        while not self._stop_event.wait(self.cfg.membership_interval_s):
            now = time.monotonic()

            with self._lock:
                node_ids = list(self._nodes)

            for node_id in node_ids:
                with self._lock:
                    node = self._nodes.get(node_id)
                    if node is None:
                        continue

                    elapsed = now - node.last_seen

                    if elapsed >= self.cfg.dead_after_s:
                        new_state = "DEAD"
                    elif elapsed >= self.cfg.suspect_after_s:
                        new_state = "SUSPECT"
                    else:
                        new_state = "ALIVE"

                    if node.state == new_state:
                        continue

                    node.state = new_state

                self._notify(node_id, new_state)
                logging.warning(
                    "Estado do nó %s alterado para %s",
                    node_id,
                    new_state,
                )

    def get_node(self, node_id: str):
        with self._lock:
            node = self._nodes.get(node_id)
            return asdict(node) if node else None

    def get_nodes(self):
        with self._lock:
            return {
                node_id: asdict(node)
                for node_id, node in self._nodes.items()
            }

    def is_draining(self, node_id: str) -> bool:
        with self.db.tx() as cur:
            cur.execute(
                "SELECT draining FROM node_flags WHERE node_id = %s",
                (node_id,),
            )
            result = cur.fetchone()

        return bool(result["draining"]) if result else False

    def set_draining(self, node_id: str, draining: bool):
        with self.db.tx() as cur:
            cur.execute(
                """
                INSERT INTO node_flags (node_id, draining, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (node_id)
                DO UPDATE SET
                    draining = EXCLUDED.draining,
                    updated_at = NOW()
                """,
                (node_id, draining),
            )

        with self._lock:
            node = self._nodes.get(node_id)
            if node:
                node.draining = draining

    def _notify(self, node_id: str, state: str):
        if self.on_change:
            try:
                self.on_change(node_id, state)
            except Exception:
                logging.exception(
                    "Erro ao notificar mudança de estado do nó %s",
                    node_id,
                )
