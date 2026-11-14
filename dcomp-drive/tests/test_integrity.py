import hashlib
import socket
import threading
import uuid

import pytest

from src.client.crypto import CryptoManager
from src.client.pipeline import DFSClient
from src.storage import node as node_module
from src.storage.integrity_node import IntegrityNode


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def node(tmp_path, monkeypatch):
    monkeypatch.setattr(node_module.wrapperKazoo, "init", lambda: None)
    monkeypatch.setattr(node_module.wrapperKazoo, "register_storage", lambda *a, **k: None)

    n = IntegrityNode("t1", "127.0.0.1", free_port(), str(tmp_path / "blocks"), scrub_interval=0)
    n.start()
    t = threading.Thread(target=n.serve_forever, daemon=True)
    t.start()
    yield n
    n.request_stop()
    t.join(timeout=3)
    n.stop()


@pytest.fixture
def client():
    return DFSClient("tcp://127.0.0.1:1", CryptoManager(None))


def test_roundtrip_writes_meta_next_to_block(node, client):
    bid, data = str(uuid.uuid4()), b"conteudo" * 100
    client._send_block_to_storage(node.addr, bid, sha(data), data)
    assert client._fetch_block_from_storage(node.addr, bid) == data
    assert (node.storage_path / f"{bid}.meta").exists()


def test_read_of_corrupted_block_is_refused_and_quarantined(node, client):
    bid, data = str(uuid.uuid4()), b"A" * 500
    client._send_block_to_storage(node.addr, bid, sha(data), data)
    (node.storage_path / bid).write_bytes(b"bit rot")  
    
    with pytest.raises(RuntimeError):
        client._fetch_block_from_storage(node.addr, bid)
    assert not (node.storage_path / bid).exists()
    assert any(p.name.startswith(bid) for p in node.corrupt_path.iterdir())


def test_scrub_finds_only_the_corrupted_block(node, client):
    good, bad = str(uuid.uuid4()), str(uuid.uuid4())
    for bid, data in ((good, b"bom"), (bad, b"ruim")):
        client._send_block_to_storage(node.addr, bid, sha(data), data)
    (node.storage_path / bad).write_bytes(b"adulterado")

    checked, corrupt = node.scrub_once()
    assert checked == 2 and corrupt == [bad]
    assert client._fetch_block_from_storage(node.addr, good) == b"bom"


def test_wrong_hash_leaves_no_meta_behind(node, client):
    bid = str(uuid.uuid4())
    with pytest.raises(RuntimeError):
        client._send_block_to_storage(node.addr, bid, sha(b"a"), b"b")
    assert not (node.storage_path / f"{bid}.meta").exists()


def test_cleanup_removes_interrupted_writes(node):
    leftover = node.storage_path / ".abc.xyz.tmp"
    leftover.write_bytes(b"pela metade")
    node._cleanup_tmp()
    assert not leftover.exists()
