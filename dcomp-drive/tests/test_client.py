import os
import hashlib
import pytest
from unittest.mock import MagicMock
from src.client.crypto import CryptoManager
from src.client.pipeline import DFSClient

def test_crypto_roundtrip_and_hash():
    key_hex = os.urandom(32).hex()
    crypto = CryptoManager(key_hex=key_hex)
    dados = b"Dados do DFS"

    payload, sha256_gerado = crypto.process_outgoing_block(dados)

    assert len(payload) == len(dados) + 28
    assert payload != dados

    dados_recuperados = crypto.process_incoming_block(payload, sha256_gerado)
    assert dados_recuperados == dados

def test_crypto_corrupted_block_detection():
    key_hex = os.urandom(32).hex()
    crypto = CryptoManager(key_hex=key_hex)
    payload, sha256_gerado = crypto.process_outgoing_block(b"Bloco original")

    payload_corrompido = bytearray(payload)
    payload_corrompido[15] ^= 0xFF

    with pytest.raises(ValueError, match="Integridade do bloco falhou"):
        crypto.process_incoming_block(bytes(payload_corrompido), sha256_gerado)

def test_pipeline_put_and_get_with_failover(tmp_path):
    key_hex = os.urandom(32).hex()
    crypto = CryptoManager(key_hex=key_hex)
    
    client = DFSClient(controller_addr="tcp://localhost:5000", crypto=crypto, chunk_size=10)

    arquivo_origem = tmp_path / "origem.txt"
    conteudo_original = b"A" * 10 + b"B" * 10 + b"C" * 5
    arquivo_origem.write_bytes(conteudo_original)

    fake_storage = {}
    metadados_salvos = []

    def mock_rpc_controller(action, payload, identificador="N/A"):
        if action == "INIT_WRITE":
            return {"req_id": "1", "status": "ACK", "file_id": "file-999"}
            
        elif action == "ALLOCATE_BLOCK":
            metadados_salvos.append({
                "block_id": payload["block_id"],
                "block_index": payload["block_index"],
                "sha256": payload["sha256"],
                "replicas": [
                    {"node_id": "node1", "addr": "tcp://node1:5555"},
                    {"node_id": "node2", "addr": "tcp://node2:5556"}
                ]
            })
            return {
                "req_id": "2", "status": "ACK", 
                "nodes": [{"node_id": "node1", "addr": "tcp://node1:5555"}, {"node_id": "node2", "addr": "tcp://node2:5556"}]
            }
            
        elif action in ("COMMIT_BLOCK", "FINISH_WRITE", "REPORT_CORRUPTION"):
            return {"req_id": "3", "status": "ACK"}
            
        elif action == "GET_FILE_METADATA":
            return {
                "req_id": "4", "status": "ACK",
                "file_sha256": hashlib.sha256(conteudo_original).hexdigest(),
                "blocks": metadados_salvos
            }

    def mock_send_to_storage(node_addr, block_id, sha256, data):
        fake_storage[(node_addr, block_id)] = data

    def mock_fetch_from_storage(node_addr, block_id):
        if node_addr == "tcp://node1:5555" and block_id == metadados_salvos[0]["block_id"]:
            return b"DADOS_TOTALMENTE_CORROMPIDOS"
        return fake_storage[(node_addr, block_id)]

    client._rpc_controller = MagicMock(side_effect=mock_rpc_controller)
    client._send_block_to_storage = MagicMock(side_effect=mock_send_to_storage)
    client._fetch_block_from_storage = MagicMock(side_effect=mock_fetch_from_storage)

    
    res_put = client.put_file(str(arquivo_origem), "/teste.txt")
    assert res_put["blocks_count"] == 3

    arquivo_destino = tmp_path / "destino.txt"
    hash_final = client.get_file("/teste.txt", str(arquivo_destino))

    assert arquivo_destino.read_bytes() == conteudo_original
    assert hash_final == hashlib.sha256(conteudo_original).hexdigest()

    chamadas_report = [
        call for call in client._rpc_controller.call_args_list 
        if call.args[0] == "REPORT_CORRUPTION"
    ]
    assert len(chamadas_report) == 1
    assert chamadas_report[0].args[1]["node_id"] == "node1"
