import uuid
import json
import hashlib
import logging
import zmq
from pathlib import Path
from src.client.crypto import CryptoManager

logger = logging.getLogger("ClienteDFS")

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB
ZMQ_TIMEOUT_MS = 5000


class DFSClient:
    def __init__(self, controller_addr: str, crypto: CryptoManager, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.controller_addr = controller_addr
        self.crypto = crypto
        self.chunk_size = chunk_size
        self.ctx = zmq.Context()

    def _create_dealer_socket(self, addr: str) -> zmq.Socket:
        socket = self.ctx.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode("utf-8"))
        socket.setsockopt(zmq.RCVTIMEO, ZMQ_TIMEOUT_MS)
        socket.setsockopt(zmq.SNDTIMEO, ZMQ_TIMEOUT_MS)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(addr)
        return socket

    def _rpc_controller(self, action: str, payload: dict, identificador: str = "N/A") -> dict:
        sock = self._create_dealer_socket(self.controller_addr)
        try:
            req_id = str(uuid.uuid4())
            envelope = {"req_id": req_id, "action": action, **payload}
            
            logger.debug(f"RPC {action} -> Controlador", extra={"evento": action, "identificador": identificador})
            
            sock.send_multipart([b"", json.dumps(envelope).encode("utf-8")])
            _, raw_res = sock.recv_multipart()
            res = json.loads(raw_res.decode("utf-8"))

            if res.get("req_id") != req_id or res.get("status") != "ACK":
                msg_erro = res.get('msg', 'Erro Desconhecido')
                logger.error(f"Falha RPC {action}: {msg_erro}", extra={"evento": f"{action}_ERR", "identificador": identificador})
                raise RuntimeError(f"Erro do Controlador em {action}: {msg_erro}")
            
            return res
        finally:
            sock.close()

    def _send_block_to_storage(self, node_addr: str, block_id: str, sha256: str, data: bytes):
        sock = self._create_dealer_socket(node_addr)
        try:
            req_id = str(uuid.uuid4())
            meta = {"req_id": req_id, "action": "PUT_BLOCK", "block_id": block_id, "sha256": sha256}
            
            logger.debug(f"Enviando para {node_addr}", extra={"evento": "PUT_BLOCK", "identificador": block_id})
            
            sock.send_multipart([b"", json.dumps(meta).encode("utf-8"), data])
            _, raw_res = sock.recv_multipart()
            res = json.loads(raw_res.decode("utf-8"))

            if res.get("status") != "ACK":
                msg = res.get('msg')
                logger.error(f"Rejeitado por {node_addr}: {msg}", extra={"evento": "PUT_BLOCK_ERR", "identificador": block_id})
                raise RuntimeError(f"Nó {node_addr} rejeitou o bloco {block_id}: {msg}")
        finally:
            sock.close()

    def _fetch_block_from_storage(self, node_addr: str, block_id: str) -> bytes:
        sock = self._create_dealer_socket(node_addr)
        try:
            req_id = str(uuid.uuid4())
            meta = {"req_id": req_id, "action": "GET_BLOCK", "block_id": block_id}
            
            logger.debug(f"Baixando de {node_addr}", extra={"evento": "GET_BLOCK", "identificador": block_id})
            
            sock.send_multipart([b"", json.dumps(meta).encode("utf-8")])
            _, raw_res, data = sock.recv_multipart()
            res = json.loads(raw_res.decode("utf-8"))

            if res.get("status") != "ACK":
                raise RuntimeError(f"Erro ao ler bloco {block_id} de {node_addr}: {res.get('status')}")
            return data
        finally:
            sock.close()

    def put_file(self, local_path: str, remote_path: str) -> dict:
        path = Path(local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Arquivo não encontrado: {local_path}")
            
        file_size = path.stat().st_size
        logger.info(f"Iniciando upload de '{local_path}' -> '{remote_path}'", extra={"evento": "UPLOAD_START", "identificador": remote_path})

        full_file_hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(self.chunk_size):
                full_file_hasher.update(chunk)
        file_sha256 = full_file_hasher.hexdigest()

        init_res = self._rpc_controller("INIT_WRITE", {
            "remote_path": remote_path,
            "size_bytes": file_size,
            "encrypted": self.crypto.enabled,
            "file_sha256": file_sha256,
        }, identificador=remote_path)
        file_id = init_res["file_id"]

        blocks_sent = 0
        with open(path, "rb") as f:
            block_index = 0
            while chunk := f.read(self.chunk_size):
                payload, block_sha256 = self.crypto.process_outgoing_block(chunk)
                block_id = str(uuid.uuid4())

                alloc_res = self._rpc_controller("ALLOCATE_BLOCK", {
                    "file_id": file_id,
                    "block_id": block_id,
                    "block_index": block_index,
                    "size_bytes": len(payload),
                    "sha256": block_sha256,
                }, identificador=block_id)
                
                target_nodes = alloc_res["nodes"]
                logger.info(f"Alocado nos nós: {[n['node_id'] for n in target_nodes]}", extra={"evento": "BLOCK_ALLOCATED", "identificador": block_id})

                committed_node_ids = []
                for node in target_nodes:
                    self._send_block_to_storage(node["addr"], block_id, block_sha256, payload)
                    committed_node_ids.append(node["node_id"])

                self._rpc_controller("COMMIT_BLOCK", {
                    "file_id": file_id,
                    "block_id": block_id,
                    "node_ids": committed_node_ids,
                }, identificador=block_id)
                
                block_index += 1
                blocks_sent += 1

        self._rpc_controller("FINISH_WRITE", {
            "file_id": file_id,
            "file_sha256": file_sha256,
        }, identificador=file_id)
        
        logger.info(f"Upload finalizado ({blocks_sent} blocos).", extra={"evento": "UPLOAD_END", "identificador": file_id})
        return {"file_id": file_id, "file_sha256": file_sha256, "blocks_count": blocks_sent}

    def get_file(self, remote_path: str, dest_path: str) -> str:
        logger.info(f"Iniciando download de '{remote_path}'", extra={"evento": "DOWNLOAD_START", "identificador": remote_path})
        
        meta_res = self._rpc_controller("GET_FILE_METADATA", {"remote_path": remote_path}, identificador=remote_path)
        blocks = sorted(meta_res["blocks"], key=lambda b: b["block_index"])
        expected_file_hash = meta_res["file_sha256"]

        full_file_hasher = hashlib.sha256()

        with open(dest_path, "wb") as f:
            for blk in blocks:
                block_id = blk["block_id"]
                expected_block_hash = blk["sha256"]
                replicas = blk["replicas"]

                for replica in replicas:
                    node_id, node_addr = replica["node_id"], replica["addr"]
                    try:
                        payload = self._fetch_block_from_storage(node_addr, block_id)
                        raw_chunk = self.crypto.process_incoming_block(payload, expected_block_hash)

                        f.write(raw_chunk)
                        full_file_hasher.update(raw_chunk)
                        logger.debug(f"Bloco recebido e validado", extra={"evento": "BLOCK_DOWNLOADED", "identificador": block_id})
                        break
                    except ValueError as ve:
                        logger.warning(f"Corrupção em {node_id}: {ve}", extra={"evento": "CORRUPTION_DETECTED", "identificador": block_id})
                        try:
                            self._rpc_controller("REPORT_CORRUPTION", {"block_id": block_id, "node_id": node_id}, identificador=block_id)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.warning(f"Falha de rede com {node_id}: {e}", extra={"evento": "NODE_FAILED", "identificador": block_id})
                else:
                    Path(dest_path).unlink(missing_ok=True)
                    logger.error(f"Sem réplicas íntegras", extra={"evento": "DOWNLOAD_FAILED", "identificador": remote_path})
                    raise RuntimeError(f"Falha ao ler bloco {block_id}")

        calculated_file_hash = full_file_hasher.hexdigest()
        if calculated_file_hash != expected_file_hash:
            Path(dest_path).unlink(missing_ok=True)
            logger.error("Hash final do arquivo inválido", extra={"evento": "FILE_CORRUPTED", "identificador": remote_path})
            raise ValueError("Integridade do arquivo completo falhou!")

        logger.info("Download finalizado com integridade garantida", extra={"evento": "DOWNLOAD_END", "identificador": remote_path})
        return calculated_file_hash

    def list_dir(self, remote_path: str = "/") -> list[dict]:
        logger.info(f"Listando diretório", extra={"evento": "LIST_DIR", "identificador": remote_path})
        res = self._rpc_controller("LIST_DIR", {"remote_path": remote_path}, identificador=remote_path)
        return res.get("entries", [])

    def delete_file(self, remote_path: str):
        logger.info(f"Solicitando exclusão", extra={"evento": "DELETE_FILE", "identificador": remote_path})
        self._rpc_controller("DELETE_FILE", {"remote_path": remote_path}, identificador=remote_path)
