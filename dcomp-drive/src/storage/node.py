"""Servidor RPC e armazenamento local de blocos do DFS"""

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import tempfile
import threading
from pathlib import Path

import zmq

from src.wrapper import wrapperKazoo


BLOCK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$") # regex
SHA256_PATTERN = re.compile(r"^[a-fA-F0-9]{64}$")


class NodeRequestError(Exception):
    """Erro de validação ou execução de uma operação RPC"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class StorageNodeServer:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        storage_path: str,
    ):
        if not node_id or not node_id.strip():
            raise ValueError("O identificador do nó é obrigatório.")

        if not host or not host.strip():
            raise ValueError("O host anunciado pelo nó é obrigatório.")

        if not 1 <= port <= 65535:
            raise ValueError("A porta deve estar entre 1 e 65535.")

        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr = f"tcp://{host}:{port}"
        self.storage_path = Path(storage_path)

        self.context = zmq.Context()
        self.socket = None
        self._stop_event = threading.Event()

    def start(self):
        """Prepara o armazenamento, inicia o RPC e registra o nó """

        if self.socket is not None:
            raise RuntimeError("O servidor do nó já foi iniciado.")

        self.storage_path.mkdir(parents=True, exist_ok=True)

        socket = self.context.socket(zmq.ROUTER)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVTIMEO, 1000)

        try:
            socket.bind(f"tcp://*:{self.port}")
            self.socket = socket

            # O nó só é anunciado depois que sua porta RPC está disponível
            wrapperKazoo.init()

            usage = shutil.disk_usage(self.storage_path)

            wrapperKazoo.register_storage(
                self.node_id,
                disks=[usage.total],
                available=[usage.free],
                addr=self.addr,
                host=self.host,
            )

        except Exception:
            socket.close(0)
            self.socket = None
            raise

        logging.info(
            "Nó %s iniciado em %s, armazenando blocos em %s",
            self.node_id,
            self.addr,
            self.storage_path,
        )

    def serve_forever(self):
        """Recebe e processa requisições RPC até o encerramento"""

        if self.socket is None:
            raise RuntimeError("O servidor precisa ser iniciado antes.")

        while not self._stop_event.is_set():
            try:
                frames = self.socket.recv_multipart()
            except zmq.Again:
                continue

            self.handle_request(frames)

    def handle_request(self, frames: list[bytes]):
        """Processa uma mensagem ROUTER e responde ao cliente correspondente """

        if not frames:
            return

        identity = frames[0]
        req_id = None
        action = None
        response_data = None

        try:
            if len(frames) < 3 or frames[1] != b"":
                raise NodeRequestError(
                    "INVALID_REQUEST",
                    "Formato da mensagem RPC inválido.",
                )

            try:
                metadata = json.loads(frames[2].decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise NodeRequestError(
                    "INVALID_REQUEST",
                    "Metadados da requisição inválidos.",
                )

            if not isinstance(metadata, dict):
                raise NodeRequestError(
                    "INVALID_REQUEST",
                    "Os metadados devem ser um objeto JSON.",
                )

            req_id = metadata.get("req_id")
            action = metadata.get("action")

            if action == "PUT_BLOCK":
                if len(frames) != 4:
                    raise NodeRequestError(
                        "INVALID_REQUEST",
                        "PUT_BLOCK exige metadados e conteúdo do bloco.",
                    )

                block_size = self._put_block(metadata, frames[3])

                response = {
                    "status": "ACK",
                    "code": "OK",
                    "req_id": req_id,
                    "size_bytes": block_size,
                }

            elif action == "GET_BLOCK":
                if len(frames) != 3:
                    raise NodeRequestError(
                        "INVALID_REQUEST",
                        "GET_BLOCK exige somente os metadados.",
                    )

                response_data = self._get_block(metadata)

                response = {
                    "status": "ACK",
                    "code": "OK",
                    "req_id": req_id,
                    "size_bytes": len(response_data),
                }

            else:
                raise NodeRequestError(
                    "UNKNOWN_ACTION",
                    f"Operação não reconhecida: {action}",
                )

        except NodeRequestError as error:
            response = {
                "status": "ERR",
                "code": error.code,
                "msg": str(error),
                "req_id": req_id,
            }

            # O cliente espera três quadros mesmo quando GET_BLOCK falha.
            if action == "GET_BLOCK":
                response_data = b""

        except OSError as error:
            logging.exception("Erro de armazenamento no nó %s", self.node_id)

            response = {
                "status": "ERR",
                "code": "STORAGE_ERROR",
                "msg": str(error),
                "req_id": req_id,
            }

            if action == "GET_BLOCK":
                response_data = b""

        except Exception:
            logging.exception("Erro inesperado ao processar RPC no nó %s", self.node_id)

            response = {
                "status": "ERR",
                "code": "INTERNAL_ERROR",
                "msg": "Erro interno ao processar a requisição.",
                "req_id": req_id,
            }

            if action == "GET_BLOCK":
                response_data = b""

        self._send_response(identity, response, response_data)

    def _block_path(self, block_id: str) -> Path:
        """Valida o identificador antes de utilizá-lo como nome de arquivo """

        if not isinstance(block_id, str) or not BLOCK_ID_PATTERN.fullmatch(block_id):
            raise NodeRequestError(
                "INVALID_BLOCK_ID",
                "Identificador do bloco inválido.",
            )

        return self.storage_path / block_id

    def _put_block(self, metadata: dict, data: bytes) -> int:
        """Verifica o hash e grava o bloco por substituição atômica """

        block_id = metadata.get("block_id")
        expected_hash = metadata.get("sha256")

        block_path = self._block_path(block_id)

        if not isinstance(expected_hash, str) or not SHA256_PATTERN.fullmatch(expected_hash):
            raise NodeRequestError(
                "INVALID_HASH",
                "Hash SHA-256 inválido.",
            )

        actual_hash = hashlib.sha256(data).hexdigest()

        if actual_hash != expected_hash.lower():
            raise NodeRequestError(
                "HASH_MISMATCH",
                "O hash do bloco recebido não corresponde ao informado.",
            )

        temporary_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.storage_path,
                prefix=f".{block_id}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = temporary_file.name
                temporary_file.write(data)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            os.replace(temporary_path, block_path)
            temporary_path = None

        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)

        logging.debug("Bloco %s armazenado no nó %s", block_id, self.node_id)
        return len(data)

    def _get_block(self, metadata: dict) -> bytes:
        """Recupera um bloco armazenado localmente """

        block_id = metadata.get("block_id")
        block_path = self._block_path(block_id)

        try:
            return block_path.read_bytes()
        except FileNotFoundError:
            raise NodeRequestError(
                "BLOCK_NOT_FOUND",
                f"Bloco {block_id} não encontrado.",
            )

    def _send_response(
        self,
        identity: bytes,
        response: dict,
        data: bytes | None = None,
    ):
        """Preserva o delimitador e o formato de resposta esperado pelo DEALER """

        if self.socket is None:
            return

        frames = [
            identity,
            b"",
            json.dumps(response).encode("utf-8"),
        ]

        if data is not None:
            frames.append(data)

        self.socket.send_multipart(frames)

    def request_stop(self):
        """Solicita o encerramento do laço de atendimento """

        self._stop_event.set()

    def stop(self):
        """Fecha os recursos locais do servidor """

        self.request_stop()

        if self.socket is not None:
            self.socket.close(0)
            self.socket = None

        self.context.term()
        logging.info("Nó %s encerrado", self.node_id)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Servidor de armazenamento do DComp Drive."
    )

    parser.add_argument("--id", dest="node_id", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--storage-path", required=True)

    return parser.parse_args()


def main():
    args = parse_args()

    log_level = getattr(
        logging,
        os.getenv("LOG_LEVEL", "INFO").upper(),
        logging.INFO,
    )

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    node = StorageNodeServer(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        storage_path=args.storage_path,
    )

    signal.signal(signal.SIGINT, lambda signum, frame: node.request_stop())
    signal.signal(signal.SIGTERM, lambda signum, frame: node.request_stop())

    try:
        node.start()
        node.serve_forever()
    finally:
        node.stop()


if __name__ == "__main__":
    main()
