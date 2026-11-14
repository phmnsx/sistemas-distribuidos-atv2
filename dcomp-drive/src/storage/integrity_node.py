"""Acréscimo de funcionalidades pra classe StorageNodeServer, de modo que aplique o RF4"""
import hashlib
import json
import logging
import os
import signal
import threading
import time

from src.storage.node import NodeRequestError, StorageNodeServer, parse_args


class IntegrityNode(StorageNodeServer):
    def __init__(self, *args, scrub_interval: float = 300.0, **kwargs):
        super().__init__(*args, **kwargs)  
        self.scrub_interval = scrub_interval
        self.corrupt_path = self.storage_path / "corrupt"


    def start(self):
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.corrupt_path.mkdir(exist_ok=True)
        self._cleanup_tmp() 

        super().start()  
        if self.scrub_interval and self.scrub_interval > 0:
            threading.Thread(target=self._scrub_loop, name="scrubber", daemon=True).start()

    def _cleanup_tmp(self):
        #Apaga arquivos que são temporários que sobram quando o nó cai durante o processo da gravação
        for leftover in self.storage_path.glob(".*.tmp"):
            leftover.unlink(missing_ok=True)
            logging.warning("Removido resto de gravação incompleta: %s", leftover.name)


    def _put_block(self, metadata: dict, data: bytes) -> int: 
        block_id = metadata.get("block_id")
        expected = metadata.get("sha256")

        block_path = self._block_path(block_id)  
        if hashlib.sha256(data).hexdigest() != str(expected).lower():
            raise NodeRequestError("HASH_MISMATCH", "O hash do bloco recebido não corresponde ao informado.")

        self._write_meta(block_id, str(expected).lower(), len(data))
        try:
            return super()._put_block(metadata, data)  
        except BaseException:
            if not block_path.exists():
                self._meta_path(block_id).unlink(missing_ok=True)
            raise


    def _get_block(self, metadata: dict) -> bytes: #verificação do bloco com o hash armazenado
        data = super()._get_block(metadata) 
        block_id = metadata.get("block_id")
        if not self._matches_meta(block_id, data):
            self._quarantine(block_id) #vai pra quarentena ao não bater além de levantar erro
            raise NodeRequestError("BLOCK_CORRUPT", f"Bloco {block_id} corrompido neste nó.")
        return data


    def scrub_once(self) -> tuple[int, list[str]]:
        checked, corrupt = 0, []
        for path in sorted(self.storage_path.iterdir()):
            if not path.is_file() or path.name.startswith(".") or path.name.endswith(".meta"):
                continue
            block_id = path.name
            try:
                data = path.read_bytes()
            except FileNotFoundError:
                continue  
            checked += 1
            if not self._matches_meta(block_id, data): 
                self._quarantine(block_id)
                corrupt.append(block_id)
                logging.error("Scrubber: bloco %s corrompido, movido para quarentena", block_id)  
        logging.info("Scrubbing concluído: %d verificado(s), %d corrompido(s)", checked, len(corrupt))
        return checked, corrupt

    def _scrub_loop(self): #verificação dos blocos a cada intervalo definido no scrub_interval para comparar com o hash que tá sendo guardado
        while not self._stop_event.wait(self.scrub_interval):
            try:
                self.scrub_once()
            except Exception:
                logging.exception("Falha no scrubbing")


    def _meta_path(self, block_id: str):
        return self.storage_path / f"{block_id}.meta"

    def _write_meta(self, block_id: str, sha256: str, size: int):
        tmp = self.storage_path / f".{block_id}.meta.{os.getpid()}.tmp"
        with open(tmp, "wb") as f:
            f.write(json.dumps({"sha256": sha256, "size": size}).encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path(block_id))

    def _read_expected_hash(self, block_id: str) -> str | None:
        try:
            return json.loads(self._meta_path(block_id).read_text(encoding="utf-8")).get("sha256")
        except (FileNotFoundError, ValueError):
            return None

    def _matches_meta(self, block_id: str, data: bytes) -> bool:
        expected = self._read_expected_hash(block_id)
        return expected is not None and hashlib.sha256(data).hexdigest() == expected

    def _quarantine(self, block_id: str):
        stamp = int(time.time())
        for src, suffix in ((self._block_path(block_id), ""), (self._meta_path(block_id), ".meta")):
            try:
                os.replace(src, self.corrupt_path / f"{block_id}.{stamp}{suffix}")
            except FileNotFoundError:
                pass


def main():
    args = parse_args()  
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    node = IntegrityNode(
        node_id=args.node_id, host=args.host, port=args.port, storage_path=args.storage_path,
        scrub_interval=float(os.getenv("SCRUB_INTERVAL", "300")),
    )
    signal.signal(signal.SIGINT, lambda *_: node.request_stop())
    signal.signal(signal.SIGTERM, lambda *_: node.request_stop())
    try:
        node.start()
        node.serve_forever()
    finally:
        node.stop()


if __name__ == "__main__":
    main()
