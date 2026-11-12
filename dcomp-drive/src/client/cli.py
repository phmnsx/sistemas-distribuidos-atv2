import argparse
import logging
import os
import sys
from src.client.crypto import CryptoManager
from src.client.pipeline import DFSClient, DEFAULT_CHUNK_SIZE


class RF12Formatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'evento'):
            record.evento = 'SISTEMA'
        if not hasattr(record, 'identificador'):
            record.identificador = 'N/A'
        return super().format(record)

def setup_logging(debug: bool, log_file: str):
    level = logging.DEBUG if debug else logging.INFO
    rf12_format = "%(asctime)s | COMP:%(name)s | EVT:%(evento)s | ID:%(identificador)s | MSG:%(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    formatter = RF12Formatter(fmt=rf12_format, datefmt=date_format)

    handlers = []
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers)


def main():
    parser = argparse.ArgumentParser(description="CLI do Cliente - DFS")
    parser.add_argument("--controller", default=os.getenv("DFS_CONTROLLER", "tcp://localhost:5000"))
    parser.add_argument("--key", default=os.getenv("DFS_AES_KEY"), help="Chave AES-256 em hex (opcional)")
    parser.add_argument("--chunk-mb", type=int, default=DEFAULT_CHUNK_SIZE // (1024 * 1024))
    parser.add_argument("--debug", action="store_true", help="Ativa logs detalhados (DEBUG)")
    parser.add_argument("--log-file", default=os.getenv("DFS_CLIENT_LOG", "dfs_client.log"))

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("keygen", help="Gera chave AES-256")
    
    put_p = subparsers.add_parser("put")
    put_p.add_argument("local_path")
    put_p.add_argument("remote_path")

    get_p = subparsers.add_parser("get")
    get_p.add_argument("remote_path")
    get_p.add_argument("dest_path")

    ls_p = subparsers.add_parser("ls")
    ls_p.add_argument("remote_path", nargs="?", default="/")

    rm_p = subparsers.add_parser("rm")
    rm_p.add_argument("remote_path")

    args = parser.parse_args()

    if args.command == "keygen":
        print(os.urandom(32).hex())
        sys.exit(0)

    setup_logging(debug=args.debug, log_file=args.log_file)

    crypto = CryptoManager(key_hex=args.key)
    chunk_bytes = args.chunk_mb * 1024 * 1024
    client = DFSClient(controller_addr=args.controller, crypto=crypto, chunk_size=chunk_bytes)

    try:
        if args.command == "put":
            client.put_file(args.local_path, args.remote_path)
        elif args.command == "get":
            client.get_file(args.remote_path, args.dest_path)
        elif args.command == "ls":
            for item in client.list_dir(args.remote_path):
                tipo = "DIR " if item.get("is_directory") else "FILE"
                print(f"[{tipo}] {item.get('name')} ({item.get('size_bytes', 0)} bytes)")
        elif args.command == "rm":
            client.delete_file(args.remote_path)
    except Exception as e:
        logging.getLogger("ClienteDFS").error(f"Operação abortada: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
