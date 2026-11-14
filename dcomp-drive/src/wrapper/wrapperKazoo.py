import os
import json
import logging
import threading
import time
from kazoo.client import ChildrenWatch, Election, KazooClient, KazooState

""" Mantém uma única conexão ZooKeeper compartilhada pelo wrapper
Ela começa como None e será criada na primeira chamada de _get_zk() """

_zk: KazooClient | None = None

_path_list = [ "/storage/nodes",
              "/files",
              "/locks",
              "/config",
              "/election/controller",
              "/locks/dirs",
              "/locks/files",
              "/locks/alloc/",
              "/locks/drain/"
              ]

def _get_zk() -> KazooClient:
    global _zk
    if _zk is None:
        hosts = os.getenv("ZK_HOSTS")

        # impede a criação de uma conexão sem endereço configurado
        if not hosts:
            raise RuntimeError("A variável de ambiente ZK_HOSTS não foi definida.")

        _zk = KazooClient(hosts)
        _zk.add_listener(_handle_state)
        _zk.start()
        logging.info(f"Zookeeper conectado em {hosts}")
    return _zk

def init():
    """
    Inicializa o ZooKeeper. Sempre rode isso em controller e em cada nó na inicialização
    """
    zk = _get_zk()
    for path in _path_list:
        zk.ensure_path(path)
    logging.info("Init completo")

def register_storage(name, disks, available, addr, host=None):
    """
    Registra um nó de armazenamento no ZooKeeper.

    name: identificador do nó.
    disks: capacidade total de cada disco.
    available: espaço disponível em cada disco.
    addr: endereço RPC do nó.
    host: identificação da máquina onde o nó está executando.
    """
    if not name or not addr:
        raise ValueError("Nome e endereço do nó são obrigatórios.")

    data = {
        "addr": addr,
        "host": host or name,
        "disks": [
            {"disk": index, "total": disk, "available": disk_available}
            for index, (disk, disk_available) in enumerate(
                zip(disks, available), start=1
            )
        ],
    }

    path = f"/storage/nodes/{name}"
    zk = _get_zk()

    if zk.exists(path):
        zk.delete(path)

    zk.create(path, json.dumps(data).encode("utf-8"), ephemeral=True)
    logging.info("Nó %s registrado no ZooKeeper", name)

def get_node(path):
    """
    Retorna um node qualquer,
    Se não existe, retorna None
    """
    zk = _get_zk()
    if zk.exists(path):
        return zk.get(path)
    else:
        return None

def set_storage_watch(remove_func=None, add_func=None):
    """
    Recebe uma função de "remoção" e uma de "adição", executa tal função para cada nó (func(child_id)) deletado ou adicionado em /storage/nodes
    Retorna o watcher.
    """
    zk = _get_zk()
    previous = set()

    def _watcher(children):
        nonlocal previous
        if children is None:
            logging.warning("Nenhuma criança encontrada, talvez nó pai foi removido?")

        # childrenWatch pode fornecer None entao tratamos como lista vazia
        current = set(children or [])
        removed = previous - current
        added = current - previous

        for node_id in removed:
            logging.info(f"Criança {node_id} removida")
            if remove_func:
                remove_func(node_id)

        for node_id in added:
            logging.info(f"Criança {node_id} adicionada")
            if add_func:
                add_func(node_id)

        previous = current
    return ChildrenWatch(zk, "/storage/nodes", _watcher)

def election_watch(func=None):
    """
    Observa a eleição e chama func(lider_id) sempre que o lider mudar
    """
    zk = _get_zk()
    previous = None

    def _watch(children):
        nonlocal previous
        if not children:
            logging.warning("Nenhum candidato a eleicao")
            if previous is not None:
                if func:
                    func(None)
                previous = None
            return

        current = children[0]
        if current != previous:
            logging.info(f"Novo lider, Antes: {previous}, Agora: {current}")
            if func:
                func(current)
            previous = current

    return ChildrenWatch(zk, "/election/controller", _watch)

def election_controller(identifier:str) -> Election:
    """
    Retorna um objeto Election para entrar na fila de ser lider.
    Bloqueia apenas quando fizer .run()
    Identifier unico por réplica.
    """
    if not identifier or not identifier.strip():
        raise ValueError("Identifier obrigatorio e deve ser unico")
    return _get_zk().Election("/election/controller", identifier)

def valid_session():
    """
    Retorna True se estiver conectado, caso contrario, false
    """
    return KazooState.CONNECTED == _get_zk().state

def _handle_state(state:KazooState):
    if state == KazooState.LOST:
        logging.error("Conexão perdida, qualquer ephemeral node foi removida.")
    if state == KazooState.SUSPENDED:
        logging.warning("Conexão suspensa")

def _callerID():
    return os.getenv("container_name")

def lock_file(file:str):
    """
    Lock para escrita no mesmo arquivo
    """
    if not file:
        raise ValueError("file_id é obrigatório")
    return _get_zk().Lock(f"/locks/files/{file}", _callerID())

def lock_dir(dir:str):
    """
    Lock para editar diretório
    Formato: lock_dir("/dir1") ou lock_dir("/dir1/dir2")
    """
    name = dir.strip("/").replace("/","__")
    return _get_zk().Lock(f"/locks/dirs/{name}", _callerID())

def lock_alloc():
    """
    Lock para alocação de blocos
    """
    return _get_zk().Lock("/locks/alloc", _callerID())

def lock_drain():
    """
    Lock para drain
    """
    return _get_zk().Lock("/locks/drain", _callerID())
    