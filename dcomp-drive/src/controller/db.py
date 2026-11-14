import logging
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


""" Scheema de metadados do DFS """

SCHEMA = """
CREATE TABLE IF NOT EXISTS inodes (
    id BIGSERIAL PRIMARY KEY,
    parent_id BIGINT REFERENCES inodes(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    is_dir BOOLEAN NOT NULL DEFAULT FALSE,
    current_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mtime TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (parent_id, name)
);

INSERT INTO inodes (id, parent_id, name, is_dir)
VALUES (1, NULL, '', TRUE)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS file_versions (
    version_id TEXT PRIMARY KEY,
    inode_id BIGINT NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    file_sha256 TEXT,
    encrypted BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL,
    writer TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_versions_state
    ON file_versions(state);

CREATE INDEX IF NOT EXISTS idx_versions_inode
    ON file_versions(inode_id);

CREATE TABLE IF NOT EXISTS blocks (
    block_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL
        REFERENCES file_versions(version_id) ON DELETE CASCADE,
    idx INTEGER NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    sha256 TEXT NOT NULL,
    state TEXT NOT NULL,
    UNIQUE (version_id, idx)
);

CREATE TABLE IF NOT EXISTS replicas (
    block_id TEXT NOT NULL
        REFERENCES blocks(block_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    state TEXT NOT NULL,
    disk TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (block_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_replicas_node
    ON replicas(node_id);

CREATE INDEX IF NOT EXISTS idx_replicas_scrub
    ON replicas(state, updated_at);

CREATE TABLE IF NOT EXISTS write_leases (
    path TEXT PRIMARY KEY,
    version_id TEXT NOT NULL
        REFERENCES file_versions(version_id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_version
    ON write_leases(version_id);

CREATE TABLE IF NOT EXISTS node_flags (
    node_id TEXT PRIMARY KEY,
    draining BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class Database:
    def __init__(self, cfg):
        self.cfg = cfg
        self.params = {
            "host": cfg.db_host,
            "port": cfg.db_port,
            "dbname": cfg.db_name,
            "user": cfg.db_user,
            "password": cfg.db_password,
        }
        self.maxconn = cfg.rpc_workers + 16
        self.pool = None

    def connect(self, retries=30, delay=2.0):
        if self.pool is not None:
            return

        """ Aguarda o postgre ficar disponível durante a inicialização """
        for attempt in range(1, retries + 1):
            try:
                self.pool = pool.ThreadedConnectionPool(
                    1,
                    self.maxconn,
                    **self.params,
                )
                logger.info("Conexão com PostgreSQL estabelecida.")
                return
            except psycopg2.OperationalError as error:
                logger.warning(
                    "Tentativa %s/%s de conexão com PostgreSQL falhou: %s",
                    attempt,
                    retries,
                    error,
                )

                if attempt == retries:
                    raise RuntimeError(
                        "Não foi possível conectar ao PostgreSQL."
                    ) from error

                time.sleep(delay)

    def init_schema(self):
        self.connect()

        with self.tx() as cursor:
            
            """ Evita inicializações simultâneas do esquema """
            cursor.execute("SELECT pg_advisory_xact_lock(727274)")
            cursor.execute(SCHEMA)

        logger.info("Esquema do PostgreSQL inicializado.")

    @contextmanager
    def tx(self):
        if self.pool is None:
            self.connect()

        connection = self.pool.getconn()

        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            self.pool.putconn(
                connection,
                close=bool(connection.closed),
            )

    def close(self):
        if self.pool is not None:
            self.pool.closeall()
            self.pool = None
            logger.info("Conexões com PostgreSQL encerradas.")
