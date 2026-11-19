# DComp Drive

Sistema distribuído de armazenamento de arquivos com replicação, verificação de integridade por hash e recuperação automática de falhas.

## Equipe

**Diogo Lima de Oliveira**
**Hugo Santos da Paixão**
**Kevin Teixeira de Araújo Carvalho**
**Pedro Henrique Macedo Antunes**
**Ysrael de Jesus Sacramento**


## Tecnologias

| Ferramenta | Papel no projeto |
|---|---|
| **Python 3.12** | Linguagem do controlador, dos nós de armazenamento e do cliente |
| **ZeroMQ (pyzmq)** | Comunicação entre cliente, controlador e nós (requisição/resposta de metadados e transferência de blocos) |
| **ZooKeeper 3.8 (kazoo)** | Descoberta automática de nós, eleição do controlador ativo/reserva e locks |
| **PostgreSQL 16** | Persistência dos metadados (diretórios, arquivos, blocos, réplicas, leases) |
| **hashlib (SHA-256)** | Hash de cada bloco e do arquivo, verificado na gravação, na leitura e no scrubbing |
| **cryptography (AES-256-GCM)** | Criptografia opcional dos dados, feita no cliente |
| **Docker e Docker Compose** | Sobe todo o ambiente (controlador, nós, ZooKeeper, PostgreSQL) com poucos comandos |
| **pytest** | Testes automatizados |


## Arquitetura

- **Controlador:** guarda os metadados (árvore de diretórios, arquivo → blocos → réplicas) no PostgreSQL, decide onde cada bloco é gravado, detecta nós mortos e re-replica. Também faz o papel de nó de metadados.
- **Nós de armazenamento:** guardam e servem blocos, verificam o hash de cada bloco e rodam um scrubber periódico.
- **Cliente (CLI):** divide o arquivo em blocos de 4 MB, calcula o SHA-256, cifra com AES-256-GCM e envia os dados direto aos nós. 
- **ZooKeeper:** descoberta de nós, eleição do controlador (um ativo e um reserva) e locks.

Regras principais: cada bloco tem N réplicas (padrão 2) em nós diferentes; arquivos só ficam visíveis depois de completos; uma réplica corrompida é descartada e refeita a partir de uma cópia íntegra.


## Como subir

Requisitos: Docker e Docker Compose.

```
cd dcomp-drive
docker compose up -d --build
```

Sobe ZooKeeper, PostgreSQL, 1 controlador e 5 nós de armazenamento. Para incluir o controlador reserva (failover):

```
docker compose --profile ha up -d
```

Aguarde alguns segundos e confira com `status` (abaixo): os 5 nós devem aparecer como VIVO.


## Como usar

A CLI roda dentro do container do controlador:

```
docker exec dfs-controller python -m src.client.cli --controller tcp://localhost:5000 <comando>
```

| Comando | O que faz |
|---|---|
| `put <local> <remoto>` | Envia um arquivo, com replicação |
| `get <remoto> <destino>` | Baixa e verifica o hash |
| `ls [caminho]` | Lista um diretório |
| `rm <remoto>` | Remove um arquivo |
| `mkdir <caminho>` / `rmdir <caminho>` | Cria / remove diretório |
| `mv <origem> <destino>` | Move ou renomeia |
| `stat <remoto>` | Tamanho, hash, blocos, localização das réplicas e estado (íntegro / sub-replicado) |
| `status` | Nós (vivos/suspeitos/mortos), espaço por disco, arquivos por estado e re-replicações em andamento |
| `keygen` | Gera uma chave AES-256 |

Exemplos:

```
docker cp ./arquivo.bin dfs-controller:/tmp/arquivo.bin
docker exec dfs-controller python -m src.client.cli put /tmp/arquivo.bin /arquivo.bin
docker exec dfs-controller python -m src.client.cli stat /arquivo.bin
docker exec dfs-controller python -m src.client.cli get /arquivo.bin /tmp/copia.bin
```

## Demonstrações realizadas

1. **Replicação:** `put` de um arquivo de 10 MB e `stat` mostram 3 blocos, cada um em 2 nós diferentes.
2. **Falha de nó:** `docker stop dfs-storage-2`; em seguida `status` mostra o nó como MORTO, arquivos SUB-REPLICADOS e as cópias em andamento, até voltarem a INTEGRO. Reative com `docker start dfs-storage-2`.
3. **Corrupção:** altere um arquivo de bloco em `data/nodeN/` e faça `get`. O nó acusa o bloco corrompido, o cliente lê de outra réplica, o hash final confere e o controlador refaz a réplica.
4. **Failover do controlador:** `docker kill dfs-controller`. Cerca de 15 s depois o `dfs-controller-2` assume. Passe a usar a CLI dentro dele (`docker exec dfs-controller-2 ...`).


## Configuração

Definidas por variáveis de ambiente (no `docker-compose.yml`):

| Variável | Padrão | Descrição |
|---|---|---|
| `REPLICATION_FACTOR` | 2 | Número de réplicas por bloco |
| `MAX_DISK_USAGE` | 0.90 | Disco acima disso não recebe novas réplicas (o compose usa 1.0 porque o disco do host de desenvolvimento estava quase cheio) |
| `SUSPECT_AFTER_S` / `DEAD_AFTER_S` | 10 / 30 | Tempos para marcar um nó como suspeito / morto |
| `SCRUB_INTERVAL` | 300 | Intervalo do scrubber nos nós (segundos) |
| `LOG_LEVEL` | INFO | Nível de log |
| `DFS_AES_KEY` | — | Chave AES-256 (hex) usada pela CLI |


## Testes

```
cd dcomp-drive
python -m pytest
```

Os testes que usam banco precisam de um PostgreSQL acessível e são ignorados sem ele. Também é possível rodar dentro do container: `docker exec dfs-controller python -m pytest`.


## Estrutura

```
dcomp-drive/
├── src/client/      CLI e pipeline de envio/leitura
├── src/controller/  servidor, handlers, metadados (SQL), replicator, posicionamento
├── src/storage/     nó de armazenamento e verificação de integridade
├── src/wrapper/     acesso ao ZooKeeper
├── tests/
├── Dockerfile
└── docker-compose.yml
```

