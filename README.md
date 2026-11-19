# DComp Drive

Sistema distribuído de armazenamento de arquivos com replicação, verificação de integridade por hash e recuperação automática de falhas.

## Equipe

- Diogo Lima de Oliveira
- Hugo Santos da Paixão
- Kevin Teixeira de Araújo Carvalho
- Pedro Henrique Macedo Antunes
- Ysrael de Jesus Sacramento

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
- **Cliente (CLI):** divide o arquivo em blocos de 4 MB, calcula o SHA-256, cifra com AES-256-GCM (opcional) e envia os dados direto aos nós.
- **ZooKeeper:** descoberta de nós, eleição do controlador (um ativo e um reserva) e locks.

Regras principais: cada bloco tem N réplicas (padrão 2) em nós diferentes; arquivos só ficam visíveis depois de completos; uma réplica corrompida é descartada e refeita a partir de uma cópia íntegra.

## Como subir

Requisito: Docker e Docker Compose.

```
cd dcomp-drive
docker compose up -d --build
```

Sobe ZooKeeper, PostgreSQL, 1 controlador e 5 nós de armazenamento. Para incluir também o controlador reserva (failover):

```
docker compose --profile ha up -d
```

Para parar tudo e apagar os metadados:

```
docker compose --profile ha down -v
```

Aguarde alguns segundos e confira o estado (comando `status`, abaixo): os 5 nós devem aparecer como VIVO.

## Como usar

A CLI roda dentro do container do controlador:

```
docker exec dfs-controller python -m src.client.cli <comando>
```

| Comando | O que faz |
|---|---|
| `put <local> <remoto>` | Envia um arquivo, com replicação |
| `get <remoto> <destino>` | Baixa o arquivo e verifica o hash |
| `ls [caminho]` | Lista um diretório |
| `rm <remoto>` | Remove um arquivo |
| `mkdir <caminho>` / `rmdir <caminho>` | Cria / remove diretório |
| `mv <origem> <destino>` | Move ou renomeia |
| `stat <remoto>` | Tamanho, hash, blocos, nós de cada réplica e estado de replicação |
| `status` | Nós (vivos/suspeitos/mortos), espaço por disco, arquivos por estado e re-replicações em andamento |
| `keygen` | Gera uma chave AES-256 |

Os caminhos `<local>` e `<destino>` ficam **dentro do container** (use `/tmp`). Para levar um arquivo do seu computador para o container, use `docker cp`.

### Exemplo completo

**1. Crie um arquivo de teste de 10 MB** (ou use qualquer arquivo seu).

PowerShell (Windows):

```
$b = New-Object byte[] 10MB; (New-Object Random).NextBytes($b); [IO.File]::WriteAllBytes("$PWD\arquivo.bin", $b)
```

Linux / macOS:

```
dd if=/dev/urandom of=arquivo.bin bs=1M count=10
```

**2. Grave e consulte:**

```
docker cp ./arquivo.bin dfs-controller:/tmp/arquivo.bin
docker exec dfs-controller python -m src.client.cli put /tmp/arquivo.bin /arquivo.bin
docker exec dfs-controller python -m src.client.cli stat /arquivo.bin
```

O `stat` mostra 3 blocos, cada um em 2 nós diferentes.

**3. Baixe e compare os hashes (devem ser iguais):**

```
docker exec dfs-controller python -m src.client.cli get /arquivo.bin /tmp/copia.bin
docker exec dfs-controller sha256sum /tmp/arquivo.bin /tmp/copia.bin
```

**4. Diretórios:**

```
docker exec dfs-controller python -m src.client.cli mkdir /docs
docker exec dfs-controller python -m src.client.cli mv /arquivo.bin /docs/arquivo.bin
docker exec dfs-controller python -m src.client.cli ls /docs
docker exec dfs-controller python -m src.client.cli rm /docs/arquivo.bin
docker exec dfs-controller python -m src.client.cli rmdir /docs
```

**5. Estado do sistema:**

```
docker exec dfs-controller python -m src.client.cli status
```

### Criptografia

Gere a chave e guarde-a numa variável. Quem perder a chave não consegue mais ler o arquivo. O `--key` vem **antes** do comando, e é preciso usá-lo também no `get`.

PowerShell:

```
$chave = docker exec dfs-controller python -m src.client.cli keygen
docker exec dfs-controller python -m src.client.cli --key $chave put /tmp/arquivo.bin /secreto.bin
docker exec dfs-controller python -m src.client.cli stat /secreto.bin
docker exec dfs-controller python -m src.client.cli --key $chave get /secreto.bin /tmp/secreto_out.bin
docker exec dfs-controller sha256sum /tmp/arquivo.bin /tmp/secreto_out.bin
```

Linux / macOS: troque a primeira linha por `chave=$(docker exec dfs-controller python -m src.client.cli keygen)` e use `$chave` do mesmo jeito.

O `stat` mostra `criptografado: sim`, e os dois hashes finais são iguais.

## Demonstrações

Grave antes um arquivo (passo 2 do exemplo completo).

**Falha de nó.** Veja com `stat /arquivo.bin` em quais nós ele está e derrube um deles (exemplo: `node4`):

```
docker stop dfs-storage-4
docker exec dfs-controller python -m src.client.cli status
```

O `status` mostra o nó como MORTO e, em poucos segundos, as cópias refeitas em outro nó até os arquivos voltarem a INTEGRO. Para religar: `docker start dfs-storage-4`.

**Corrupção.** Rode `stat /arquivo.bin`, escolha um `block_id` e um nó que o guarda (exemplo: `node1`) e corrompa o bloco no disco, a partir da pasta `dcomp-drive`.

PowerShell:

```
Add-Content -Path ".\data\node1\<block_id>" -Value "corrompido" -NoNewline
```

Linux / macOS:

```
echo corrompido >> data/node1/<block_id>
```

Depois baixe o arquivo:

```
docker exec dfs-controller python -m src.client.cli get /arquivo.bin /tmp/copia2.bin
```

O log do cliente mostra `CORRUPTION_DETECTED`, o arquivo é lido de outra réplica e termina com "integridade garantida". Se a corrupção não aparecer, a outra réplica foi lida primeiro: repita o `get`.

**Failover do controlador.** Suba o reserva (`docker compose --profile ha up -d`) e derrube o ativo:

```
docker kill dfs-controller
docker exec dfs-controller-2 python -m src.client.cli --controller tcp://localhost:5000 status
```

Se aparecer `Resource temporarily unavailable`, o reserva ainda está assumindo: espere alguns segundos e repita. Para voltar ao normal: `docker start dfs-controller`.

## Testes

Com o ambiente no ar (`docker compose up -d --build`), rode dentro do container. Não precisa instalar nada:

```
docker exec -e TEST_DB_HOST=postgres -e TEST_DB_PORT=5432 dfs-controller python -m pytest
```

Resultado esperado: `112 passed`. Os testes usam um schema temporário no PostgreSQL, que é apagado no final, então não afetam os metadados do sistema.

Também é possível rodar direto no seu computador, a partir da pasta `dcomp-drive`, com o PostgreSQL do compose no ar:

```
pip install -r requirements.txt
python -m pytest
```

> Sem os parâmetros `TEST_DB_HOST` e `TEST_DB_PORT`, os testes que usam banco são ignorados dentro do container, em vez de executados.

## Configuração

Variáveis de ambiente definidas no `docker-compose.yml`, sem necessidade de recompilar:

| Variável | Padrão | Descrição |
|---|---|---|
| `REPLICATION_FACTOR` | 2 | Número de réplicas por bloco |
| `MAX_DISK_USAGE` | 0.90 | Disco acima dessa fração de uso não recebe novas réplicas |
| `SCRUB_INTERVAL` | 300 | Intervalo da verificação de integridade nos nós (segundos) |
| `LOG_LEVEL` | INFO | Nível de log |
| `DFS_AES_KEY` | — | Chave AES-256 (hex) usada pela CLI; equivale a `--key` |

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
