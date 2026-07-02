# Raft sobre gRPC

Implementação do protocolo Raft com 4 nós em Python, comunicação via gRPC/Protocol Buffers,
persistência de estado, recuperação de falhas e cliente externo em Go (interoperabilidade).

- `raft-grpc/` — versão atual (gRPC)
- `raft-pyro/` — versão anterior (Pyro5), mantida como referência

## Estrutura (raft-grpc)

| Arquivo | Descrição |
|---|---|
| `proto/server.proto` | contrato interno do Raft (AppendEntries, RequestVote, ferramentas do viz) |
| `proto/client.proto` | contrato da aplicação — **único** proto que o cliente conhece (Publish/Consume) |
| `main.py` | nó Raft (follower/candidate/leader) com persistência em disco |
| `client-go/` | cliente em Go: publica e consome, descobre o líder por redirecionamento |
| `viz.py` | painel TUI para observar o cluster e simular falhas |

## Rodar

Requisitos: Docker + Docker Compose. Tudo é buildado nas imagens (stubs gerados no build).

```bash
cd raft-grpc

# subir o cluster (4 nós)
docker compose up -d --build raft-0 raft-1 raft-2 raft-3

# cluster + painel de visualização em um comando
docker compose --profile tools run --rm viz
```

### Cliente (Go)

```bash
# modo interativo
docker compose run --rm client
> publish ola mundo
> read        # lê de qualquer nó
> read 2      # lê da réplica 2

# ou one-shot
docker compose run --rm client publish valor1 valor2
docker compose run --rm client read
```

### Teclas do viz

`1-4` derruba/restaura nó · `m` publica · `z x c v` partição · `+`/`-` velocidade · `q` sai.
Entradas em **verde** = efetivadas (committed).

## Cenários de demonstração

```bash
# Falha do líder: descubra o líder no viz e derrube o container
docker compose stop raft-X        # novo líder é eleito; publish/read seguem funcionando

# Persistência: reinicie um nó — ele volta com term/log/commit do disco
docker compose stop raft-2 && docker compose start raft-2
docker compose logs raft-2 | grep RESTORED

# Recuperação de réplica: derrube, escreva, religue — só as entradas ausentes são enviadas
docker compose stop raft-2
docker compose run --rm client publish a b c
docker compose start raft-2

# Sem quórum (maioria = 3 de 4): com 2 nós fora, escritas são rejeitadas
docker compose stop raft-1 raft-2
docker compose run --rm client publish x   # rejeitado após ~5s
```

## Encerrar

```bash
docker compose down       # para os containers (estado persistido é mantido)
docker compose down -v    # para e APAGA o estado persistido dos nós
```

## Desenvolvimento local (opcional, sem Docker)

Os stubs Python não são versionados; gere-os a partir dos protos:

```bash
pip install -r raft-grpc/requirements.txt
cd raft-grpc
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. \
    proto/server.proto proto/client.proto
```
