# Desenvolvimento

## Workflow por fase (PR)

Antes de iniciar **cada nova fase** do plano de evolução:

1. Atualizar `master` local com o remoto:
   ```bash
   git checkout master
   git pull origin master
   ```
2. Criar uma **branch nova** a partir de `master`:
   ```bash
   git checkout -b feat/pr-N-descricao-curta
   ```
3. Implementar apenas o escopo da fase (PR) correspondente.
4. Abrir PR apontando para `master` em `Joaquimborges/go-socket.io` (não para o upstream `feederco/go-socket.io`).

## Fases do plano

Ver [PLANO_EVOLUCAO_CLIENTE.md](./PLANO_EVOLUCAO_CLIENTE.md).

| PR | Escopo |
|---|---|
| PR-1 | Remover servidor, exemplos, deps Redis/UUID |
| PR-2 | Cliente mínimo (sem namespaces/rooms/ACK) |
| PR-3 | Estabilidade v4 (EIO=4, PING→PONG, WebSocket default) |
| PR-4 | Reconnect automático |
| PR-5 | Testes de integração + soak + README final |

## Testes locais

```bash
go mod tidy
go test ./... -race
```
