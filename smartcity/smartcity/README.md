# Smart City – Automação Residencial (ARP 01)

Sistema distribuído de comandos de automação residencial. Documentação técnica completa em [`docs/DOCUMENTACAO.md`](docs/DOCUMENTACAO.md).

## Como executar (Windows / PowerShell)

Pré-requisito: Docker Desktop.

```powershell
Copy-Item .env.example .env      # edite API_KEY e a senha do RabbitMQ
docker compose up --build
```

| Serviço | URL |
|---|---|
| API Gateway (Swagger/OpenAPI) | http://localhost:8080/docs |
| RabbitMQ (painel) | http://localhost:15672 (usuário/senha do `.env`) |
| Estado P2P executor-1 / executor-2 | http://localhost:8011 / http://localhost:8012 |

## Testando

```powershell
.\evidencias\testes.ps1 -ApiKey "<API_KEY do .env>"
```

Exemplo manual:

```powershell
$h = @{ "X-API-Key"="<API_KEY>"; "Content-Type"="application/json" }
$b = '{"residencia_id":"casa-01","dispositivo_id":"luz-sala","tipo_dispositivo":"luz","acao":"ligar"}'
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/v1/comandos -Headers $h -Body $b
```

Para ver a tolerância a falhas: `docker compose stop notification` e crie/consulte comandos normalmente (RNF01); `docker compose stop executor-2` e veja o líder mudar nos logs do executor-1.

## Estrutura

```
gateway/       API Gateway REST (FastAPI)
processing/    Serviço de Comandos (gRPC + outbox + banco próprio)
executor/      Executor (consumidor da fila) + cooperação P2P por sockets TCP
notification/  Serviço de Notificações (consumidor pub/sub + banco próprio)
common/        Log estruturado e topologia do RabbitMQ
proto/         Contrato gRPC
docs/          Documentação técnica e diagramas
evidencias/    Roteiro de testes
```
