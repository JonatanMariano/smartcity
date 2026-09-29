#!/usr/bin/env bash
# Roteiro de evidencias (Linux/Codespaces). Uso: bash evidencias/testes.sh minhachave123
API_KEY="${1:-minhachave123}"; BASE="${2:-http://localhost:8080}"
H=(-H "X-API-Key: $API_KEY" -H "Content-Type: application/json")
post() { curl -s -X POST "$BASE/api/v1/comandos" "${H[@]}" -d "$1"; }
idof() { python3 -c 'import sys,json;print(json.load(sys.stdin)["comando_id"])'; }
pp() { python3 -m json.tool; }

echo; echo "[1] Criar comando valido (201, resposta imediata)"
R=$(post '{"residencia_id":"casa-01","dispositivo_id":"luz-sala","tipo_dispositivo":"luz","acao":"ligar"}')
echo "$R" | pp; ID=$(echo "$R" | idof); sleep 3

echo; echo "[2] Consultar status (consistencia eventual: EXECUTADA)"
curl -s "$BASE/api/v1/comandos/$ID" "${H[@]}" | pp

echo; echo "[3] Notificacoes do comando (pub/sub)"
curl -s "$BASE/api/v1/comandos/$ID/notificacoes" "${H[@]}" | pp

echo; echo "[4] Dados invalidos -> 400"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$BASE/api/v1/comandos" "${H[@]}" \
  -d '{"residencia_id":"casa-01","dispositivo_id":"x","tipo_dispositivo":"foguete","acao":"ligar"}'

echo; echo "[5] Sem API key -> 401"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$BASE/api/v1/comandos" \
  -H 'Content-Type: application/json' -d '{}'

echo; echo "[6] Dispositivo defeituoso -> retentativas e DLQ (aguarde ~10s)"
F=$(post '{"residencia_id":"casa-01","dispositivo_id":"tomada-defeituoso","tipo_dispositivo":"tomada","acao":"ligar"}' | idof)
sleep 10
curl -s "$BASE/api/v1/comandos/$F" "${H[@]}" | pp
echo "Veja a fila q.dlq no painel do RabbitMQ (porta 15672)"

echo; echo "[7] Estado P2P replicado nas duas instancias do executor"
curl -s localhost:8011/ | pp; curl -s localhost:8012/ | pp
