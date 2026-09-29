# Roteiro de evidencias (PowerShell). Uso: .\evidencias\testes.ps1 -ApiKey "<valor do .env>"
param([string]$ApiKey = "troque-esta-chave", [string]$Base = "http://localhost:8080")
$h = @{ "X-API-Key" = $ApiKey; "Content-Type" = "application/json" }
function Post($obj) { Invoke-RestMethod -Method Post -Uri "$Base/api/v1/comandos" -Headers $h -Body ($obj | ConvertTo-Json) }

Write-Host "`n[1] Criar comando valido (201, resposta imediata)" -ForegroundColor Cyan
$c = Post @{ residencia_id="casa-01"; dispositivo_id="luz-sala"; tipo_dispositivo="luz"; acao="ligar" }
$c | ConvertTo-Json
Start-Sleep 3

Write-Host "`n[2] Consultar status (consistencia eventual: EXECUTADA)" -ForegroundColor Cyan
Invoke-RestMethod -Uri "$Base/api/v1/comandos/$($c.comando_id)" -Headers $h | ConvertTo-Json

Write-Host "`n[3] Notificacoes do comando (pub/sub)" -ForegroundColor Cyan
Invoke-RestMethod -Uri "$Base/api/v1/comandos/$($c.comando_id)/notificacoes" -Headers $h | ConvertTo-Json

Write-Host "`n[4] Dados invalidos -> 400" -ForegroundColor Cyan
try { Post @{ residencia_id="casa-01"; dispositivo_id="x"; tipo_dispositivo="foguete"; acao="ligar" } }
catch { "HTTP " + [int]$_.Exception.Response.StatusCode }

Write-Host "`n[5] Sem API key -> 401" -ForegroundColor Cyan
try { Invoke-RestMethod -Method Post -Uri "$Base/api/v1/comandos" -ContentType "application/json" -Body '{}' }
catch { "HTTP " + [int]$_.Exception.Response.StatusCode }

Write-Host "`n[6] Dispositivo defeituoso -> retentativas e DLQ" -ForegroundColor Cyan
$f = Post @{ residencia_id="casa-01"; dispositivo_id="tomada-defeituoso"; tipo_dispositivo="tomada"; acao="ligar" }
Start-Sleep 10
Invoke-RestMethod -Uri "$Base/api/v1/comandos/$($f.comando_id)" -Headers $h | ConvertTo-Json
Write-Host "Veja a fila q.dlq em http://localhost:15672"

Write-Host "`n[7] Estado P2P replicado nas duas instancias do executor" -ForegroundColor Cyan
Invoke-RestMethod -Uri "http://localhost:8011/" | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri "http://localhost:8012/" | ConvertTo-Json -Depth 5
