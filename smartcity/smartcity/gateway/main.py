"""API Gateway (REST): autentica, valida e encaminha ao servico de comandos via gRPC."""
import os
import sys
import uuid
from typing import Literal, Optional

import grpc
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import smartcity_pb2 as pb
import smartcity_pb2_grpc as pbg
from common.logs import Log

log = Log("gateway")
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    sys.exit("API_KEY nao definida (use o arquivo .env)")
PROCESSING_ADDR = os.environ["PROCESSING_ADDR"]
NOTIFICATION_URL = os.environ["NOTIFICATION_URL"]

stub = pbg.ComandoServiceStub(grpc.insecure_channel(PROCESSING_ADDR))
app = FastAPI(title="Smart City - Automação Residencial",
              description="API Gateway do sistema distribuído de comandos de automação residencial.",
              version="1.0.0")


class ComandoIn(BaseModel):
    residencia_id: str = Field(min_length=1, max_length=64, examples=["casa-01"])
    dispositivo_id: str = Field(min_length=1, max_length=64, examples=["luz-sala"])
    tipo_dispositivo: Literal["luz", "ar_condicionado", "tranca", "cortina", "camera", "tomada"]
    acao: Literal["ligar", "desligar", "ajustar", "trancar", "destrancar"]
    valor: Optional[str] = Field(default=None, max_length=32, examples=["22"])


def auth(x_api_key: Optional[str] = Header(default=None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key ausente ou inválida")


@app.middleware("http")
async def correlation(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    request.state.cid = cid
    resp = await call_next(request)
    resp.headers["X-Correlation-ID"] = cid
    log.info(f"{request.method} {request.url.path} -> {resp.status_code}", cid)
    return resp


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    detalhes = [{"campo": ".".join(str(p) for p in e["loc"][1:]), "mensagem": e["msg"]}
                for e in exc.errors()]
    return JSONResponse(status_code=400, content={"erro": "dados inválidos", "detalhes": detalhes})


def _dict(r):
    return {"comando_id": r.comando_id, "status": r.status, "residencia_id": r.residencia_id,
            "dispositivo_id": r.dispositivo_id, "tipo_dispositivo": r.tipo_dispositivo,
            "acao": r.acao, "valor": r.valor or None, "detalhe": r.detalhe,
            "criado_em": r.criado_em, "atualizado_em": r.atualizado_em,
            "correlation_id": r.correlation_id}


def _grpc_error(e: grpc.RpcError):
    code = e.code()
    if code == grpc.StatusCode.NOT_FOUND:
        raise HTTPException(404, "comando não encontrado")
    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
        raise HTTPException(503, "serviço de comandos indisponível")
    raise HTTPException(500, "erro interno")


@app.get("/health", tags=["infra"])
def health():
    return {"status": "ok"}


@app.post("/api/v1/comandos", status_code=201, tags=["comandos"], dependencies=[Depends(auth)])
def criar(body: ComandoIn, request: Request, response: Response):
    try:
        r = stub.CriarComando(pb.CriarComandoRequest(
            correlation_id=request.state.cid, residencia_id=body.residencia_id,
            dispositivo_id=body.dispositivo_id, tipo_dispositivo=body.tipo_dispositivo,
            acao=body.acao, valor=body.valor or ""), timeout=3)
    except grpc.RpcError as e:
        _grpc_error(e)
    response.headers["Location"] = f"/api/v1/comandos/{r.comando_id}"
    return _dict(r)  # responde antes do processamento assincrono (RNF08)


@app.get("/api/v1/comandos/{comando_id}", tags=["comandos"], dependencies=[Depends(auth)])
def consultar(comando_id: str, request: Request):
    try:
        r = stub.ConsultarComando(pb.ConsultarComandoRequest(
            correlation_id=request.state.cid, comando_id=comando_id), timeout=3)
    except grpc.RpcError as e:
        _grpc_error(e)
    return _dict(r)


@app.get("/api/v1/comandos/{comando_id}/notificacoes", tags=["comandos"],
         dependencies=[Depends(auth)])
def notificacoes(comando_id: str, request: Request):
    try:
        r = httpx.get(f"{NOTIFICATION_URL}/notificacoes", params={"comando_id": comando_id},
                      headers={"X-Correlation-ID": request.state.cid}, timeout=3)
        return r.json()
    except Exception:
        # degrada apenas este endpoint; criar/consultar seguem funcionando (RNF01)
        raise HTTPException(503, "serviço de notificações indisponível")
