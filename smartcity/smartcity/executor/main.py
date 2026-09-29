"""Executor de comandos: competing consumer da fila q.executor + cooperacao P2P."""
import json
import os
import random
import sqlite3
import time
import uuid
from datetime import datetime, timezone

import pika

from common import broker
from common.logs import Log
from p2p import PeerNode, StateCache, start_http

INSTANCE_ID = os.environ["INSTANCE_ID"]
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))
P2P_PORT = int(os.environ.get("P2P_PORT", "9000"))
PEERS = os.environ.get("PEERS", "").split(",")
log = Log(INSTANCE_ID)

os.makedirs("/data", exist_ok=True)
db = sqlite3.connect(f"/data/{INSTANCE_ID}.db")
db.execute("CREATE TABLE IF NOT EXISTS processados(comando_id TEXT PRIMARY KEY)")
db.commit()

cache = StateCache()
node = PeerNode(INSTANCE_ID, P2P_PORT, PEERS, cache, log)

REQUIRED = ("comando_id", "correlation_id", "dispositivo_id", "acao")


def novo_estado(acao, valor):
    return {"ligar": "ligado", "desligar": "desligado", "trancar": "trancado",
            "destrancar": "destrancado"}.get(acao, f"ajustado:{valor}")


def executar(ev):
    time.sleep(0.5)  # simula a latencia do dispositivo fisico
    if "defeituoso" in ev["dispositivo_id"] or random.random() < FAIL_RATE:
        raise RuntimeError("dispositivo não respondeu")
    return novo_estado(ev["acao"], ev.get("valor"))


def resultado(ev, rk, status, detalhe):
    return {**ev, "evento_id": str(uuid.uuid4()), "tipo": rk, "status": status,
            "detalhe": detalhe, "ocorrido_em": datetime.now(timezone.utc).isoformat()}


def on_message(ch, m, props, body):
    try:
        ev = json.loads(body)
        assert all(k in ev for k in REQUIRED)
    except Exception:
        log.error("mensagem malformada -> DLQ")
        ch.basic_reject(m.delivery_tag, requeue=False)
        return
    cid, cmd_id = ev["correlation_id"], ev["comando_id"]
    retries = (props.headers or {}).get("x-retries", 0)

    if db.execute("SELECT 1 FROM processados WHERE comando_id=?", (cmd_id,)).fetchone():
        log.info(f"comando {cmd_id} duplicado, ignorando (idempotencia)", cid)
        ch.basic_ack(m.delivery_tag)
        return
    try:
        estado = executar(ev)
    except Exception as e:
        if retries + 1 < MAX_RETRIES:
            log.warn(f"falha ({e}); tentativa {retries + 1}/{MAX_RETRIES}, reenviando", cid)
            time.sleep(retries + 1)
            ch.basic_publish("", broker.Q_EXEC, body, pika.BasicProperties(
                content_type="application/json", delivery_mode=2, correlation_id=cid,
                headers={"x-retries": retries + 1}))
            ch.basic_ack(m.delivery_tag)
        else:
            log.error(f"falha definitiva ({e}); enviando para DLQ", cid)
            broker.publish(ch, "comando.falhou",
                           resultado(ev, "comando.falhou", "FALHA", str(e)), cid)
            ch.basic_reject(m.delivery_tag, requeue=False)
        return

    entry = cache.set(ev["dispositivo_id"], estado, INSTANCE_ID)
    node.broadcast_state(ev["dispositivo_id"], entry)  # P2P
    db.execute("INSERT INTO processados VALUES (?)", (cmd_id,))
    db.commit()
    broker.publish(ch, "comando.executado", resultado(
        ev, "comando.executado", "EXECUTADA",
        f"{ev['dispositivo_id']} -> {estado} (por {INSTANCE_ID})"), cid)
    log.info(f"comando {cmd_id} executado: {ev['dispositivo_id']} -> {estado}", cid)
    ch.basic_ack(m.delivery_tag)  # ack manual somente apos concluir (at-least-once)


if __name__ == "__main__":
    node.start()
    start_http(node, cache, 8000)
    while True:
        try:
            conn = broker.connect(log)
            ch = conn.channel()
            broker.declare_topology(ch)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(broker.Q_EXEC, on_message)
            log.info("aguardando comandos em q.executor")
            ch.start_consuming()
        except Exception as e:
            log.error(f"consumer: {e}; reconectando")
            time.sleep(3)
