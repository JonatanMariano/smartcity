"""Servico de Notificacoes: consumidor pub/sub (comando.*), banco proprio, API de leitura."""
import json
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from common import broker
from common.logs import Log

log = Log("notification")
DB_PATH = os.environ.get("DB_PATH", "/data/notification.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
lock = threading.Lock()
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute("""CREATE TABLE IF NOT EXISTS notificacoes(
    id INTEGER PRIMARY KEY AUTOINCREMENT, comando_id TEXT, evento TEXT,
    mensagem TEXT, correlation_id TEXT, criado_em TEXT)""")
db.commit()

MSG = {
    "comando.criado": "Seu comando foi recebido e está na fila de execução.",
    "comando.executado": "Seu comando foi executado com sucesso.",
    "comando.falhou": "Não foi possível executar seu comando.",
}


def consume():
    while True:
        try:
            conn = broker.connect(log)
            ch = conn.channel()
            broker.declare_topology(ch)
            ch.basic_qos(prefetch_count=10)

            def cb(ch, m, props, body):
                try:
                    ev = json.loads(body)
                    cmd_id, cid = ev["comando_id"], ev["correlation_id"]
                except Exception:
                    log.error("mensagem malformada -> DLQ")
                    ch.basic_reject(m.delivery_tag, requeue=False)
                    return
                texto = MSG.get(m.routing_key, m.routing_key)
                if ev.get("detalhe"):
                    texto += f" ({ev['detalhe']})"
                with lock:
                    db.execute("INSERT INTO notificacoes(comando_id,evento,mensagem,correlation_id,criado_em)"
                               " VALUES (?,?,?,?,?)",
                               (cmd_id, m.routing_key, texto, cid,
                                datetime.now(timezone.utc).isoformat()))
                    db.commit()
                log.info(f"notificacao [{m.routing_key}] para comando {cmd_id}", cid)
                ch.basic_ack(m.delivery_tag)

            ch.basic_consume(broker.Q_NOTIF, cb)
            ch.start_consuming()
        except Exception as e:
            log.error(f"consumer: {e}; reconectando")
            time.sleep(3)


@asynccontextmanager
async def lifespan(app):
    threading.Thread(target=consume, daemon=True).start()
    yield


app = FastAPI(title="Notificações", lifespan=lifespan)


@app.get("/notificacoes")
def listar(comando_id: str):
    with lock:
        rows = db.execute("SELECT * FROM notificacoes WHERE comando_id=? ORDER BY id",
                          (comando_id,)).fetchall()
    return [dict(r) for r in rows]
