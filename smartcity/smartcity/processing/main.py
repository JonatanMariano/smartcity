"""Servico de Comandos (processamento): gRPC server, banco proprio, outbox -> RabbitMQ."""
import json
import os
import sqlite3
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, timezone

import grpc

import smartcity_pb2 as pb
import smartcity_pb2_grpc as pbg
from common import broker
from common.logs import Log

log = Log("processing")
DB_PATH = os.environ.get("DB_PATH", "/data/processing.db")
GRPC_PORT = os.environ.get("GRPC_PORT", "50051")
lock = threading.Lock()
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row


def now():
    return datetime.now(timezone.utc).isoformat()


def init_db():
    with lock:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS comandos(
            id TEXT PRIMARY KEY, correlation_id TEXT, residencia_id TEXT,
            dispositivo_id TEXT, tipo_dispositivo TEXT, acao TEXT, valor TEXT,
            status TEXT, detalhe TEXT, criado_em TEXT, atualizado_em TEXT);
        CREATE TABLE IF NOT EXISTS outbox(
            id INTEGER PRIMARY KEY AUTOINCREMENT, routing_key TEXT, payload TEXT,
            correlation_id TEXT, publicado INTEGER DEFAULT 0);
        """)
        db.commit()


def to_response(r):
    return pb.ComandoResponse(
        comando_id=r["id"], status=r["status"], residencia_id=r["residencia_id"],
        dispositivo_id=r["dispositivo_id"], tipo_dispositivo=r["tipo_dispositivo"],
        acao=r["acao"], valor=r["valor"] or "", detalhe=r["detalhe"] or "",
        criado_em=r["criado_em"], atualizado_em=r["atualizado_em"],
        correlation_id=r["correlation_id"])


class ComandoServicer(pbg.ComandoServiceServicer):
    def CriarComando(self, req, ctx):
        cid = req.correlation_id or str(uuid.uuid4())
        cmd_id, ts = str(uuid.uuid4()), now()
        evento = {
            "evento_id": str(uuid.uuid4()), "tipo": "comando.criado", "comando_id": cmd_id,
            "correlation_id": cid, "residencia_id": req.residencia_id,
            "dispositivo_id": req.dispositivo_id, "tipo_dispositivo": req.tipo_dispositivo,
            "acao": req.acao, "valor": req.valor, "status": "RECEBIDA",
            "detalhe": "", "ocorrido_em": ts,
        }
        with lock:  # estado + evento na mesma transacao (padrao Outbox)
            db.execute("INSERT INTO comandos VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                       (cmd_id, cid, req.residencia_id, req.dispositivo_id,
                        req.tipo_dispositivo, req.acao, req.valor, "RECEBIDA", "", ts, ts))
            db.execute("INSERT INTO outbox(routing_key,payload,correlation_id) VALUES (?,?,?)",
                       ("comando.criado", json.dumps(evento), cid))
            db.commit()
            row = db.execute("SELECT * FROM comandos WHERE id=?", (cmd_id,)).fetchone()
        log.info(f"comando {cmd_id} criado ({req.acao} {req.dispositivo_id})", cid)
        return to_response(row)

    def ConsultarComando(self, req, ctx):
        with lock:
            row = db.execute("SELECT * FROM comandos WHERE id=?", (req.comando_id,)).fetchone()
        if not row:
            ctx.abort(grpc.StatusCode.NOT_FOUND, "comando nao encontrado")
        return to_response(row)


def publisher_loop():
    """Publica eventos pendentes da outbox com confirmacao do broker (at-least-once)."""
    while True:
        try:
            conn = broker.connect(log)
            ch = conn.channel()
            broker.declare_topology(ch)
            ch.confirm_delivery()
            log.info("publisher conectado ao broker")
            while True:
                with lock:
                    rows = db.execute(
                        "SELECT * FROM outbox WHERE publicado=0 ORDER BY id LIMIT 50").fetchall()
                for r in rows:
                    broker.publish(ch, r["routing_key"], json.loads(r["payload"]),
                                   r["correlation_id"])
                    with lock:
                        db.execute("UPDATE outbox SET publicado=1 WHERE id=?", (r["id"],))
                        db.commit()
                    log.info(f"evento {r['routing_key']} publicado", r["correlation_id"])
                conn.sleep(0.5)
        except Exception as e:
            log.error(f"publisher: {e}; reconectando")
            time.sleep(3)


def status_loop():
    """Consome resultados do executor e atualiza o status (consistencia eventual)."""
    while True:
        try:
            conn = broker.connect(log)
            ch = conn.channel()
            broker.declare_topology(ch)
            ch.basic_qos(prefetch_count=10)

            def cb(ch, m, props, body):
                try:
                    ev = json.loads(body)
                    cmd_id, status, cid = ev["comando_id"], ev["status"], ev["correlation_id"]
                except Exception:
                    log.error("mensagem de status malformada -> DLQ")
                    ch.basic_reject(m.delivery_tag, requeue=False)
                    return
                with lock:
                    db.execute("UPDATE comandos SET status=?, detalhe=?, atualizado_em=? WHERE id=?",
                               (status, ev.get("detalhe", ""), now(), cmd_id))
                    db.commit()
                log.info(f"comando {cmd_id} -> {status}", cid)
                ch.basic_ack(m.delivery_tag)

            ch.basic_consume(broker.Q_STATUS, cb)
            ch.start_consuming()
        except Exception as e:
            log.error(f"status consumer: {e}; reconectando")
            time.sleep(3)


if __name__ == "__main__":
    init_db()
    threading.Thread(target=publisher_loop, daemon=True).start()
    threading.Thread(target=status_loop, daemon=True).start()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pbg.add_ComandoServiceServicer_to_server(ComandoServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    log.info(f"gRPC ouvindo na porta {GRPC_PORT}")
    server.wait_for_termination()
