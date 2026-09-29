"""Topologia RabbitMQ compartilhada (exchange topic + filas + DLQ)."""
import json
import os
import time

import pika

EXCHANGE = "smartcity.events"
DLX = "smartcity.dlx"
Q_EXEC = "q.executor"            # fila ponto-a-ponto (competing consumers)
Q_STATUS = "q.status"            # retorno de status para o servico de comandos
Q_NOTIF = "q.notificacoes"       # consumidor de notificacoes
Q_DLQ = "q.dlq"                  # fila de mensagens mortas


def connect(log):
    url = os.environ["RABBITMQ_URL"]  # descoberta por configuracao (RNF03)
    while True:
        try:
            return pika.BlockingConnection(pika.URLParameters(url))
        except Exception as e:
            log.warn(f"RabbitMQ indisponivel, tentando novamente: {e}")
            time.sleep(3)


def declare_topology(ch):
    ch.exchange_declare(EXCHANGE, "topic", durable=True)
    ch.exchange_declare(DLX, "fanout", durable=True)
    ch.queue_declare(Q_DLQ, durable=True)
    ch.queue_bind(Q_DLQ, DLX)
    dl = {"x-dead-letter-exchange": DLX}
    ch.queue_declare(Q_EXEC, durable=True, arguments=dl)
    ch.queue_bind(Q_EXEC, EXCHANGE, "comando.criado")
    ch.queue_declare(Q_STATUS, durable=True, arguments=dl)
    ch.queue_bind(Q_STATUS, EXCHANGE, "comando.executado")
    ch.queue_bind(Q_STATUS, EXCHANGE, "comando.falhou")
    ch.queue_declare(Q_NOTIF, durable=True, arguments=dl)
    ch.queue_bind(Q_NOTIF, EXCHANGE, "comando.*")


def publish(ch, routing_key, payload, cid):
    ch.basic_publish(
        EXCHANGE, routing_key, json.dumps(payload).encode(),
        pika.BasicProperties(content_type="application/json", delivery_mode=2,
                             correlation_id=cid, message_id=payload.get("evento_id"),
                             headers={"x-retries": 0}),
    )
