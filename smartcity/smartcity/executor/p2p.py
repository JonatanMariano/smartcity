"""Cooperacao Peer-to-Peer entre instancias do executor (sockets TCP brutos).

- Descoberta de pares: HELLO periodico (heartbeat) para os enderecos em PEERS.
- Replicacao de cache: STATE enviado direto de par para par a cada mudanca.
- Sincronizacao total: o HELLO carrega o snapshot; vale a versao mais recente (LWW).
- Eleicao simplificada de lider: maior ID entre as instancias vivas (Bully deterministico).
"""
import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PEER_TIMEOUT = 10  # segundos sem heartbeat => par considerado morto


class StateCache:
    def __init__(self):
        self._d, self._l = {}, threading.Lock()

    def set(self, dev, estado, origem):
        e = {"estado": estado, "versao": time.time(), "atualizado_por": origem}
        with self._l:
            self._d[dev] = e
        return e

    def merge(self, snap):
        n = 0
        with self._l:
            for dev, e in snap.items():
                cur = self._d.get(dev)
                if cur is None or e["versao"] > cur["versao"]:
                    self._d[dev] = e
                    n += 1
        return n

    def snapshot(self):
        with self._l:
            return dict(self._d)


class PeerNode:
    def __init__(self, instance_id, port, peers, cache, log):
        self.id, self.port, self.cache, self.log = instance_id, port, cache, log
        self.peers = [p.strip() for p in peers if p.strip()]
        self.alive, self.leader = {}, instance_id
        self._l = threading.Lock()

    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()
        threading.Thread(target=self._heartbeat, daemon=True).start()

    # ---- servidor ----
    def _serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.port))
        srv.listen(16)
        self.log.info(f"P2P ouvindo na porta {self.port}")
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            conn.settimeout(3)
            msg = json.loads(conn.makefile("rb").readline())
            if msg["tipo"] == "HELLO":
                self._touch(msg["de"])
                self.cache.merge(msg.get("cache", {}))
                reply = {"tipo": "HELLO_ACK", "de": self.id, "cache": self.cache.snapshot()}
            elif msg["tipo"] == "STATE":
                self.cache.merge({msg["dispositivo"]: msg["entrada"]})
                self.log.info(f"cache replicado por {msg['de']}: {msg['dispositivo']}"
                              f" = {msg['entrada']['estado']}")
                reply = {"tipo": "ACK", "de": self.id}
            else:
                reply = {"tipo": "ERRO"}
            conn.sendall((json.dumps(reply) + "\n").encode())
        except Exception:
            pass
        finally:
            conn.close()

    # ---- cliente ----
    def _rpc(self, addr, msg):
        host, port = addr.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=2) as s:
            s.sendall((json.dumps(msg) + "\n").encode())
            return json.loads(s.makefile("rb").readline())

    def _touch(self, pid):
        with self._l:
            novo = pid not in self.alive
            self.alive[pid] = time.time()
        if novo:
            self.log.info(f"par descoberto: {pid}")

    def _heartbeat(self):
        while True:
            for addr in self.peers:
                try:
                    r = self._rpc(addr, {"tipo": "HELLO", "de": self.id,
                                         "cache": self.cache.snapshot()})
                    self._touch(r["de"])
                    if self.cache.merge(r.get("cache", {})):
                        self.log.info(f"cache sincronizado com {r['de']}")
                except Exception:
                    pass
            self._reelect()
            time.sleep(3)

    def _reelect(self):
        with self._l:
            t = time.time()
            for pid in [p for p, ts in self.alive.items() if t - ts > PEER_TIMEOUT]:
                del self.alive[pid]
                self.log.warn(f"par perdido: {pid}")
            novo = max([self.id, *self.alive])
            mudou = novo != self.leader
            self.leader = novo
        if mudou:
            self.log.info(f"lider eleito: {novo}")

    def broadcast_state(self, dev, entry):
        def run():
            for addr in self.peers:
                try:
                    self._rpc(addr, {"tipo": "STATE", "de": self.id,
                                     "dispositivo": dev, "entrada": entry})
                except Exception:
                    self.log.warn(f"par {addr} inacessivel; sera sincronizado no heartbeat")
        threading.Thread(target=run, daemon=True).start()

    def info(self):
        with self._l:
            return {"instancia": self.id, "lider": self.leader, "pares_vivos": list(self.alive)}


def start_http(node, cache, port=8000):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({**node.info(), "dispositivos": cache.snapshot()},
                              ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever,
                     daemon=True).start()
