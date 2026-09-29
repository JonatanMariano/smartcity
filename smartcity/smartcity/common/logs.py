"""Log estruturado (JSON) com timestamp e correlation_id - RF09 / RNF05."""
import json
import logging
import sys
from datetime import datetime, timezone


class _Fmt(logging.Formatter):
    def __init__(self, servico):
        super().__init__()
        self.servico = servico

    def format(self, r):
        return json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "servico": self.servico,
            "nivel": r.levelname,
            "correlation_id": getattr(r, "correlation_id", "-"),
            "mensagem": r.getMessage(),
        }, ensure_ascii=False)


class Log:
    def __init__(self, servico):
        self._l = logging.getLogger(servico)
        self._l.setLevel(logging.INFO)
        self._l.propagate = False
        if not self._l.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setFormatter(_Fmt(servico))
            self._l.addHandler(h)

    def info(self, msg, cid="-"):
        self._l.info(msg, extra={"correlation_id": cid})

    def warn(self, msg, cid="-"):
        self._l.warning(msg, extra={"correlation_id": cid})

    def error(self, msg, cid="-"):
        self._l.error(msg, extra={"correlation_id": cid})
