"""Vigia da fila de IA (Spec 14 §3 e §4): enquanto o recurso computacional (GPU sob demanda) esta ligando, a run nao deve
cair por silencio nem por tempo de turno, e o usuario precisa ver que a espera nao e erro.

So age depois de `after_s` sem evento do modelo; com a GPU quente nunca consulta nada. Quando o estado da fila e de partida,
pausa os relogios da run (como a espera de aprovacao) e devolve um texto para o evento `thinking`. A pausa tem teto (`cap_s`)
para uma fila travada nao segurar a run para sempre."""
from __future__ import annotations

import logging
import time

import httpx

log = logging.getLogger("harness.queuewatch")

_BOOTING = ("STARTING", "FALLBACK", "LOADING")
_ETAPA = {"STARTING": "preparando a partida", "FALLBACK": "alugando e preparando a máquina", "LOADING": "carregando o modelo"}


def _minutos(eta_s) -> str:
    m = max(1, round((eta_s or 60) / 60))
    return "1 minuto" if m == 1 else f"{m} minutos"


def texto(st: dict, primeiro: bool) -> str:
    estado = st.get("state")
    if primeiro:
        quando = "de 2 a 4 minutos" if st.get("path") == "restart" else "de 8 a 18 minutos"
        return f"O computador da IA estava desligado e está sendo ligado agora. Isso leva {quando} e não é erro: sua pergunta está na fila e será respondida automaticamente."
    return f"Ainda ligando o computador da IA ({_ETAPA.get(estado, 'ligando')}). Faltam cerca de {_minutos(st.get('eta_s'))}."


class QueueWatch:
    def __init__(self, url: str, key: str, http: httpx.AsyncClient, after_s: float = 15.0, poll_s: float = 10.0, cap_s: float = 1700.0):
        self.url, self.key, self.http = url, key, http
        self.after_s, self.poll_s, self.cap_s = after_s, poll_s, cap_s

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    async def _status(self) -> dict | None:
        try:
            r = await self.http.get(self.url, headers={"Authorization": f"Bearer {self.key}"} if self.key else {}, timeout=5.0)
            return r.json() if r.status_code == 200 else None
        except Exception as e:  # noqa: BLE001
            log.info("status da fila indisponivel: %r", e)
            return None

    async def tick(self, ctx, state: dict) -> str | None:
        """Chamado quando o modelo ficou `after_s` sem emitir evento. Devolve o texto do aviso (ou None).
        `state` guarda (por run) o instante da pausa, o ultimo texto e quantas vezes ja avisou."""
        now = time.monotonic()
        if now < state.get("next_poll", 0.0):  # pausada, o laco acorda a cada 0,5 s: consulta so a cada poll_s
            return None
        state["next_poll"] = now + self.poll_s
        st = await self._status()
        now = time.monotonic()
        booting = bool(st) and st.get("state") in _BOOTING and not st.get("fail")
        gasto = state.get("paused_for", 0.0) + ((now - state["pause_at"]) if state.get("pause_at") is not None else 0.0)
        if booting and gasto < self.cap_s:
            if not ctx.is_paused:
                ctx.pause()
                state["pause_at"] = now
            txt = texto(st, primeiro=not state.get("told"))
            state["told"] = True
            if txt == state.get("last"):
                return None
            state["last"] = txt
            return txt
        # nao esta ligando (pronta, falha, sem dados ou teto atingido): solta os relogios
        await self.release(ctx, state)
        if st and st.get("state") == "READY" and state.get("told") and not state.get("ready_told"):
            state["ready_told"] = True
            return "Computador da IA pronto. Respondendo agora."
        return None

    async def release(self, ctx, state: dict) -> None:
        if ctx.is_paused and state.get("pause_at") is not None:
            state["paused_for"] = state.get("paused_for", 0.0) + (time.monotonic() - state["pause_at"])
            state["pause_at"] = None
            ctx.resume()
