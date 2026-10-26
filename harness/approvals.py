"""Confirmacao autenticada (spec 6.3): ferramenta sensivel espera a decisao do usuario.

Fluxo: broker -> request() cria a aprovacao no backend (POST /internal/approvals), emite o
evento SSE `approval_required` e BLOQUEIA ate a decisao (vinda de POST /v1/approvals/{id},
repassada pelo backend com a sessao autenticada) ou ate expirar. So depois a ferramenta roda.
Enquanto espera, os timeouts da run ficam pausados (RunContext.pause).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from harness.backend import Backend
from harness.config import Settings
from harness.events import EventBus
from harness.runctx import ApprovalWait, RunContext, RunContexts
from harness.store import Store

log = logging.getLogger("harness.approvals")

SCHEDULE_PREAPPROVED = frozenset({"email_send"})


class ApprovalService:
    def __init__(self, settings: Settings, store: Store, backend: Backend, bus: EventBus,
                 contexts: RunContexts):
        self.s = settings
        self.store = store
        self.backend = backend
        self.bus = bus
        self.contexts = contexts
        self._waiters: dict[str, RunContext] = {}

    async def request(self, ctx: RunContext, *, kind: str, summary: str, risk: str,
                      details: dict | None = None, force: bool = False) -> str:
        """-> 'approved' | 'denied' | 'expired' | 'external_denied'.

        Padrao: o agente tem liberdade total e a confirmacao e feita na CONVERSA (skills). O cartao
        de confirmacao autenticado so existe quando o template liga `limits.confirm_actions`.
        """
        # `confirm_actions`: true = todo tipo de acao; lista = so esses tipos; `force` = o agente
        # pediu (rt_confirm) porque a regra do template mandou, valha o manifesto ou nao.
        rule = ctx.limits.get("confirm_actions")
        # sem regra ligada no template, `rt_confirm` NAO abre cartao: a confirmacao e na conversa.
        # (Antes, o modelo chamava rt_confirm por habito e, em canal externo, o atendimento nunca
        # conseguia gerar Pix: o cliente nao pode aprovar cartao.)
        applies = rule is True or (isinstance(rule, list) and (force or kind in rule))
        if not applies:
            log.info("acao sem cartao (confirm_actions desligado) run=%s kind=%s", ctx.run_id, kind)
            return "approved"
        if ctx.channel == "schedule":
            # rotina agendada: ninguem esta na tela. So o que o dono autorizou ao criar a rotina
            # (envio de e-mail) segue; o resto (dinheiro, escrita, publicacao) e negado.
            return "approved" if kind in SCHEDULE_PREAPPROVED else "external_denied"
        if (ctx.actor or {}).get("type") == "external" and \
                ctx.limits.get("external_approval") != "human":
            return "external_denied"
        timeout = self.s.approval_timeout_s
        expires = datetime.now(UTC) + timedelta(seconds=timeout)
        approval_id = await self.backend.create_approval(ctx.run_id, ctx.run_token, {
            "run_id": ctx.run_id, "tenant_id": ctx.tenant_id, "user_id": ctx.user_id,
            "chat_id": ctx.chat_id, "summary": summary, "risk": risk, "kind": kind,
            "details": ctx.redactor.scrub_obj(details or {}),
            "expires_at": expires.isoformat()})
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        ctx.approvals[approval_id] = ApprovalWait(approval_id, fut, kind, summary)
        self._waiters[approval_id] = ctx
        await self.store.save_approval(approval_id, {
            "run_id": ctx.run_id, "tenant_id": ctx.tenant_id, "chat_id": ctx.chat_id,
            "kind": kind, "status": "pending", "expires_at": expires.isoformat()})
        ctx.pause()
        await self.store.set_run(ctx.run_id, status="waiting_approval")
        await self.bus.publish(ctx.run_id, "approval_required", {
            "approval_id": approval_id, "summary": summary, "risk": risk,
            "expires_at": expires.isoformat(), "actions": ["approve", "deny"]}, ctx)
        try:
            return await asyncio.wait_for(fut, timeout)
        except TimeoutError:
            if await self.store.decide_approval(approval_id, "expired"):
                return "expired"
            return "approved" if fut.done() and fut.result() == "approved" else "denied"
        except asyncio.CancelledError:
            await self.store.decide_approval(approval_id, "expired")
            raise
        finally:
            # quem nao decidiu ate aqui perdeu a vez (fim da run, cancelamento): clique tardio
            # nao reabre nada
            try:
                await self.store.decide_approval(approval_id, "expired")
            except Exception:  # noqa: BLE001
                pass
            ctx.resume()
            ctx.approvals.pop(approval_id, None)
            self._waiters.pop(approval_id, None)
            if not ctx.forgotten:
                await self.store.set_run(ctx.run_id, status="running")

    async def decide(self, approval_id: str, decision: str, *, tenant_id: str | None,
                     user_id: str | None) -> tuple[int, dict]:
        """Resolve a espera. Devolve (http_status, corpo)."""
        rec = await self.store.get_approval(approval_id)
        if rec is None:
            return 404, {"detail": "Aprovação não encontrada.", "code": "not_found"}
        if tenant_id and rec.get("tenant_id") != tenant_id:
            return 404, {"detail": "Aprovação não encontrada.", "code": "not_found"}
        ctx = self._waiters.get(approval_id)
        if ctx is not None and user_id and ctx.user_id and user_id != ctx.user_id:
            return 403, {"detail": "Apenas o dono da conversa pode decidir.", "code": "forbidden"}
        status = "approved" if decision == "approve" else "denied"
        if not await self.store.decide_approval(approval_id, status):
            cur = (await self.store.get_approval(approval_id)) or {}
            return 200, {"status": cur.get("status") if cur.get("status") in ("expired",)
                         else "already_decided"}
        if ctx is not None:
            w = ctx.approvals.get(approval_id)
            if w and not w.future.done():
                w.future.set_result(status)
        return 200, {"status": status}
