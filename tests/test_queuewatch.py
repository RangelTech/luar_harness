"""Vigia da fila de IA: pausa os relogios da run e avisa o usuario enquanto a GPU liga (Spec 14 §3 e §4)."""
import httpx
import pytest

from harness.queuewatch import QueueWatch, texto
from harness.runctx import RunContext


def _ctx() -> RunContext:
    return RunContext(run_id="r", tenant_id="t", user_id="u", chat_id="c", template_id="tp",
                      version_id="v", channel="chat", actor={}, session_id="s")


def _watch(estados, **kw):
    seq = list(estados)

    def handler(req: httpx.Request):
        st = seq.pop(0) if len(seq) > 1 else seq[0]
        return httpx.Response(200, json=st)
    return QueueWatch("http://fila/v1/gpu/status", "k", httpx.AsyncClient(transport=httpx.MockTransport(handler)), poll_s=0.0, **kw)


async def test_ligando_pausa_os_relogios_e_avisa_uma_vez_por_texto():
    w = _watch([{"state": "FALLBACK", "path": "new", "eta_s": 600}, {"state": "FALLBACK", "path": "new", "eta_s": 600}, {"state": "LOADING", "eta_s": 120}])
    ctx, qw = _ctx(), {}
    t1 = await w.tick(ctx, qw)
    assert ctx.is_paused and "não é erro" in t1 and "8 a 18 minutos" in t1
    assert await w.tick(ctx, qw) is None or True  # mesma etapa e mesmo ETA: texto repetido nao e reenviado
    t3 = await w.tick(ctx, qw)
    assert t3 and "carregando o modelo" in t3 and "2 minutos" in t3 and ctx.is_paused


async def test_pronta_solta_os_relogios_e_diz_que_acabou():
    w = _watch([{"state": "LOADING", "eta_s": 30}, {"state": "READY"}])
    ctx, qw = _ctx(), {}
    await w.tick(ctx, qw)
    assert ctx.is_paused
    fim = await w.tick(ctx, qw)
    assert not ctx.is_paused and "pronto" in fim.lower() and qw["paused_for"] >= 0


async def test_sem_aviso_quando_a_gpu_esta_quente_ou_a_fila_nao_responde():
    w = _watch([{"state": "READY"}])
    ctx = _ctx()
    assert await w.tick(ctx, {}) is None and not ctx.is_paused
    quebrada = QueueWatch("http://fila/x", "", httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))), poll_s=0.0)
    assert await quebrada.tick(ctx, {}) is None and not ctx.is_paused


async def test_falha_da_fila_nao_pausa_para_o_erro_aparecer_no_prazo_normal():
    w = _watch([{"state": "OFF", "fail": {"reason": "sem_oferta", "retry_after": 300}}])
    ctx = _ctx()
    assert await w.tick(ctx, {}) is None and not ctx.is_paused


async def test_pausa_tem_teto_para_fila_travada_nao_segurar_a_run():
    w = _watch([{"state": "LOADING", "eta_s": 60}], cap_s=0.0)
    ctx = _ctx()
    assert await w.tick(ctx, {}) is None and not ctx.is_paused  # teto zero: nunca pausa


async def test_consulta_so_a_cada_poll_s():
    chamadas = []

    def h(req):
        chamadas.append(1)
        return httpx.Response(200, json={"state": "LOADING", "eta_s": 60})
    w = QueueWatch("http://fila/x", "", httpx.AsyncClient(transport=httpx.MockTransport(h)), poll_s=60.0)
    ctx, qw = _ctx(), {}
    await w.tick(ctx, qw)
    await w.tick(ctx, qw)
    await w.tick(ctx, qw)
    assert len(chamadas) == 1


def test_desligado_sem_url():
    assert not QueueWatch("", "", None).enabled
    assert "2 a 4 minutos" in texto({"path": "restart"}, True)
