"""Familia B (broker e credenciais, spec 14.2 B1-B16), offline."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import sqlite3
from decimal import Decimal

import pytest

from broker.data import guard_select
from broker.errors import ToolError
from broker.http_tool import host_allowed, ip_forbidden
from broker.pix import brl, idempotency_key, parse_amount
from harness.runctx import session_id_for
from harness.security import Redactor, collect_secret_values
from tests.conftest import (
    BROKER_TOKEN,
    S_APIKEY,
    S_DB,
    S_MAIL,
    S_MP,
    SENTINELS,
    final,
    types,
)


def tool(name: str, args: dict | None = None) -> str:
    return f"[[tool:{name} {json.dumps(args or {})}]]"


def tool_results(events) -> list[dict]:
    """Resultado de cada ferramenta como o agente o recebeu (preview no tool.completed)."""
    return [e for e in events if e.type == "step" and e.data["status"] in ("done", "error")]


async def last_result(rig, message, *, approve=None, **body_kw) -> dict:
    """Roda 1 chamada de ferramenta e devolve o JSON do broker via diretiva [[echo]]."""
    ev = await rig.send(rig.body(message=message + "[[echo]]", **body_kw),
                        on_event=rig.approver(approve) if approve else None)
    assert final(ev).type == "done", [(e.type, e.data) for e in ev]
    return json.loads(final(ev).data["text"])


# =================================================================== B1 / B5
def dump_redis(rig) -> str:
    srv = rig.redis_server
    out = []
    for db in srv.dbs.values():
        for k in list(db.keys()):
            item = db.get(k)
            out.append(f"{k!r}={item.value!r}" if item is not None else str(k))
    return "\n".join(out)


def dump_tree(root) -> str:
    chunks = []
    for p in root.rglob("*"):
        if p.is_file() and p.stat().st_size < 5_000_000:
            try:
                chunks.append(p.read_bytes().decode("utf-8", "replace"))
            except OSError:
                pass
    return "\n".join(chunks)


async def test_b1_b5_credentials_only_in_memory_and_never_leak(rig, caplog):
    caplog.set_level(logging.DEBUG)
    msg = ("[[say:vou consultar]]"
           + tool("rt_sql_read", {"datasource": "vendas", "query": "select * from clientes"})
           + tool("rt_http", {"method": "GET", "url": "https://api.partner.test/v1/x",
                              "headers": {"Authorization": "Bearer {{secret:API_KEY}}"}})
           + tool("rt_pix_create", {"amount": "10.50", "reference": "leak-1"})
           + tool("rt_email_send", {"to": ["a@b.test"], "subject": "oi", "body": "corpo"})
           + "[[proposal:u-leakcheck|---\\nname: u-leakcheck\\ndescription: d\\n---\\ncorpo]]"
           + "[[memproposal:lembrar algo]][[say: fim]]")
    ev = await rig.send(rig.body(run_id="leak1", message=msg), on_event=rig.approver())
    assert final(ev).type == "done", [(e.type, e.data) for e in ev]

    # nada em Redis / disco / SSE / logs / backend / entradas do Hermes
    haystacks = {
        "redis": dump_redis(rig),
        "disco": dump_tree(rig.tmp.joinpath("profiles")) + dump_tree(rig.tmp / "users")
        + dump_tree(rig.tmp / "tenant_skills"),
        "sse": json.dumps([e.data for e in ev]),
        "logs": caplog.text,
        "backend": json.dumps({"u": rig.backend.usage, "m": rig.backend.messages,
                               "a": rig.backend.approvals, "art": rig.backend.artifacts,
                               "s": rig.backend.skill_proposals, "c": rig.backend.charges,
                               "mem": rig.backend.memory_proposals}, default=str),
        "hermes": json.dumps([r["body"] for r in rig.hermes[0].requests]),
    }
    for where, blob in haystacks.items():
        for sentinel in SENTINELS:
            assert sentinel not in blob, f"{sentinel} vazou em {where}"
    # o pedido guardado no Redis nao tem a chave `credentials`
    assert '"credentials"' not in haystacks["redis"]
    # as credenciais foram usadas de verdade (MP recebeu o token; parceiro recebeu o segredo)
    assert any(S_MP in r.headers.get("authorization", "") for r in rig.ext.mp_requests())
    assert rig.ext.partner_calls[0]["headers"]["authorization"] == f"Bearer {S_APIKEY}"
    # e foram esquecidas ao fim
    assert rig.kernel.contexts.active() == 0
    assert len(rig.kernel.pending) == 0


async def test_b1_context_is_wiped_after_run_even_on_error_and_cancel(rig):
    t = rig.spawn(rig.body(run_id="wipe1", message="[[sleep:30]]"))
    await asyncio.sleep(0.6)
    ctx = rig.kernel.contexts.by_run("wipe1")
    assert ctx is not None and ctx.credentials
    held = ctx.credentials
    await rig.client.post("/v1/runs/wipe1/cancel", json={},
                          headers={"Authorization": "Bearer svc-token-test-0001"})
    await t
    assert ctx.forgotten and ctx.credentials == {} and not held
    assert rig.kernel.contexts.by_session(ctx.session_id) is None

    ev = await rig.send(rig.body(run_id="wipe2", message="[[fail]]"))
    assert final(ev).data["code"] == "failed"
    assert rig.kernel.contexts.active() == 0


async def test_b5_secret_echoed_by_partner_is_redacted(rig):
    res = await last_result(rig, tool("rt_http", {
        "method": "GET", "url": "https://api.partner.test/eco",
        "headers": {"Authorization": "Bearer {{secret:API_KEY}}"}}))
    assert res["ok"] is True
    assert S_APIKEY not in json.dumps(res) and "[REDACTED]" in res["result"]["body"]


# ======================================================================== B2
async def test_b2_broker_without_active_run_is_401(rig):
    sid = session_id_for("t1", "u1", "nada")
    status, body = await rig.broker(sid, "rt_echo", {"text": "x"})
    assert (status, body["code"]) == (401, "no_active_run")
    status, body = await rig.broker(sid, "rt_echo", {"text": "x"}, token="token-invalido")
    assert (status, body["code"]) == (401, "unauthorized")
    r = await rig.client.post("/broker/v1/call", json={"session_id": sid, "tool": "rt_echo",
                                                       "args": {}})
    assert r.status_code == 401


async def test_b2_session_of_a_finished_run_is_dead(rig):
    ev = await rig.send(rig.body(run_id="dead1", chat="cd", message="oi"))
    assert final(ev).type == "done"
    status, body = await rig.broker(session_id_for("t1", "u1", "cd"), "rt_echo", {"text": "x"})
    assert status == 401 and body["code"] == "no_active_run"


# ======================================================================== B3
async def test_b3_foreign_worker_token_cannot_use_other_workers_session(rig2):
    rig = rig2
    reg = rig.kernel.workers
    reg.workers["w1"].spec.broker_token = "tok-w1"
    reg.workers["w2"].spec.broker_token = "tok-w2"
    for h, tok in zip(rig.hermes, ("tok-w1", "tok-w2"), strict=True):
        h.broker_token = tok
    t = rig.spawn(rig.body(run_id="fw1", message="[[sleep:3]]"))
    await asyncio.sleep(0.7)
    ctx = rig.kernel.contexts.by_run("fw1")
    mine, other = ("tok-w1", "tok-w2") if ctx.worker_id == "w1" else ("tok-w2", "tok-w1")
    status, body = await rig.broker(ctx.session_id, "rt_echo", {"text": "ok"}, token=other)
    assert (status, body["code"]) == (403, "foreign_session")
    status, body = await rig.broker(ctx.session_id, "rt_echo", {"text": "ok"}, token=mine)
    assert status == 200 and body["ok"] is True
    # token compartilhado antigo tambem e aceito (modo '*') mas o do pool errado nao
    await rig.client.post("/v1/runs/fw1/cancel", json={},
                          headers={"Authorization": "Bearer svc-token-test-0001"})
    await t


async def test_b3_other_tenants_session_id_is_not_reachable_by_guessing(rig):
    t = rig.spawn(rig.body(run_id="g1", tenant="tA", chat="c1", message="[[sleep:3]]"))
    await asyncio.sleep(0.6)
    # tenant B tenta o session_id do tenant A trocando pedacos: nao existe contexto
    for sid in (session_id_for("tB", "u1", "c1"), "tA:u1:outro", "tA:shared:c1", "tA"):
        status, body = await rig.broker(sid, "rt_echo", {"text": "x"})
        assert status == 401, sid
    await rig.client.post("/v1/runs/g1/cancel", json={},
                          headers={"Authorization": "Bearer svc-token-test-0001"})
    await t


# ======================================================================== B4
async def test_b4_tool_outside_manifest_is_denied(rig):
    b = rig.body(message=tool("rt_pix_create", {"amount": "5", "reference": "x"}) + "[[echo]]",
                 tools=["rt_echo"])
    ev = await rig.send(b)
    out = json.loads(final(ev).data["text"])
    assert out["error"] == "broker_http_403" and out["broker"]["code"] == "tool_not_allowed"
    assert not rig.ext.mp_requests()
    # e uma ferramenta inventada
    b = rig.body(message=tool("rt_inventada") + "[[echo]]")
    out = json.loads(final(await rig.send(b)).data["text"])
    assert out["broker"]["code"] == "tool_not_allowed"


async def test_b4_credential_grants_limit_what_template_can_use(rig):
    res = await last_result(rig, tool("rt_pix_create", {"amount": "5", "reference": "g1"}),
                            grants=["email"])
    assert res["ok"] is False and res["error"]["code"] == "forbidden_target"
    assert not rig.ext.mp_requests()


# ======================================================================== B6
def test_b6_amount_is_exact_decimal():
    assert parse_amount("10.50") == Decimal("10.50")
    assert parse_amount("10,5") == Decimal("10.50")
    assert parse_amount(25) == Decimal("25.00")
    assert parse_amount("R$ 3.07") == Decimal("3.07")
    for bad in ("0", "-1", "10.505", "abc", "", None, True, "NaN", "Infinity", "1e400"):
        with pytest.raises(ToolError):
            parse_amount(bad)
    # float binario impreciso e recusado, nunca arredondado em silencio
    with pytest.raises(ToolError):
        parse_amount(0.1 + 0.2)
    assert brl(Decimal("1234567.80")) == "R$ 1.234.567,80"
    k1 = idempotency_key("t", "c", "ref", Decimal("10.50"))
    assert k1 == idempotency_key("t", "c", "ref", Decimal("10.5"))
    assert k1 != idempotency_key("t", "c", "ref", Decimal("10.51"))
    assert k1 != idempotency_key("t", "c2", "ref", Decimal("10.50"))


async def test_b6_pix_requires_authenticated_confirmation_before_creating(rig):
    seen = {}

    async def watcher(ev):
        if ev.type == "approval_required":
            seen["payments_before"] = len(rig.ext.payments)
            seen["card"] = ev.data
            await rig.decide(ev.data["approval_id"], "approve")

    msg = tool("rt_pix_create", {"amount": "10.50", "reference": "ped-1",
                                 "description": "Pedido 1"}) + "[[echo]]"
    ev = await rig.send(rig.body(message=msg), on_event=watcher)
    res = json.loads(final(ev).data["text"])
    assert seen["payments_before"] == 0  # nada criado antes do clique
    assert seen["card"]["risk"] == "high" and "R$ 10,50" in seen["card"]["summary"]
    assert res["ok"] and res["result"]["status"] == "pending" and res["result"]["copy_paste"]
    assert len(rig.ext.payments) == 1
    sent = next(r for r in rig.ext.mp_requests() if r.method == "POST")
    body = json.loads(sent.content)
    assert body["transaction_amount"] == 10.5 and body["external_reference"] == "ped-1"
    assert sent.headers["x-idempotency-key"] == idempotency_key("t1", "c1", "ped-1",
                                                                Decimal("10.50"))
    # QR como artefato (evento + upload + registro)
    arts = [e for e in ev if e.type == "artifact"]
    assert len(arts) == 1 and arts[0].data["kind"] == "image"
    aid = arts[0].data["artifact_id"]
    assert rig.backend.files[aid].startswith(b"\x89PNG")
    assert rig.backend.artifacts[aid]["complete"]["title"].startswith("QR Code Pix")
    # cobranca registrada na plataforma
    assert rig.backend.charges["ped-1"]["payment_id"] == res["result"]["payment_id"]
    assert [e.type for e in ev].count("artifact") == 1  # sem duplicar broker x tradutor


async def test_b6_pix_denied_creates_nothing(rig):
    res = await last_result(rig, tool("rt_pix_create", {"amount": "10", "reference": "n1"}),
                            approve="deny")
    assert res["ok"] is False and res["error"]["code"] == "approval_denied"
    assert not rig.ext.mp_requests()


async def test_b6_pix_confirmation_expires(make_rig):
    rig = await make_rig(1, approval_timeout_s=0.8)
    res = await last_result(rig, tool("rt_pix_create", {"amount": "10", "reference": "e1"}))
    assert res["error"]["code"] == "approval_expired"
    assert not rig.ext.mp_requests()


async def test_b6_pix_idempotent_same_reference_and_amount(rig):
    msg = (tool("rt_pix_create", {"amount": "30", "reference": "dup"})
           + tool("rt_pix_create", {"amount": "30.00", "reference": "dup"}))
    ev = await rig.send(rig.body(run_id="px1", chat="cpx", message=msg + "[[echo]]"),
                        on_event=rig.approver())
    assert len(rig.ext.payments) == 1
    assert json.loads(final(ev).data["text"])["result"]["reused"] is True
    # outra run, mesmo chat: reaproveita a cobranca (lookup na plataforma), sem novo POST
    res = await last_result(rig, tool("rt_pix_create", {"amount": "30", "reference": "dup"}),
                            approve="approve", chat="cpx")
    assert res["result"]["reused"] is True and len(rig.ext.payments) == 1
    # mesma referencia com outro valor e recusada
    res = await last_result(rig, tool("rt_pix_create", {"amount": "31", "reference": "dup"}),
                            approve="approve", chat="cpx")
    assert res["error"]["code"] == "guardrail" and len(rig.ext.payments) == 1


async def test_b6_pix_ceiling_and_invalid_amounts(rig):
    res = await last_result(rig, tool("rt_pix_create", {"amount": "10000.01", "reference": "t"}))
    assert res["error"]["code"] == "guardrail"
    res = await last_result(rig, tool("rt_pix_create", {"amount": "60", "reference": "t2"}),
                            limits={"pix_max_amount": "50"})
    assert res["error"]["code"] == "guardrail"  # teto do template menor que o global
    res = await last_result(rig, tool("rt_pix_create", {"amount": "10.999", "reference": "t3"}))
    assert res["error"]["code"] == "invalid_args"
    res = await last_result(rig, tool("rt_pix_create", {"amount": "10", "reference": "bad ref!"}))
    assert res["error"]["code"] == "invalid_args"
    assert not rig.ext.mp_requests()


async def test_b6_payment_status_and_delivery_only_by_provider(rig):
    msg = tool("rt_pix_create", {"amount": "12", "reference": "st-1"})
    await rig.send(rig.body(chat="cst", message=msg), on_event=rig.approver())
    pid = next(iter(rig.ext.payments))
    res = await last_result(rig, tool("rt_payment_status", {"reference": "st-1"}), chat="cst")
    assert res["result"]["status"] == "pending"
    assert "confirmação oficial" in res["result"]["note"] or "aviso oficial" in res["result"]["note"]
    rig.ext.pay(pid, "approved")
    res = await last_result(rig, tool("rt_payment_status", {"payment_id": pid}), chat="cst")
    assert res["result"]["status"] == "paid"
    res = await last_result(rig, tool("rt_mp_read", {"limit": 5}), chat="cst")
    assert res["result"]["count"] == 1
    assert "payer" not in json.dumps(res)


async def test_b6_external_actor_cannot_confirm_money(rig):
    ev = await rig.send(rig.body(user=None, actor_type="external", channel="chatwoot",
                                 message=tool("rt_pix_create", {"amount": "5", "reference": "x1"})
                                 + "[[echo]]"))
    res = json.loads(final(ev).data["text"])
    assert res["error"]["code"] == "approval_denied"
    assert "approval_required" not in types(ev) and not rig.ext.mp_requests()


# ======================================================================== B7
async def test_b7_email_needs_approval_and_has_limits(rig):
    res = await last_result(rig, tool("rt_email_list", {"limit": 3}))
    assert res["ok"] and res["result"]["count"] == 1  # leitura sem aprovacao
    assert "dado" in res["result"]["note"]

    send = {"to": ["cli@x.test"], "subject": "Cobranca", "body": "Ola"}
    res = await last_result(rig, tool("rt_email_send", send), approve="deny")
    assert res["error"]["code"] == "approval_denied" and not rig.email.sent
    res = await last_result(rig, tool("rt_email_send", send), approve="approve")
    assert res["ok"] and rig.email.sent == [{"account": "Financeiro", "to": ["cli@x.test"],
                                             "subject": "Cobranca", "body": "Ola"}]
    rig.email.sent.clear()
    many = {"to": [f"p{i}@x.test" for i in range(6)], "subject": "s", "body": "b"}
    res = await last_result(rig, tool("rt_email_send", many), approve="approve")
    assert res["error"]["code"] == "guardrail" and not rig.email.sent
    res = await last_result(rig, tool("rt_email_send", {**send, "to": ["nao-e-email"]}))
    assert res["error"]["code"] == "invalid_args"
    res = await last_result(rig, tool("rt_email_send", {**send, "subject": "a\r\nBcc: x@y.z"}))
    assert res["error"]["code"] == "invalid_args"
    res = await last_result(rig, tool("rt_email_send", send), approve="approve",
                            limits={"max_recipients": 1}, tools=["rt_email_send"])
    assert res["ok"]


# ======================================================================== B9
def test_b9_guard_select_rules():
    assert guard_select("select 1;").startswith("select")
    assert guard_select("WITH a AS (select 1) select * from a")
    assert guard_select("select 'drop table x; --' as t")  # literal nao conta
    for bad in ("select 1; drop table clientes", "delete from clientes",
                "with x as (delete from clientes returning *) select * from x",
                "select * into novo from clientes", "update clientes set nome='x'",
                "pragma table_info(clientes)", "", "  "):
        with pytest.raises(ToolError):
            guard_select(bad)


async def test_b9_sql_read_makes_dataset_artifact_with_cache_warning(rig):
    q = {"datasource": "vendas", "query": "select id, nome, saldo from clientes order by id",
         "title": "Clientes"}
    ev = await rig.send(rig.body(message=tool("rt_sql_read", q) + tool("rt_sql_read", q)
                                 + "[[echo]]"))
    res = json.loads(final(ev).data["text"])
    assert res["ok"] and res["result"]["row_count"] == 3 and res["result"]["kind"] == "dataset"
    assert res["result"]["preview"][0] == [1, "Ana", 10.5]
    assert "AVISO" in res["result"]  # segunda chamada igual no mesmo turno
    assert [e.type for e in ev].count("artifact") == 1
    aid = next(e.data["artifact_id"] for e in ev if e.type == "artifact")
    stored = json.loads(rig.backend.files[aid])
    assert len(stored["rows"]) == 3 and stored["columns"][1]["name"] == "nome"


async def test_b9_sql_read_blocks_writes_and_respects_row_limit(rig):
    res = await last_result(rig, tool("rt_sql_read", {
        "datasource": "vendas", "query": "select 1; delete from clientes"}))
    assert res["error"]["code"] == "guardrail"
    res = await last_result(rig, tool("rt_sql_read", {
        "datasource": "vendas", "query": "select * from clientes", "limit": 2}))
    assert res["result"]["row_count"] == 2 and res["result"]["truncated_at_limit"] is True
    res = await last_result(rig, tool("rt_sql_read", {"datasource": "outra", "query": "select 1"}))
    assert res["error"]["code"] == "invalid_args"
    res = await last_result(rig, tool("rt_sql_read", {"datasource": "vendas",
                                                      "query": "select * from nao_existe"}))
    assert res["error"]["code"] == "tool_error"
    assert S_DB not in json.dumps(res)


async def test_b9_data_describe_lists_tables_without_config(rig):
    res = await last_result(rig, tool("rt_data_describe", {}))
    d = res["result"]["datasources"][0]
    assert d["datasource"] == "vendas" and {t["table"] for t in d["tables"]} >= {"clientes",
                                                                                 "pedidos"}
    assert "path" not in json.dumps(res)


def db_rows(rig, sql):
    con = sqlite3.connect(rig.db_path)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


async def test_b9_sql_write_guardrails_and_confirmation(rig):
    ins = "insert into pedidos (cliente_id, valor) values (1, 99)"
    # sem aprovacao nada muda
    res = await last_result(rig, tool("rt_sql_write", {"datasource": "vendas", "statement": ins}),
                            approve="deny")
    assert res["error"]["code"] == "approval_denied" and db_rows(rig, "select * from pedidos") == []
    # aprovado executa
    res = await last_result(rig, tool("rt_sql_write", {"datasource": "vendas", "statement": ins}),
                            approve="approve")
    assert res["ok"] and res["result"]["affected_rows"] == 1
    assert db_rows(rig, "select valor from pedidos") == [(99.0,)]
    # guardrails: sem WHERE, fora da allowlist, DDL, 2 statements
    for stmt in ("update pedidos set valor = 1", "delete from pedidos",
                 "update clientes set saldo = 0 where id = 1", "drop table pedidos",
                 ins + "; " + ins, "select * from pedidos"):
        res = await last_result(rig, tool("rt_sql_write",
                                          {"datasource": "vendas", "statement": stmt}),
                                approve="approve")
        assert res["ok"] is False and res["error"]["code"] == "guardrail", stmt
    assert db_rows(rig, "select count(*) from pedidos") == [(1,)]
    assert db_rows(rig, "select saldo from clientes where id=1") == [(10.5,)]
    # tabela fora da lista explicita nao escreve (lista vazia = qualquer tabela, padrao do produto)
    res = await last_result(rig, tool("rt_sql_write", {"datasource": "vendas", "statement": ins}),
                            approve="approve", limits={"write_tables": ["outra_tabela"]})
    assert res["error"]["code"] == "guardrail"


async def test_b9_sql_transaction_is_atomic_and_chains_returned(rig):
    stmts = ["insert into pedidos (cliente_id, valor) values (1, 5)",
             "insert into pedidos (cliente_id, valor) values (2, {{returned:0}})"]
    res = await last_result(rig, tool("rt_sql_transaction", {
        "datasource": "vendas", "statements": stmts}), approve="approve")
    assert res["ok"] and res["result"]["committed"]
    assert db_rows(rig, "select id, valor from pedidos order by id") == [(1, 5.0), (2, 1.0)]
    # a segunda falha (id duplicado) desfaz a primeira
    bad = ["insert into pedidos (cliente_id, valor) values (9, 9)",
           "insert into pedidos (id, cliente_id, valor) values (1, 9, 9)"]
    res = await last_result(rig, tool("rt_sql_transaction", {
        "datasource": "vendas", "statements": bad}), approve="approve")
    assert res["ok"] is False and res["error"]["code"] == "tool_error"
    assert db_rows(rig, "select count(*) from pedidos") == [(2,)]
    res = await last_result(rig, tool("rt_sql_transaction", {
        "datasource": "vendas", "statements": ["x"] * 51}), approve="approve")
    assert res["error"]["code"] == "guardrail"


# ======================================================================= B10
def test_b10_host_and_ip_rules():
    assert host_allowed("api.partner.test", ["api.partner.test"])
    assert host_allowed("a.b.partner.test", ["*.partner.test"])
    assert not host_allowed("partner.test", ["*.partner.test"])
    assert not host_allowed("evil.com", ["partner.test"])
    assert not host_allowed("api.partner.test.evil.com", ["api.partner.test"])
    for ip in ("10.0.0.1", "127.0.0.1", "169.254.169.254", "192.168.1.1", "172.16.0.1", "::1",
               "fe80::1", "0.0.0.0", "::ffff:10.0.0.1", "100.64.0.1", "100.127.255.254", "198.18.0.1",
               "192.0.0.8", "2001:db8::1"):
        assert ip_forbidden(ip), ip
    assert not ip_forbidden("93.184.216.34")


async def test_b10_rt_http_allowlist_ssrf_and_secret_resolution(rig):
    ok = await last_result(rig, tool("rt_http", {
        "method": "GET", "url": "https://api.partner.test/v1/dados?x=1",
        "headers": {"X-Key": "{{secret:API_KEY}}"}}))
    assert ok["ok"] and ok["result"]["status"] == 200
    assert rig.ext.partner_calls[-1]["headers"]["x-key"] == S_APIKEY  # resolvido no broker

    cases = {
        "https://evil.example.com/": "forbidden_target",          # fora da allowlist
        "http://internal.test/admin": "forbidden_target",         # allowlisted mas resolve p/ 10.x
        "http://meta.test/latest/meta-data": "forbidden_target",  # metadata 169.254.169.254
        "http://local.test/": "forbidden_target",                 # loopback
        "http://127.0.0.1:8080/": "forbidden_target",             # literal
        "https://user:pw@api.partner.test/": "guardrail",         # credencial embutida
        "ftp://api.partner.test/": "invalid_args",
        "https://{{secret:API_KEY}}.partner.test/": "guardrail",  # segredo no host
    }
    for url, code in cases.items():
        res = await last_result(rig, tool("rt_http", {"method": "GET", "url": url}))
        assert res["ok"] is False and res["error"]["code"] == code, (url, res)
    res = await last_result(rig, tool("rt_http", {
        "method": "GET", "url": "https://api.partner.test/x",
        "headers": {"A": "{{secret:NAO_EXISTE}}"}}))
    assert res["error"]["code"] == "invalid_args"
    # com lista explicita, host fora dela nao passa (sem lista = qualquer host publico)
    res = await last_result(rig, tool("rt_http", {"method": "GET",
                                                  "url": "https://api.partner.test/x"}),
                            limits={"http_allowed_hosts": ["so-este.test"]})
    assert res["error"]["code"] == "forbidden_target"
    assert len(rig.ext.partner_calls) == 1  # so a primeira chegou ao parceiro


async def test_b10_rt_http_write_methods_need_confirmation(rig):
    args = {"method": "POST", "url": "https://api.partner.test/pedidos", "body": {"a": 1}}
    res = await last_result(rig, tool("rt_http", args), approve="deny")
    assert res["error"]["code"] == "approval_denied" and not rig.ext.partner_calls
    res = await last_result(rig, tool("rt_http", args), approve="approve")
    assert res["ok"] and rig.ext.partner_calls[-1]["method"] == "POST"
    res = await last_result(rig, tool("rt_http", args), limits={"http_write_confirm": False})
    assert res["ok"]


async def test_b10_dns_rebinding_resolve_uma_vez_e_conecta_no_ip_validado(rig):
    """Resolvedor que muda de resposta a cada chamada: o IP validado e o IP usado."""
    calls: list[str] = []
    answers = [["93.184.216.34"], ["169.254.169.254"], ["10.0.0.5"]]

    async def flip(host: str) -> list[str]:
        calls.append(host)
        return answers[min(len(calls) - 1, len(answers) - 1)]

    rig.kernel.broker.resolver = flip
    res = await last_result(rig, tool("rt_http", {
        "method": "GET", "url": "https://api.partner.test:8443/v1/x?q=1"}))
    assert res["ok"], res
    assert calls == ["api.partner.test"]  # uma unica resolucao
    req = next(r for r in rig.ext.requests if r.url.host == "93.184.216.34")
    assert req.headers["host"] == "api.partner.test:8443"
    assert req.extensions["sni_hostname"] == "api.partner.test"
    assert (req.url.port, req.url.path, req.url.query) == (8443, "/v1/x", b"q=1")
    assert not [r for r in rig.ext.requests if r.url.host == "api.partner.test"]


async def test_b10_resposta_com_um_ip_proibido_barra_tudo(rig):
    async def mixed(host: str) -> list[str]:
        return ["93.184.216.34", "100.64.0.9"]

    rig.kernel.broker.resolver = mixed
    res = await last_result(rig, tool("rt_http", {"method": "GET",
                                                  "url": "https://api.partner.test/"}))
    assert res["error"]["code"] == "forbidden_target"
    assert not rig.ext.partner_calls


def test_b10_pinned_target_e_ipv6_transicao():
    from broker.http_tool import pinned_target
    url, hdr, ext = pinned_target("https://api.x.test/a?b=1", "93.184.216.34")
    assert url == "https://93.184.216.34/a?b=1"
    assert hdr == {"Host": "api.x.test"} and ext == {"sni_hostname": "api.x.test"}
    url, hdr, _ = pinned_target("http://api.x.test:81/", "2606:4700::1111")
    assert url == "http://[2606:4700::1111]:81/" and hdr == {"Host": "api.x.test:81"}
    _, _, ext = pinned_target("http://8.8.8.8/", "8.8.8.8")
    assert ext == {}
    for ip in ("64:ff9b::a00:1", "2002:a00:1::", "::ffff:100.64.0.1", "::ffff:169.254.169.254"):
        assert ip_forbidden(ip), ip
    assert not ip_forbidden("2606:4700::1111")


# ======================================================================= B11
async def test_b11_artifact_put_roundtrip_and_validation(rig):
    payload = b"%PDF-1.4 conteudo"
    res = await last_result(rig, tool("rt_artifact_put", {
        "name": "contrato.pdf", "kind": "file",
        "content_base64": base64.b64encode(payload).decode()}))
    aid = res["result"]["artifact_id"]
    assert rig.backend.files[aid] == payload
    assert rig.backend.artifacts[aid]["init"]["extension"] == "pdf"
    assert rig.backend.artifacts[aid]["init"]["kind"] == "file"
    res = await last_result(rig, tool("rt_artifact_put", {"name": "a", "kind": "file",
                                                          "content_base64": "@@@"}))
    assert res["error"]["code"] == "invalid_args"
    res = await last_result(rig, tool("rt_artifact_put", {"name": "a", "kind": "exe",
                                                          "content_base64": "AAAA"}))
    assert res["error"]["code"] == "invalid_args"


# ======================================================================= B12
async def test_b12_docs_search_and_memory_recall(rig):
    rig.backend.docs.append({"tenant_id": "t1", "text": "Politica de troca em 7 dias"})
    rig.backend.docs.append({"tenant_id": "t2", "text": "Politica secreta do outro tenant"})
    rig.backend.memories[("t1", "u1")] = [{"text": "prefere e-mail"}]
    res = await last_result(rig, tool("rt_search_docs", {"query": "politica"}))
    assert [r["text"] for r in res["result"]["results"]] == ["Politica de troca em 7 dias"]
    res = await last_result(rig, tool("rt_memory_recall", {"query": "x"}))
    assert res["result"]["memories"] == [{"text": "prefere e-mail"}]
    ev = await rig.send(rig.body(user=None, actor_type="external", channel="api",
                                 message=tool("rt_memory_recall", {}) + "[[echo]]"))
    assert json.loads(final(ev).data["text"])["result"]["memories"] == []  # externo: sem memoria


# ======================================================================= B13
async def test_b13_credentials_survive_only_while_run_is_active(rig):
    ev = await rig.send(rig.body(message="[[say:oi]]"))
    assert final(ev).type == "done"
    assert rig.kernel.contexts.all() == []
    assert not rig.kernel.pending._d


# ======================================================================= B14
async def test_b14_oversized_tool_output_is_cut_with_warning(make_rig):
    rig = await make_rig(1, tool_output_limit=2000)
    res = await last_result(rig, tool("rt_search_docs", {"query": "a"}))
    assert res["ok"]  # saida pequena passa
    rig.backend.docs.extend({"tenant_id": "t1", "text": "a" * 900 + str(i)} for i in range(5))
    ev = await rig.send(rig.body(message=tool("rt_search_docs", {"query": "a", "top_k": 10})
                                 + "[[echo]]"))
    out = json.loads(final(ev).data["text"])
    assert out["truncated"] is True and "NÃO some" in out["note"]


# ======================================================================= B15
async def test_b15_tool_call_cap_per_turn(rig):
    t = rig.spawn(rig.body(run_id="cap1", message="[[sleep:5]]",
                           limits={"max_tool_calls_per_turn": 5}))
    await asyncio.sleep(0.6)
    sid = session_id_for("t1", "u1", "c1")
    results = [(await rig.broker(sid, "rt_echo", {"text": str(i)}))[1] for i in range(7)]
    assert [r["ok"] for r in results] == [True] * 5 + [False] * 2
    assert results[5]["error"]["code"] == "limit"
    await rig.client.post("/v1/runs/cap1/cancel", json={},
                          headers={"Authorization": "Bearer svc-token-test-0001"})
    await t


# ======================================================================= B16
async def test_b16_audit_logs_tool_use_without_values(rig, caplog):
    caplog.set_level(logging.INFO, logger="harness.audit")
    await last_result(rig, tool("rt_sql_read", {"datasource": "vendas",
                                                "query": "select * from clientes"}))
    recs = [json.loads(r.message) for r in caplog.records if r.name == "harness.audit"]
    assert recs and recs[0]["tool"] == "rt_sql_read" and recs[0]["ok"] is True
    assert recs[0]["credential_kind"] == "datasource" and recs[0]["tenant_id"] == "t1"
    blob = json.dumps(recs)
    for s in SENTINELS:
        assert s not in blob
    assert "select" not in blob.lower()  # nem o SQL


# ================================================================== redacao
def test_redactor_masks_values_by_key_and_by_secrets_block():
    creds = {"secrets": {"A": "valor-secreto-1"}, "payment": {"access_token": "tok-abcdef"},
             "datasources": [{"name": "d", "secret": "senha-do-banco"}],
             "email_accounts": [{"password": "mail-pass-1", "email_address": "x@y.test"}]}
    vals = collect_secret_values(creds)
    assert {"valor-secreto-1", "tok-abcdef", "senha-do-banco", "mail-pass-1"} <= vals
    assert "x@y.test" not in vals
    red = Redactor.from_credentials(creds)
    out = red.scrub_obj({"a": "usei tok-abcdef e senha-do-banco", "l": ["mail-pass-1"]})
    assert "tok-abcdef" not in json.dumps(out) and "[REDACTED]" in json.dumps(out)


_ = (S_MAIL, BROKER_TOKEN)


# ===================================================== cancelamento em espera
async def test_cancel_while_waiting_for_confirmation_releases_everything(rig):
    msg = tool("rt_pix_create", {"amount": "10", "reference": "cw1"}) + "[[say:nao chega]]"
    seen = asyncio.Event()

    async def on(ev):
        if ev.type == "approval_required":
            seen.set()

    t = rig.spawn(rig.body(run_id="cw", message=msg), on_event=on)
    await asyncio.wait_for(seen.wait(), 10)
    r = await rig.client.post("/v1/runs/cw/cancel", json={},
                              headers={"Authorization": "Bearer svc-token-test-0001"})
    assert r.json()["status"] == "cancelling"
    ev = await asyncio.wait_for(t, 15)
    assert final(ev).type == "error" and final(ev).data["code"] == "cancelled"
    assert not rig.ext.mp_requests() and rig.kernel.contexts.active() == 0
    # decidir depois do fim nao executa nada
    aid = next(e.data["approval_id"] for e in ev if e.type == "approval_required")
    r = await rig.decide(aid, "approve")
    assert r.status_code == 200 and not rig.ext.mp_requests()


async def test_b6b_rt_confirm_sem_regra_nao_bloqueia_canal_externo(rig):
    ev = await rig.send(rig.body(user=None, actor_type="external", channel="chatwoot",
                                 limits={"confirm_actions": False},
                                 message=tool("rt_confirm", {"summary": "Gerar Pix de R$ 5,00"})
                                 + "[[echo]]"))
    res = json.loads(final(ev).data["text"])
    assert res["ok"] is True and res["result"]["approved"] is True
    assert "approval_required" not in types(ev)
