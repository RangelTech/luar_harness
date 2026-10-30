"""Configuracao do kernel-harness: tudo por env (nenhum segredo na imagem)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    return default if v is None else v.strip().lower() in ("1", "true", "yes", "on")


def _csv(name: str) -> list[str]:
    return [p.strip() for p in _env(name).split(",") if p.strip()]


@dataclass
class WorkerSpec:
    id: str
    url: str
    key: str = ""
    broker_token: str = ""  # token do pool deste worker (identifica o worker no broker)
    slots: int = 8  # = RT_WORKER_SLOTS do front do worker efemero


@dataclass
class Settings:
    # --- infra
    redis_url: str = "redis://localhost:6379/0"
    boot_retry_s: float = 2.0  # espera entre tentativas se o Redis ainda nao responde
    # --- auth
    service_tokens: list[str] = field(default_factory=list)  # bearer backend -> harness
    run_token_secret: str = ""  # HS256 do run_token (compartilhado com o backend)
    run_token_required: bool = True
    broker_tokens: list[str] = field(default_factory=list)  # tokens compartilhados do pool
    # --- workers
    workers: list[WorkerSpec] = field(default_factory=list)
    worker_health_interval_s: float = 5.0
    worker_probe_failures: int = 3  # falhas seguidas de /health ate marcar o worker fora
    # --- backend / servicos
    backend_url: str = "http://backend:8090"
    backend_internal_token: str = ""
    litellm_url: str = "http://llm-gw:4000"
    litellm_key: str = ""
    runtime_manager_url: str = ""
    rm_service: str = "kernel-harness"
    rm_secret: str = ""
    apps_base_domain: str = "apps.rangeltech.net"
    mercado_pago_api: str = "https://api.mercadopago.com"
    google_calendar_base: str = "https://www.googleapis.com/calendar/v3"
    google_sheets_base: str = "https://sheets.googleapis.com/v4"
    ms_graph_base: str = "https://graph.microsoft.com/v1.0"
    # --- disco compartilhado do pool (volumes)
    profiles_root: str = "/srv/hermes/profiles"
    users_root: str = "/srv/hermes/users"
    tenant_skills_root: str = "/skills/tenant"
    system_skills_dir: str = "/skills/system"
    # --- fila / agendamento (spec 5.3)
    tenant_concurrency: int = 2
    tenant_weights: dict[str, float] = field(default_factory=dict)
    queue_expiry_s: float = 180.0  # espera maxima na fila (a posicao e a estimativa aparecem ao usuario)
    run_est_s: float = 25.0  # duracao media estimada de uma run, para o ETA da fila
    model_retry_delay_s: float = 8.0
    queue_max_total: int = 1000
    queue_max_per_tenant: int = 50
    chat_lock_ttl_ms: int = 15_000
    scheduler_tick_s: float = 0.05
    events_ttl_s: int = 600
    run_state_ttl_s: int = 86_400
    idempotency_ttl_s: int = 86_400
    # --- limites da run (spec 4.3)
    silence_timeout_s: float = 120.0
    turn_timeout_s: float = 1800.0
    queue_status_url: str = ""   # GET de estado da fila de IA (llm-queue /v1/gpu/status); vazio = vigia desligado
    queue_status_key: str = ""
    queue_watch_after_s: float = 15.0
    queue_watch_poll_s: float = 10.0
    queue_watch_cap_s: float = 1700.0
    max_tool_calls_per_turn: int = 40
    tool_output_limit: int = 24_000
    sql_max_rows: int = 50_000
    sql_read_timeout_s: float = 60.0
    artifact_preview_rows: int = 10
    approval_timeout_s: float = 120.0
    hermes_reconnect_attempts: int = 3
    hermes_reconnect_backoff_s: float = 0.4
    hermes_start_retries: int = 4
    # --- compactacao / usage
    compress_threshold: float = 0.60
    context_window: int = 200_000
    usage_mode: str = "cumulative"  # cumulative | turn (R-09 / H5)
    # --- Pix
    pix_max_amount: str = "10000.00"
    # --- rt_http
    http_allowed_hosts_default: list[str] = field(default_factory=list)
    http_max_response_chars: int = 20_000
    # --- varredura de pending/ (aprendizado)
    sweep_enabled: bool = True
    # --- dev
    insecure_dev: bool = False
    # --- Hermes real: segredo mestre das chaves por perfil (vazio = modo legado/fake)
    hermes_master_secret: str = ""
    plugin_src: str = "/opt/rt/plugins/rt-broker"
    llm_worker_key: str = ""  # chave virtual do gateway de modelos gravada no .env de cada perfil
    worker_system_skills_dir: str = "/skills/system"  # caminho das skills de sistema NO worker

    @classmethod
    def from_env(cls) -> Settings:
        workers_raw = _env("HARNESS_WORKERS", "[]")
        workers = [WorkerSpec(**w) for w in json.loads(workers_raw)]
        return cls(
            redis_url=_env("REDIS_URL", cls.redis_url),
            service_tokens=_csv("HARNESS_SERVICE_TOKENS"),
            run_token_secret=_env("HARNESS_RUN_TOKEN_SECRET"),
            run_token_required=_bool("HARNESS_RUN_TOKEN_REQUIRED", True),
            broker_tokens=_csv("HARNESS_BROKER_TOKENS"),
            workers=workers,
            worker_health_interval_s=_float("HARNESS_WORKER_HEALTH_INTERVAL_S", 5.0),
            worker_probe_failures=_int("HARNESS_WORKER_PROBE_FAILURES", 3),
            run_est_s=_float("HARNESS_RUN_EST_S", 25.0),
            backend_url=_env("BACKEND_URL", cls.backend_url),
            backend_internal_token=_env("BACKEND_INTERNAL_TOKEN"),
            litellm_url=_env("LITELLM_URL", cls.litellm_url),
            litellm_key=_env("LITELLM_KEY"),
            runtime_manager_url=_env("RUNTIME_MANAGER_URL"),
            rm_service=_env("RM_SERVICE", "kernel-harness"),
            rm_secret=_env("RM_SECRET"),
            apps_base_domain=_env("APPS_BASE_DOMAIN", cls.apps_base_domain),
            mercado_pago_api=_env("MERCADO_PAGO_API", cls.mercado_pago_api),
            profiles_root=_env("PROFILES_ROOT", cls.profiles_root),
            users_root=_env("USERS_ROOT", cls.users_root),
            tenant_skills_root=_env("TENANT_SKILLS_ROOT", cls.tenant_skills_root),
            system_skills_dir=_env("SYSTEM_SKILLS_DIR", cls.system_skills_dir),
            tenant_concurrency=_int("HARNESS_TENANT_CONCURRENCY", 2),
            tenant_weights=json.loads(_env("HARNESS_TENANT_WEIGHTS", "{}")),
            queue_expiry_s=_float("HARNESS_QUEUE_EXPIRY_S", 180.0),
            model_retry_delay_s=_float("HARNESS_MODEL_RETRY_S", 8.0),
            silence_timeout_s=_float("HARNESS_SILENCE_TIMEOUT_S", 120.0),
            turn_timeout_s=_float("HARNESS_TURN_TIMEOUT_S", 1800.0),
            queue_status_url=_env("HARNESS_QUEUE_STATUS_URL", ""),
            queue_status_key=_env("HARNESS_QUEUE_STATUS_KEY", ""),
            queue_watch_after_s=_float("HARNESS_QUEUE_WATCH_AFTER_S", 15.0),
            queue_watch_poll_s=_float("HARNESS_QUEUE_WATCH_POLL_S", 10.0),
            queue_watch_cap_s=_float("HARNESS_QUEUE_WATCH_CAP_S", 1700.0),
            max_tool_calls_per_turn=_int("HARNESS_MAX_TOOL_CALLS", 40),
            tool_output_limit=_int("HARNESS_TOOL_OUTPUT_LIMIT", 24_000),
            approval_timeout_s=_float("HARNESS_APPROVAL_TIMEOUT_S", 120.0),
            compress_threshold=_float("HARNESS_COMPRESS_THRESHOLD", 0.60),
            context_window=_int("HARNESS_CONTEXT_WINDOW", 200_000),
            usage_mode=_env("HARNESS_USAGE_MODE", "cumulative"),
            pix_max_amount=_env("HARNESS_PIX_MAX_AMOUNT", "10000.00"),
            http_allowed_hosts_default=_csv("HARNESS_HTTP_ALLOWED_HOSTS"),
            insecure_dev=_bool("HARNESS_INSECURE_DEV", False),
            hermes_master_secret=_env("HERMES_MASTER_SECRET"),
            plugin_src=_env("RT_PLUGIN_SRC", cls.plugin_src),
            llm_worker_key=_env("LLM_WORKER_KEY"),
            worker_system_skills_dir=_env("WORKER_SYSTEM_SKILLS_DIR", cls.worker_system_skills_dir),
        )
