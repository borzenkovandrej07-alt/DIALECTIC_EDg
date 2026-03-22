"""
ai_provider.py — Мультипровайдер с роутингом по агентам.

Переменные окружения (Railway / .env):
  AI_DEBATE_PRIMARY — кто первым отвечает в дебатах:
      mistral (по умолчанию) | groq | openrouter | together | gemini
  GROQ_MODEL, OPENROUTER_MODEL, TOGETHER_MODEL — модели для соответствующего primary
  OPENROUTER_SYNTH_MODEL — опционально, иначе как OPENROUTER_MODEL
  MISTRAL_SYNTH_MODEL — для synth при primary=mistral (по умолчанию mistral-large-latest)

Fallback после ошибки primary: остальные провайдеры по цепочке (без повтора того же API).
"""

import logging
import os
import re
import time
import asyncio
import aiohttp

from config import MAX_TOKENS_PER_AGENT, AGENT_TEMPERATURE

logger = logging.getLogger(__name__)
TIMEOUT = aiohttp.ClientTimeout(total=180)

# ── Ключи ──────────────────────────────────────────────────────────────────────
MISTRAL_API_KEY    = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_KEY_2  = os.getenv("MISTRAL_API_KEY_2", "")   # резервный Mistral
MISTRAL_MODEL      = os.getenv("MISTRAL_MODEL", "mistral-small-latest")
MISTRAL_URL        = "https://api.mistral.ai/v1/chat/completions"

OPENROUTER_API_KEY   = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_API_KEY_2 = os.getenv("OPENROUTER_API_KEY_2", "")  # резервный OpenRouter
OPENROUTER_URL       = "https://openrouter.ai/api/v1/chat/completions"

TOGETHER_API_KEY   = os.getenv("TOGETHER_API_KEY", "")
TOGETHER_API_KEY_2 = os.getenv("TOGETHER_API_KEY_2", "")  # резервный Together
TOGETHER_URL       = "https://api.together.xyz/v1/chat/completions"

GROQ_API_KEY       = os.getenv("GROQ_API_KEY", "")
GROQ_API_KEY_2     = os.getenv("GROQ_API_KEY_2", "")   # второй аккаунт
GROQ_API_KEY_3     = os.getenv("GROQ_API_KEY_3", "")   # третий аккаунт
GROQ_URL           = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL         = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"

OPENROUTER_MODEL = os.getenv(
    "OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free"
).strip() or "meta-llama/llama-3.3-70b-instruct:free"
OPENROUTER_SYNTH_MODEL = os.getenv("OPENROUTER_SYNTH_MODEL", "").strip() or OPENROUTER_MODEL

TOGETHER_MODEL = os.getenv(
    "TOGETHER_MODEL", "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free"
).strip() or "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-1.5-flash").strip() or "gemini-1.5-flash"

# Cerebras — бесплатный, очень быстрый
CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY", "")
CEREBRAS_MODEL   = os.getenv("CEREBRAS_MODEL", "llama-3.3-70b").strip() or "llama-3.3-70b"
CEREBRAS_URL     = "https://api.cerebras.ai/v1/chat/completions"


# ── Трекинг моделей для честного лейбла в отчёте ─────────────────────────────
MODELS_USED: dict = {}  # {"bull": "Mistral Small", "synth": "Mistral Large", ...}

def _track_model(agent_key: str, provider: str, model: str):
    labels = {
        "mistral-small-latest":                    "Mistral Small",
        "mistral-large-latest":                    "Mistral Large",
        "llama-3.3-70b-versatile":                 "Groq/Llama 3.3 70B",
        "meta-llama/llama-3.3-70b-instruct:free":  "OpenRouter/Llama 3.3 70B",
        "google/gemma-3-27b-it:free":              "OpenRouter/Gemma 3 27B",
    }
    label = labels.get(model, f"{provider}/{model}")
    MODELS_USED[agent_key] = label
    logger.info(f"[{agent_key}] использует: {label}")


def _debate_primary_env() -> str:
    return os.getenv("AI_DEBATE_PRIMARY", "mistral").strip().lower() or "mistral"


def _can_use_primary(name: str) -> bool:
    if name == "mistral":
        return bool(MISTRAL_API_KEY or MISTRAL_API_KEY_2)
    if name == "groq":
        return bool(GROQ_API_KEY or GROQ_API_KEY_2 or GROQ_API_KEY_3)
    if name == "openrouter":
        return bool(OPENROUTER_API_KEY or OPENROUTER_API_KEY_2)
    if name == "together":
        return bool(TOGETHER_API_KEY or TOGETHER_API_KEY_2)
    if name == "gemini":
        return bool(GEMINI_API_KEY)
    if name == "cerebras":
        return bool(CEREBRAS_API_KEY)
    return False


def _resolve_agent_models() -> dict:
    """Кто первым обрабатывает дебаты (остальное — fallback в _call_best_available)."""
    want = _debate_primary_env()
    if want not in ("mistral", "groq", "openrouter", "together", "gemini", "cerebras", "mixed"):
        logger.warning("AI_DEBATE_PRIMARY=%s неизвестен — использую mistral", want)
        want = "mistral"
    if not _can_use_primary(want):
        logger.warning(
            "AI_DEBATE_PRIMARY=%s недоступен (нет ключа) — откат на mistral/groq по наличию ключей",
            want,
        )
        if _can_use_primary("mistral"):
            want = "mistral"
        elif _can_use_primary("groq"):
            want = "groq"
        elif _can_use_primary("openrouter"):
            want = "openrouter"
        elif _can_use_primary("together"):
            want = "together"
        elif _can_use_primary("gemini"):
            want = "gemini"
        elif _can_use_primary("cerebras"):
            want = "cerebras"
        else:
            want = next(
                (n for n in ("groq", "openrouter", "together", "gemini", "mistral")
                 if _can_use_primary(n)),
                "mistral",
            )

    mm = os.getenv("MISTRAL_MODEL", MISTRAL_MODEL).strip() or MISTRAL_MODEL
    syn_m = os.getenv("MISTRAL_SYNTH_MODEL", "mistral-large-latest").strip() or "mistral-large-latest"

    if want == "groq":
        m = {"bull": {"provider": "groq", "model": GROQ_MODEL},
             "verifier": {"provider": "groq", "model": GROQ_MODEL},
             "bear": {"provider": "groq", "model": GROQ_MODEL},
             "synth": {"provider": "groq", "model": GROQ_MODEL}}
    elif want == "openrouter":
        m = {"bull": {"provider": "openrouter", "model": OPENROUTER_MODEL},
             "verifier": {"provider": "openrouter", "model": OPENROUTER_MODEL},
             "bear": {"provider": "openrouter", "model": OPENROUTER_MODEL},
             "synth": {"provider": "openrouter", "model": OPENROUTER_SYNTH_MODEL}}
    elif want == "together":
        m = {"bull": {"provider": "together", "model": TOGETHER_MODEL},
             "verifier": {"provider": "together", "model": TOGETHER_MODEL},
             "bear": {"provider": "together", "model": TOGETHER_MODEL},
             "synth": {"provider": "together", "model": TOGETHER_MODEL}}
    elif want == "gemini":
        m = {"bull": {"provider": "gemini", "model": GEMINI_MODEL},
             "verifier": {"provider": "gemini", "model": GEMINI_MODEL},
             "bear": {"provider": "gemini", "model": GEMINI_MODEL},
             "synth": {"provider": "gemini", "model": GEMINI_MODEL}}
    elif want == "cerebras":
        m = {"bull":     {"provider": "cerebras", "model": CEREBRAS_MODEL},
             "bear":     {"provider": "cerebras", "model": CEREBRAS_MODEL},
             "verifier": {"provider": "cerebras", "model": CEREBRAS_MODEL},
             "synth":    {"provider": "mistral",  "model": syn_m}}

    elif want == "mixed":
        # Настоящая диалектика — каждый агент на СВОЁМ провайдере
        # Bull    → Cerebras (быстрый, бесплатный, оптимист)
        # Bear    → Groq     (агрессивный скептик)
        # Verifier→ Together (педантичный факт-чекер)
        # Synth   → Mistral Large (мудрый синтезатор)
        def pick(pref_order):
            for p in pref_order:
                if _can_use_primary(p):
                    return p
            return "mistral"

        bull_p = pick(["cerebras", "together", "openrouter", "groq", "mistral"])
        bear_p = pick(["groq", "cerebras", "together", "openrouter", "mistral"])
        ver_p  = pick(["together", "openrouter", "cerebras", "groq", "mistral"])
        syn_p  = pick(["mistral", "groq", "cerebras"])

        def model_for(p):
            return {
                "cerebras": CEREBRAS_MODEL,
                "groq":     GROQ_MODEL,
                "together": TOGETHER_MODEL,
                "openrouter": OPENROUTER_MODEL,
                "mistral":  mm,
            }.get(p, mm)

        m = {
            "bull":     {"provider": bull_p, "model": model_for(bull_p)},
            "bear":     {"provider": bear_p, "model": model_for(bear_p)},
            "verifier": {"provider": ver_p,  "model": model_for(ver_p)},
            "synth":    {"provider": syn_p,  "model": syn_m if syn_p == "mistral" else model_for(syn_p)},
        }
        logger.info(f"Mixed режим: Bull={bull_p} Bear={bear_p} Verifier={ver_p} Synth={syn_p}")

    else:
        m = {"bull": {"provider": "mistral", "model": mm},
             "verifier": {"provider": "mistral", "model": mm},
             "bear": {"provider": "mistral", "model": mm},
             "synth": {"provider": "mistral", "model": syn_m}}

    logger.info("Дебаты: первичный провайдер = %s (AI_DEBATE_PRIMARY)", want)
    return m


def get_models_summary() -> str:
    if not MODELS_USED:
        return "🐂 Bull | 🐻 Bear | 🔍 Verifier | ⚖️ Synth"
    bull     = MODELS_USED.get("bull", "?")
    bear     = MODELS_USED.get("bear", "?")
    verifier = MODELS_USED.get("verifier", "?")
    synth    = MODELS_USED.get("synth", "?")
    return (
        f"🐂 Bull = {bull} | "
        f"🐻 Bear = {bear} | "
        f"🔍 Verifier = {verifier} | "
        f"⚖️ Synth = {synth}"
    )


# ── Модели по агентам (первый ход дебатов) ────────────────────────────────────
AGENT_MODELS = _resolve_agent_models()

_AGENT_MAX_TOKENS = {
    "bull":     2500,   # увеличено — 4 возможности Russia Edge не обрезаются
    "bear":     2500,   # увеличено — 4 риска Russia Edge не обрезаются
    "verifier": 1000,
    "synth":    6000,   # полный синтез с эффектами 2-3 порядка
}


# ── Базовый вызов ──────────────────────────────────────────────────────────────

async def _call_openai_style(
    url: str, api_key: str, model: str,
    prompt: str, system: str, temperature: float, name: str,
    extra_headers: dict = None, agent_key: str = None
) -> str:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    max_tok = _AGENT_MAX_TOKENS.get(agent_key, MAX_TOKENS_PER_AGENT)
    payload = {
        "model": model,
        "messages": messages,
        "temperature": min(temperature, 1.0),
        "max_tokens": max_tok,
    }

    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                err = await resp.text()
                raise RuntimeError(f"{name} HTTP {resp.status}: {err[:300]}")
            data = await resp.json()
            return data["choices"][0]["message"]["content"].strip()


# ── Провайдеры ────────────────────────────────────────────────────────────────

async def _call_groq(prompt: str, system: str, temperature: float,
                     model: str = None, agent_key: str = None) -> str:
    """Groq#1 → Groq#2 при 429."""
    if not GROQ_API_KEY and not GROQ_API_KEY_2 and not GROQ_API_KEY_3:
        raise ValueError("Нет GROQ_API_KEY")

    m = model or GROQ_MODEL
    keys_to_try = []
    if GROQ_API_KEY:   keys_to_try.append(("Groq#1", GROQ_API_KEY))
    if GROQ_API_KEY_2: keys_to_try.append(("Groq#2", GROQ_API_KEY_2))
    if GROQ_API_KEY_3: keys_to_try.append(("Groq#3", GROQ_API_KEY_3))

    last_err = None
    for key_name, key in keys_to_try:
        try:
            result = await _call_openai_style(
                GROQ_URL, key, m, prompt, system, temperature, key_name,
                agent_key=agent_key
            )
            if agent_key:
                _track_model(agent_key, key_name, m)
            logger.info(f"Groq {key_name} ✅")
            return result
        except RuntimeError as e:
            err_s = str(e)
            if "429" in err_s:
                current_idx = keys_to_try.index((key_name, key))
                has_next = current_idx < len(keys_to_try) - 1
                if has_next:
                    logger.warning(f"{key_name} лимит → сразу пробую следующий ключ...")
                    last_err = e
                    continue
                else:
                    wait_m = re.search(r"try again in ([\d.]+)\s*s", err_s, re.I)
                    if wait_m:
                        sec = min(30.0, float(wait_m.group(1)) + 1.0)
                        logger.warning("%s последний ключ — жду %.1fs...", key_name, sec)
                        await asyncio.sleep(sec)
                        try:
                            result = await _call_openai_style(
                                GROQ_URL, key, m, prompt, system, temperature,
                                key_name, agent_key=agent_key,
                            )
                            if agent_key:
                                _track_model(agent_key, key_name, m)
                            logger.info(f"Groq {key_name} ✅ (после паузы)")
                            return result
                        except RuntimeError as e2:
                            last_err = e2
                    logger.warning(f"{key_name} лимит исчерпан")
                    last_err = e
                    continue
            raise
    raise RuntimeError(f"Все Groq ключи исчерпаны. Последняя ошибка: {last_err}")


async def _call_mistral(prompt: str, system: str, temperature: float,
                        model: str = None, agent_key: str = None) -> str:
    """Mistral с автопереключением KEY_1 → KEY_2 при 429."""
    m = model or MISTRAL_MODEL
    keys_to_try = []
    if MISTRAL_API_KEY:   keys_to_try.append(("Mistral#1", MISTRAL_API_KEY))
    if MISTRAL_API_KEY_2: keys_to_try.append(("Mistral#2", MISTRAL_API_KEY_2))
    if not keys_to_try:
        raise ValueError("Нет MISTRAL_API_KEY")
    last_err = None
    for key_name, key in keys_to_try:
        try:
            result = await _call_openai_style(
                MISTRAL_URL, key, m,
                prompt, system, temperature, key_name,
                agent_key=agent_key
            )
            if agent_key:
                _track_model(agent_key, key_name, m)
            logger.info(f"{key_name} ✅")
            return result
        except RuntimeError as e:
            if "429" in str(e):
                logger.warning(f"{key_name} лимит — пробую Mistral#2...")
                last_err = e
                await asyncio.sleep(2.5)
                continue
            raise
    raise RuntimeError(f"Все Mistral ключи исчерпаны: {last_err}")


_OR_HEADERS = {
    "HTTP-Referer": "https://dialectic-edge.bot",
    "X-Title": "Dialectic Edge",
}


async def _call_openrouter_model(
    prompt: str,
    system: str,
    temperature: float,
    model: str,
    agent_key: str = None,
) -> str:
    """OpenRouter: KEY_1 → KEY_2 при 429/402."""
    keys_try = []
    if OPENROUTER_API_KEY:
        keys_try.append(("OpenRouter", OPENROUTER_API_KEY))
    if OPENROUTER_API_KEY_2:
        keys_try.append(("OpenRouter#2", OPENROUTER_API_KEY_2))
    if not keys_try:
        raise ValueError("Нет OPENROUTER_API_KEY")
    last_err = None
    for key_name, key in keys_try:
        try:
            result = await _call_openai_style(
                OPENROUTER_URL, key, model,
                prompt, system, temperature, key_name,
                extra_headers=_OR_HEADERS,
                agent_key=agent_key,
            )
            if agent_key:
                _track_model(agent_key, key_name, model)
            return result
        except RuntimeError as e:
            err = str(e)
            if "429" in err or "402" in err:
                logger.warning("%s лимит OpenRouter — следующий ключ...", key_name)
                last_err = e
                continue
            raise
    raise RuntimeError(f"Все OpenRouter ключи исчерпаны: {last_err}")


async def _call_openrouter_llama(prompt: str, system: str, temperature: float,
                                  agent_key: str = None) -> str:
    return await _call_openrouter_model(
        prompt, system, temperature,
        "meta-llama/llama-3.3-70b-instruct:free",
        agent_key,
    )


async def _call_openrouter_gemma(prompt: str, system: str, temperature: float,
                                  agent_key: str = None) -> str:
    return await _call_openrouter_model(
        prompt, system, temperature,
        "google/gemma-3-27b-it:free",
        agent_key,
    )


async def _call_cerebras(prompt: str, system: str, temperature: float,
                         agent_key: str = None) -> str:
    """Cerebras — бесплатный, быстрый (~1000 токен/сек)."""
    if not CEREBRAS_API_KEY:
        raise RuntimeError("Нет CEREBRAS_API_KEY")
    try:
        result = await _call_openai_style(
            CEREBRAS_URL, CEREBRAS_API_KEY, CEREBRAS_MODEL,
            prompt, system, min(temperature, 1.0), "Cerebras",
            agent_key=agent_key
        )
        if agent_key:
            _track_model(agent_key, "Cerebras", CEREBRAS_MODEL)
        return result
    except RuntimeError as e:
        logger.warning(f"Cerebras ❌: {e}")
        raise


async def _call_gemini(
    prompt: str, system: str, temperature: float, agent_key: str = None
) -> str:
    if not GEMINI_API_KEY:
        raise ValueError("Нет GEMINI_API_KEY")
    m = GEMINI_MODEL
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent"
    max_tok = _AGENT_MAX_TOKENS.get(agent_key, MAX_TOKENS_PER_AGENT)
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": min(temperature, 1.0),
            "maxOutputTokens": max_tok,
        },
    }
    if system:
        body["systemInstruction"] = {"parts": [{"text": system}]}
    params = {"key": GEMINI_API_KEY}
    async with aiohttp.ClientSession() as s:
        async with s.post(url, params=params, json=body, timeout=TIMEOUT) as resp:
            raw = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Gemini HTTP {resp.status}: {raw[:400]}")
            data = await resp.json()
            cand = data.get("candidates") or []
            if not cand:
                raise RuntimeError(f"Gemini: нет candidates — {raw[:250]}")
            parts = cand[0].get("content", {}).get("parts") or []
            if not parts or not parts[0].get("text"):
                raise RuntimeError("Gemini: пустой текст")
            out = parts[0]["text"].strip()
            if agent_key:
                _track_model(agent_key, "Gemini", m)
            return out


async def _call_together(
    prompt: str,
    system: str,
    temperature: float,
    model: str = None,
    agent_key: str = None,
) -> str:
    """Together AI — KEY_1 → KEY_2 при 429."""
    m = model or TOGETHER_MODEL
    keys_to_try = []
    if TOGETHER_API_KEY:   keys_to_try.append(("Together#1", TOGETHER_API_KEY))
    if TOGETHER_API_KEY_2: keys_to_try.append(("Together#2", TOGETHER_API_KEY_2))
    if not keys_to_try:
        raise ValueError("Нет TOGETHER_API_KEY")

    last_err = None
    for key_name, key in keys_to_try:
        try:
            result = await _call_openai_style(
                TOGETHER_URL, key, m,
                prompt, system, temperature, key_name,
                agent_key=agent_key
            )
            if agent_key:
                _track_model(agent_key, key_name, m)
            logger.info(f"{key_name} ✅")
            return result
        except RuntimeError as e:
            if "429" in str(e):
                logger.warning(f"{key_name} лимит — пробую Together#2...")
                last_err = e
                continue
            raise
    raise RuntimeError(f"Все Together ключи исчерпаны: {last_err}")


# ── Throttle для Mistral ──────────────────────────────────────────────────────
_LAST_MISTRAL_CALL = 0.0
_mistral_lock = None

def _get_mistral_lock() -> asyncio.Lock:
    global _mistral_lock
    if _mistral_lock is None:
        _mistral_lock = asyncio.Lock()
    return _mistral_lock

async def _call_mistral_throttled(prompt: str, system: str, temperature: float,
                                   model: str = None, agent_key: str = None) -> str:
    global _LAST_MISTRAL_CALL
    lock = _get_mistral_lock()
    async with lock:
        now = time.time()
        wait = 3.0 - (now - _LAST_MISTRAL_CALL)
        if wait > 0:
            await asyncio.sleep(wait)
        _LAST_MISTRAL_CALL = time.time()
    return await _call_mistral(prompt, system, temperature, model, agent_key=agent_key)


# ── Роутер ────────────────────────────────────────────────────────────────────

async def _call_for_agent(agent_key: str, prompt: str, system: str, temperature: float) -> str:
    config = AGENT_MODELS.get(agent_key)

    if config:
        provider = config["provider"]
        model    = config["model"]
        try:
            if provider == "mistral":
                result = await _call_mistral_throttled(prompt, system, temperature, model, agent_key=agent_key)
            elif provider == "groq":
                result = await _call_groq(prompt, system, temperature, model, agent_key=agent_key)
            elif provider == "openrouter":
                result = await _call_openrouter_model(
                    prompt, system, temperature, model, agent_key=agent_key
                )
            elif provider == "together":
                result = await _call_together(
                    prompt, system, temperature, model, agent_key=agent_key
                )
            elif provider == "gemini":
                result = await _call_gemini(prompt, system, temperature, agent_key=agent_key)
            elif provider == "cerebras":
                result = await _call_cerebras(prompt, system, temperature, agent_key=agent_key)
            else:
                raise ValueError(f"Неизвестный провайдер: {provider}")
            logger.info(f"[{agent_key}] → {provider}/{model} ✅")
            return result
        except Exception as e:
            logger.warning(f"[{agent_key}] → {provider}/{model} ❌ {e}")

        # Synth: только для Mistral Large → пробуем Small
        if (
            agent_key == "synth"
            and provider == "mistral"
            and model
            and "large" in model.lower()
        ):
            try:
                result = await _call_mistral_throttled(
                    prompt, system, temperature, "mistral-small-latest", agent_key=agent_key
                )
                logger.info(f"[{agent_key}] fallback → mistral-small ✅")
                return result
            except Exception as e2:
                logger.warning(f"[{agent_key}] synth mistral-small ❌ {e2}")

    skip_p = frozenset({config["provider"]} if config else [])
    return await _call_best_available(
        prompt, system, temperature, agent_key,
        skip_providers=skip_p,
    )


async def _call_best_available(
    prompt: str,
    system: str,
    temperature: float,
    agent_name: str = "general",
    *,
    skip_providers: frozenset | None = None,
) -> str:
    """
    Цепочка fallback. skip_providers — не вызывать тот же API повторно
    (primary уже отработал или упал).
    """
    skip = set(skip_providers or [])

    providers = []
    if "mistral" not in skip and (MISTRAL_API_KEY or MISTRAL_API_KEY_2):
        providers.append(("Mistral Small",
            lambda p, s, t: _call_mistral_throttled(p, s, t, agent_key=agent_name)))

    if "groq" not in skip and (GROQ_API_KEY or GROQ_API_KEY_2 or GROQ_API_KEY_3):
        providers.append(("Groq/Llama",
            lambda p, s, t: _call_groq(p, s, t, agent_key=agent_name)))

    if "openrouter" not in skip and (OPENROUTER_API_KEY or OPENROUTER_API_KEY_2):
        providers.append(("OpenRouter/Llama",
            lambda p, s, t: _call_openrouter_llama(p, s, t, agent_key=agent_name)))
        providers.append(("OpenRouter/Gemma",
            lambda p, s, t: _call_openrouter_gemma(p, s, t, agent_key=agent_name)))

    if "together" not in skip and (TOGETHER_API_KEY or TOGETHER_API_KEY_2):
        providers.append(("Together/Llama",
            lambda p, s, t: _call_together(p, s, t, agent_key=agent_name)))

    if "cerebras" not in skip and CEREBRAS_API_KEY:
        providers.append(("Cerebras/Llama",
            lambda p, s, t: _call_cerebras(p, s, t, agent_key=agent_name)))

    if "gemini" not in skip and GEMINI_API_KEY:
        providers.append(("Gemini",
            lambda p, s, t: _call_gemini(p, s, t, agent_key=agent_name)))

    if not providers:
        raise ValueError("Нет API ключей! Добавь GROQ_API_KEY и MISTRAL_API_KEY в Railway")

    last_error = None
    for name, caller in providers:
        try:
            result = await caller(prompt, system, temperature)
            logger.info(f"[{agent_name}] fallback → {name} ✅")
            return result
        except Exception as e:
            logger.warning(f"[{agent_name}] fallback → {name} ❌ {e}")
            last_error = e

    raise RuntimeError(f"Все провайдеры недоступны. Последняя ошибка: {last_error}")


# ── Публичный класс ───────────────────────────────────────────────────────────

class AgentProvider:

    async def bull(self, prompt: str, system: str = "", temperature: float = None) -> str:
        t = temperature or AGENT_TEMPERATURE
        return await _call_for_agent("bull", prompt, system, t)

    async def bear(self, prompt: str, system: str = "", temperature: float = None) -> str:
        t = (temperature or AGENT_TEMPERATURE) * 0.4
        return await _call_for_agent("bear", prompt, system, t)

    async def verifier(self, prompt: str, system: str = "", temperature: float = None) -> str:
        t = 0.1
        return await _call_for_agent("verifier", prompt, system, t)

    async def synth(self, prompt: str, system: str = "", temperature: float = None) -> str:
        t = (temperature or AGENT_TEMPERATURE) * 0.6
        return await _call_for_agent("synth", prompt, system, t)

    async def complete(self, prompt: str, system: str = "", temperature: float = None) -> str:
        t = temperature or AGENT_TEMPERATURE
        return await _call_best_available(prompt, system, t, "general", skip_providers=frozenset())


ai = AgentProvider()
