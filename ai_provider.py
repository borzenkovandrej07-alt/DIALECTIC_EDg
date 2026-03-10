"""
ai_provider.py — Мультипровайдер для России.

Порядок попыток для каждого агента:
1. Mistral (console.mistral.ai — бесплатно 1B токенов/месяц)
2. Together.ai (together.ai — $1 бесплатно при регистрации)
3. OpenRouter (openrouter.ai — бесплатные модели)
4. Fallback — возвращает честное сообщение

Все три регистрируются без карты через Google аккаунт.
"""

import logging
import os
import aiohttp

from config import MAX_TOKENS_PER_AGENT, AGENT_TEMPERATURE

logger = logging.getLogger(__name__)
TIMEOUT = aiohttp.ClientTimeout(total=90)

# ── Ключи ──────────────────────────────────────────────────────────────────────
MISTRAL_API_KEY  = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL    = os.getenv("MISTRAL_MODEL", "mistral-small-latest")
MISTRAL_URL      = "https://api.mistral.ai/v1/chat/completions"

TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY", "")
TOGETHER_MODEL   = os.getenv("TOGETHER_MODEL", "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free")
TOGETHER_URL     = "https://api.together.xyz/v1/chat/completions"

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL   = os.getenv("OPENROUTER_MODEL", "mistralai/mistral-7b-instruct:free")
OPENROUTER_URL     = "https://openrouter.ai/api/v1/chat/completions"


# ── Базовые вызовы ─────────────────────────────────────────────────────────────

async def _call_openai_style(url: str, api_key: str, model: str,
                              prompt: str, system: str,
                              temperature: float, name: str) -> str:
    """Универсальный вызов для OpenAI-совместимых API."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": min(temperature, 1.0),
        "max_tokens": MAX_TOKENS_PER_AGENT,
    }
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload,
                          headers=headers, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                err = await resp.text()
                raise RuntimeError(f"{name} {resp.status}: {err[:200]}")
            data = await resp.json()
            return data["choices"][0]["message"]["content"].strip()


async def _call_mistral(prompt: str, system: str, temperature: float) -> str:
    return await _call_openai_style(
        MISTRAL_URL, MISTRAL_API_KEY, MISTRAL_MODEL,
        prompt, system, temperature, "Mistral"
    )

async def _call_together(prompt: str, system: str, temperature: float) -> str:
    return await _call_openai_style(
        TOGETHER_URL, TOGETHER_API_KEY, TOGETHER_MODEL,
        prompt, system, temperature, "Together"
    )

async def _call_openrouter(prompt: str, system: str, temperature: float) -> str:
    return await _call_openai_style(
        OPENROUTER_URL, OPENROUTER_API_KEY, OPENROUTER_MODEL,
        prompt, system, temperature, "OpenRouter"
    )


async def _call_best_available(prompt: str, system: str,
                                temperature: float, agent_name: str) -> str:
    """
    Пробует провайдеров по порядку.
    Mistral → Together → OpenRouter → ошибка с объяснением.
    """
    providers = []

    if MISTRAL_API_KEY:
        providers.append(("Mistral", _call_mistral))
    if TOGETHER_API_KEY:
        providers.append(("Together", _call_together))
    if OPENROUTER_API_KEY:
        providers.append(("OpenRouter", _call_openrouter))

    if not providers:
        raise ValueError(
            "Нет API ключей! Добавь в Railway Variables:\n"
            "MISTRAL_API_KEY — console.mistral.ai\n"
            "TOGETHER_API_KEY — together.ai\n"
            "OPENROUTER_API_KEY — openrouter.ai"
        )

    last_error = None
    for name, caller in providers:
        try:
            result = await caller(prompt, system, temperature)
            logger.info(f"{agent_name} → {name} ✅")
            return result
        except Exception as e:
            logger.warning(f"{agent_name} → {name} ❌ {e}")
            last_error = e

    raise RuntimeError(f"Все провайдеры недоступны. Последняя ошибка: {last_error}")


# ── Агенты ────────────────────────────────────────────────────────────────────

class AgentProvider:
    """
    Агенты с разными температурами — разные характеры.
    Провайдер выбирается автоматически по доступности.
    """

    async def bull(self, prompt: str, system: str = "",
                   temperature: float = None) -> str:
        t = temperature or AGENT_TEMPERATURE  # 0.7 — творческий
        return await _call_best_available(prompt, system, t, "🐂 Bull")

    async def bear(self, prompt: str, system: str = "",
                   temperature: float = None) -> str:
        t = (temperature or AGENT_TEMPERATURE) * 0.4  # 0.28 — холодный
        return await _call_best_available(prompt, system, t, "🐻 Bear")

    async def verifier(self, prompt: str, system: str = "",
                       temperature: float = None) -> str:
        t = 0.1  # минимум — факты
        return await _call_best_available(prompt, system, t, "🔍 Verifier")

    async def synth(self, prompt: str, system: str = "",
                    temperature: float = None) -> str:
        t = (temperature or AGENT_TEMPERATURE) * 0.6  # 0.42 — взвешенный
        return await _call_best_available(prompt, system, t, "⚖️ Synth")

    async def complete(self, prompt: str, system: str = "",
                       temperature: float = None) -> str:
        t = temperature or AGENT_TEMPERATURE
        return await _call_best_available(prompt, system, t, "🤖 General")


ai = AgentProvider()
