"""AI Provider Implementations - OpenRouter, Groq, Together, Mistral with Fallback Chain"""

import asyncio
import logging
from typing import Optional, AsyncGenerator, Any, Dict, List
from dataclasses import dataclass

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

logger = logging.getLogger(__name__)

if not AIOHTTP_AVAILABLE:
    logger.warning("aiohttp not installed. AI providers will not work.")


@dataclass
class AIMessage:
    """Структурированное AI сообщение"""
    role: str  # "user", "assistant", "system"
    content: str


@dataclass
class AIResponse:
    """Ответ от AI провайдера"""
    content: str
    model: str
    tokens_used: int
    provider_name: str


class OpenRouterProvider:
    """OpenRouter - главный провайдер с доступом к лучшим моделям"""

    def __init__(
        self,
        api_key: str,
        model: str = "openrouter/auto",
        base_url: str = "https://openrouter.ai/api/v1",
        timeout: int = 60,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def generate(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> AIResponse:
        """Генерация ответа через OpenRouter"""
        await self.initialize()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": "https://github.com/dialectic-edge",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]
                    tokens = data.get("usage", {}).get("total_tokens", 0)

                    return AIResponse(
                        content=content,
                        model=self.model,
                        tokens_used=tokens,
                        provider_name="OpenRouter",
                    )
                else:
                    logger.error(f"OpenRouter HTTP {resp.status}")
                    raise Exception(f"OpenRouter error: {resp.status}")

        except asyncio.TimeoutError:
            raise Exception("OpenRouter timeout")
        except Exception as e:
            logger.error(f"OpenRouter error: {e}")
            raise

    async def stream(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> AsyncGenerator[str, None]:
        """Потоковая генерация через OpenRouter"""
        await self.initialize()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": "https://github.com/dialectic-edge",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status != 200:
                    raise Exception(f"OpenRouter streaming error: {resp.status}")

                async for line in resp.content:
                    line = line.decode("utf-8").strip()
                    if line.startswith("data: "):
                        data_str = line[6:]
                        if data_str == "[DONE]":
                            break
                        try:
                            data = eval(data_str)
                            delta = data.get("choices", [{}])[0].get("delta", {})
                            if "content" in delta:
                                yield delta["content"]
                        except Exception:
                            continue

        except asyncio.TimeoutError:
            raise Exception("OpenRouter stream timeout")
        except Exception as e:
            logger.error(f"OpenRouter stream error: {e}")
            raise


class GroqProvider:
    """Groq - быстрый провайдер для малых моделей"""

    def __init__(
        self,
        api_key: str,
        model: str = "mixtral-8x7b-32768",
        base_url: str = "https://api.groq.com/openai/v1",
        timeout: int = 60,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def generate(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> AIResponse:
        """Генерация ответа через Groq"""
        await self.initialize()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]
                    tokens = data.get("usage", {}).get("total_tokens", 0)

                    return AIResponse(
                        content=content,
                        model=self.model,
                        tokens_used=tokens,
                        provider_name="Groq",
                    )
                else:
                    raise Exception(f"Groq error: {resp.status}")

        except asyncio.TimeoutError:
            raise Exception("Groq timeout")
        except Exception as e:
            logger.error(f"Groq error: {e}")
            raise


class TogetherProvider:
    """Together AI - децентрализованная вычислительная сеть"""

    def __init__(
        self,
        api_key: str,
        model: str = "meta-llama/Llama-2-70b-chat-hf",
        base_url: str = "https://api.together.xyz/v1",
        timeout: int = 60,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def generate(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> AIResponse:
        """Генерация ответа через Together"""
        await self.initialize()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]
                    tokens = data.get("usage", {}).get("total_tokens", 0)

                    return AIResponse(
                        content=content,
                        model=self.model,
                        tokens_used=tokens,
                        provider_name="Together",
                    )
                else:
                    raise Exception(f"Together error: {resp.status}")

        except asyncio.TimeoutError:
            raise Exception("Together timeout")
        except Exception as e:
            logger.error(f"Together error: {e}")
            raise


class MistralProvider:
    """Mistral - европейский провайдер высокого качества"""

    def __init__(
        self,
        api_key: str,
        model: str = "mistral-large",
        base_url: str = "https://api.mistral.ai/v1",
        timeout: int = 60,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def generate(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> AIResponse:
        """Генерация ответа через Mistral"""
        await self.initialize()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]
                    tokens = data.get("usage", {}).get("total_tokens", 0)

                    return AIResponse(
                        content=content,
                        model=self.model,
                        tokens_used=tokens,
                        provider_name="Mistral",
                    )
                else:
                    raise Exception(f"Mistral error: {resp.status}")

        except asyncio.TimeoutError:
            raise Exception("Mistral timeout")
        except Exception as e:
            logger.error(f"Mistral error: {e}")
            raise


class AIProviderChain:
    """
    Fallback chain для AI провайдеров.
    Пытается каждый провайдер в порядке приоритета.
    """

    def __init__(
        self,
        providers: Optional[List[Any]] = None,
        api_keys: Optional[Dict[str, str]] = None,
    ):
        """
        providers: список провайдеров (или создадим из api_keys)
        api_keys: словарь {provider_name -> api_key}
        """
        if providers:
            self.providers = providers
        elif api_keys:
            self.providers = []
            if "openrouter" in api_keys:
                self.providers.append(
                    OpenRouterProvider(api_key=api_keys["openrouter"])
                )
            if "groq" in api_keys:
                self.providers.append(GroqProvider(api_key=api_keys["groq"]))
            if "together" in api_keys:
                self.providers.append(TogetherProvider(api_key=api_keys["together"]))
            if "mistral" in api_keys:
                self.providers.append(MistralProvider(api_key=api_keys["mistral"]))
        else:
            self.providers = []

    async def generate(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
        fallback: bool = True,
    ) -> AIResponse:
        """
        Генерация с автоматическим fallback.
        fallback=False - выбросит ошибку при первом отказе.
        """
        errors = []

        for i, provider in enumerate(self.providers):
            try:
                logger.info(
                    f"Trying provider {i+1}/{len(self.providers)}: {provider.__class__.__name__}"
                )
                response = await provider.generate(messages, temperature, max_tokens)
                logger.info(f"✅ Success with {provider.__class__.__name__}")
                return response

            except Exception as e:
                error_msg = f"{provider.__class__.__name__}: {str(e)}"
                errors.append(error_msg)
                logger.warning(f"⚠️  Provider failed: {error_msg}")

                if not fallback:
                    raise

                # Пробуем следующего
                if i < len(self.providers) - 1:
                    logger.info("Falling back to next provider...")
                    await asyncio.sleep(0.5)
                    continue
                else:
                    # Последний провайдер - нет куда падать
                    break

        # Все провайдеры исчерпаны
        error_summary = "\n".join(errors)
        logger.error(f"❌ All providers failed:\n{error_summary}")
        raise Exception(f"All AI providers failed:\n{error_summary}")

    async def stream(
        self,
        messages: List[AIMessage],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> AsyncGenerator[str, None]:
        """Потоковая генерация с fallback"""
        errors = []

        for i, provider in enumerate(self.providers):
            # Проверяем есть ли метод stream
            if not hasattr(provider, "stream"):
                continue

            try:
                logger.info(
                    f"Streaming from provider {i+1}/{len(self.providers)}: {provider.__class__.__name__}"
                )
                async for chunk in provider.stream(messages, temperature, max_tokens):
                    yield chunk
                logger.info(f"✅ Stream success with {provider.__class__.__name__}")
                return

            except Exception as e:
                error_msg = f"{provider.__class__.__name__}: {str(e)}"
                errors.append(error_msg)
                logger.warning(f"⚠️  Provider stream failed: {error_msg}")
                await asyncio.sleep(0.5)
                continue

        # Все провайдеры исчерпаны - используем fallback на обычный generate
        logger.warning("Falling back to non-streaming generation...")
        response = await self.generate(messages, temperature, max_tokens)
        yield response.content

    async def close_all(self) -> None:
        """Закрытие всех сессий"""
        for provider in self.providers:
            if hasattr(provider, "close"):
                await provider.close()

    def add_provider(self, provider: Any) -> None:
        """Добавление провайдера в цепь"""
        self.providers.append(provider)

    def __repr__(self) -> str:
        providers_names = [p.__class__.__name__ for p in self.providers]
        return f"AIProviderChain({' → '.join(providers_names)})"
