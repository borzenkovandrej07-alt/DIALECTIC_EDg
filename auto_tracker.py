"""
auto_tracker.py — Автоматическая проверка прогнозов и обновление Track Record.

Логика:
1. Парсит прогнозы из DIGEST_CACHE (последний /daily)
2. Собирает актуальные цены (Binance + Yahoo)
3. Определяет результат:
   - Простые случаи → автоматически
   - Смешанные → нейросеть (Groq/Mistral)
4. Загружает на GitHub
"""

import asyncio
import logging
import os
import re
import aiohttp
from datetime import datetime
from typing import Optional

# Загружаем .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# API URLs
BINANCE_URL = "https://api.binance.com/api/v3"
FNG_URL = "https://api.alternative.me/fng/"
GITHUB_RAW_URL = "https://raw.githubusercontent.com/{repo}/main/DIGEST_CACHE.md"
GITHUB_REPO = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")

# Провайдеры LLM для смешанных случаев
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")


class PriceFetcher:
    """Сборщик цен с рынков."""
    
    def __init__(self):
        self.crypto_map = {
            "BTC": "BTCUSDT",
            "ETH": "ETHUSDT",
            "BTCUSDT": "BTCUSDT",
            "ETHUSDT": "ETHUSDT",
        }
        self.stock_map = {
            "S&P": "^GSPC",
            "SPX": "^GSPC",
            "S&P500": "^GSPC",
            "Nasdaq": "^NDX",
            "NDX": "^NDX",
            "VIX": "^VIX",
            "WTI": "CL=F",
            "НЕФТЬ": "CL=F",
            "НЕФТ": "CL=F",
            "GOLD": "GC=F",
            "ЗОЛОТ": "GC=F",
        }
    
    async def get_crypto_price(self, symbol: str) -> Optional[dict]:
        """Цена с Binance."""
        pair = self.crypto_map.get(symbol.upper(), symbol.upper() + "USDT")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BINANCE_URL}/ticker/24hr",
                    params={"symbol": pair},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {
                            "price": float(data["lastPrice"]),
                            "change": float(data["priceChangePercent"]),
                            "high": float(data["highPrice"]),
                            "low": float(data["lowPrice"]),
                        }
        except Exception as e:
            logger.warning(f"Binance error {symbol}: {e}")
        return None
    
    # Заголовки для Yahoo
    YAHOO_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    async def get_stock_price(self, symbol: str) -> Optional[dict]:
        """Цена с Binance (фьючерсы) или Yahoo."""
        
        # Binance фьючерсы для металлов и нефти
        binance_map = {
            "GOLD": "XAUUSDT",  # Золото
            "ЗОЛОТ": "XAUUSDT",
            "XAU": "XAUUSDT",
            "НЕФТ": "OILUSDT",  # Нефть
            "НЕФТЬ": "OILUSDT",
            "WTI": "OILUSDT",
            "CL": "OILUSDT",
            "BTC": "BTCUSDT",
            "ETH": "ETHUSDT",
        }
        
        ticker = self.stock_map.get(symbol.upper(), symbol)
        if not ticker:
            ticker = symbol.upper().replace(" ", "")
        
        # Пробуем Binance если есть маппинг
        binance_pair = binance_map.get(ticker.upper())
        if binance_pair:
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"{BINANCE_URL}/ticker/24hr"
                    async with session.get(
                        url, params={"symbol": binance_pair},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return {
                                "price": float(data["lastPrice"]),
                                "change": float(data["priceChangePercent"]),
                            }
            except Exception as e:
                logger.warning(f"Binance error {binance_pair}: {e}")
        
        # Yahoo как fallback (с заголовками)
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                async with session.get(
                    url,
                    params={"interval": "1d", "range": "5d"},
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get("chart", {}).get("result", [])
                        if result:
                            meta = result[0].get("meta", {})
                            return {
                                "price": meta.get("regularMarketPrice", 0),
                                "change": meta.get("regularMarketChangePercent", 0),
                            }
        except Exception as e:
            pass
        
        return None
    
    async def get_fear_greed(self) -> Optional[dict]:
        """Fear & Greed Index."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(FNG_URL, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {
                            "value": int(data["data"][0]["value"]),
                            "classification": data["data"][0]["value_classification"]
                        }
        except Exception as e:
            logger.warning(f"F&Greed error: {e}")
        return None
    
    async def get_all_prices(self, assets: list[str]) -> dict:
        """Получить все цены."""
        prices = {}
        
        # Уникальные активы
        unique_assets = list(set(assets))
        
        # Параллельно
        tasks = []
        asset_list = []
        for asset in unique_assets:
            asset_clean = asset.upper().replace(" ", "")
            # Крипта - через Binance
            if asset_clean in self.crypto_map or "USDT" in asset_clean:
                tasks.append(self.get_crypto_price(asset))
                asset_list.append(asset.upper())
            else:
                # Остальное - через get_stock_price
                tasks.append(self.get_stock_price(asset))
                asset_list.append(asset.upper())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for asset, result in zip(asset_list, results):
            if isinstance(result, dict):
                prices[asset] = result
                logger.info(f"Got price for {asset}: {result}")
            else:
                logger.warning(f"No price for {asset}: {result}")
        
        # F&G
        fng = await self.get_fear_greed()
        if fng:
            prices["Fear&Greed"] = fng
            logger.info(f"Got F&G: {fng}")
        
        logger.info(f"All prices: {prices.keys()}")
        return prices


class ForecastParser:
    """Парсит прогнозы из текста отчёта."""
    
    @staticmethod
    def extract_forecasts(report_text: str) -> list[dict]:
        """Извлекает прогнозы из текста."""
        forecasts = []
        
        # Паттерны для разных типов прогнозов
        patterns = [
            # BTC/ETH с направлением: "BTC → медвежий" или "BTC 🐻 МЕДВЕЖИЙ"
            r"([BTCЕ]+TH?)\s*[→🐻🐂🟡]*\s*(МЕДВЕЖИЙ|BEARISH|BULLISH|быч[ий]|медвеж[ий]|NEUTRAL|CASH|LONG|SHORT)",
            # Точная цена: "VIX: 31.05" или "VIX = 31.05"
            r"(VIX)\s*[:=]\s*(\d+\.?\d*)",
            # S&P: "S&P 500: 6369"
            r"(S&P\s*\d*|SPX)\s*[:=]*\s*(\d+\.?\d*)",
            # Нефть/Золото с ценой
            r"(Нефть|WTI|Gold|Золото)\s*[:=]*\s*\$?(\d+\.?\d*)",
            # Fear & Greed
            r"(Fear\s*&\s*Greed|F&Greed)\s*[:=]*\s*(\d+)",
        ]
        
        lines = report_text.split('\n')
        for line in lines:
            line = line.strip()
            
            # Направление (стрелки/эмодзи)
            direction_match = re.search(
                r"([BTCЕ]+TH?)\s*[→🐻🐂🟡\s]*(МЕДВЕЖИЙ|BEARISH|BULLISH|быч[ий]|медвеж[ий]|NEUTRAL|CASH|LONG|SHORT)",
                line, re.IGNORECASE
            )
            if direction_match:
                asset = direction_match.group(1).upper()
                direction = direction_match.group(2).upper()
                
                forecasts.append({
                    "asset": asset,
                    "type": "direction",
                    "forecast": direction,
                    "line": line[:100]
                })
                continue
            
            # Точная цена
            price_match = re.search(r"(VIX|S&P|SPX|Нефть|WTI|Gold|Золото|Fear.*Greed).*?(\d+\.?\d*)", line, re.IGNORECASE)
            if price_match:
                asset = price_match.group(1)
                price = price_match.group(2)
                
                forecasts.append({
                    "asset": asset,
                    "type": "price",
                    "forecast": price,
                    "line": line[:100]
                })
        
        return forecasts


class ResultChecker:
    """Определяет результат прогноза."""
    
    def __init__(self, price_fetcher: PriceFetcher):
        self.fetcher = price_fetcher
    
    async def check_forecast(self, forecast: dict, current_prices: dict) -> dict:
        """Проверяет один прогноз."""
        asset = forecast["asset"]
        ftype = forecast["type"]
        forecast_value = forecast["forecast"]
        
        # Получаем текущую цену
        price_data = current_prices.get(asset.upper())
        if not price_data:
            return {"result": "Неизвестно", "accuracy": "—", "reason": "Нет цены"}
        
        current_price = price_data.get("price", 0)
        change = price_data.get("change", 0)
        
        if ftype == "direction":
            return self._check_direction(forecast_value, change, current_price)
        elif ftype == "price":
            return self._check_price(forecast_value, current_price)
        
        return {"result": "Неизвестно", "accuracy": "—"}
    
    def _check_direction(self, forecast: str, change: float, price: float) -> dict:
        """Проверка направления."""
        forecast = forecast.upper()
        
        # Бычий / LONG
        if "BULL" in forecast or "БЫЧ" in forecast or "LONG" in forecast:
            if change > 0.5:
                return {"result": "✅ Верно", "accuracy": "100%", "reason": f"Рост {change:+.2f}%"}
            elif change < -0.5:
                return {"result": "❌ Неверно", "accuracy": "0%", "reason": f"Падение {change:+.2f}%"}
            else:
                return {"result": "⚠️ Смешанный", "accuracy": "50%", "reason": "Боковик"}
        
        # Медвежий / SHORT
        elif "BEAR" in forecast or "МЕДВ" in forecast or "SHORT" in forecast:
            if change < -0.5:
                return {"result": "✅ Верно", "accuracy": "100%", "reason": f"Падение {change:+.2f}%"}
            elif change > 0.5:
                return {"result": "❌ Неверно", "accuracy": "0%", "reason": f"Рост {change:+.2f}%"}
            else:
                return {"result": "⚠️ Смешанный", "accuracy": "50%", "reason": "Боковик"}
        
        # Neutral / CASH
        elif "NEUTRAL" in forecast or "CASH" in forecast:
            if abs(change) < 2:
                return {"result": "✅ Верно", "accuracy": "100%", "reason": "Боковик предсказан"}
            else:
                return {"result": "⚠️ Смешанный", "accuracy": "50%", "reason": f"Волатильность {change:+.2f}%"}
        
        return {"result": "⚠️ Смешанный", "accuracy": "—"}
    
    def _check_price(self, forecast_price: str, current_price: float) -> dict:
        """Проверка точной цены."""
        try:
            forecast_val = float(forecast_price)
        except:
            return {"result": "⚠️ Смешанный", "accuracy": "—"}
        
        diff_pct = abs((current_price - forecast_val) / forecast_val * 100)
        
        if diff_pct < 1:
            return {"result": "✅ Точно", "accuracy": "100%", "reason": f"{current_price:.2f}"}
        elif diff_pct < 3:
            return {"result": "✅ Верно", "accuracy": "95%", "reason": f"{current_price:.2f}"}
        elif diff_pct < 5:
            return {"result": "⚠️ Близко", "accuracy": "80%", "reason": f"{current_price:.2f}"}
        else:
            return {"result": "⚠️ Смешанный", "accuracy": "50%", "reason": f"Прогноз: {forecast_val}, факт: {current_price:.2f}"}


class AIChecker:
    """Проверка смешанных случаев через нейросеть."""
    
    async def check_mixed(self, forecast: dict, current_prices: dict, verdict: str) -> dict:
        """Использует LLM для определения результата."""
        asset = forecast["asset"]
        ftype = forecast["type"]
        forecast_value = forecast["forecast"]
        
        price_data = current_prices.get(asset.upper(), {})
        current_price = price_data.get("price", 0)
        change = price_data.get("change", 0)
        
        prompt = f"""Ты эксперт по финансовым рынкам. Определи результат прогноза.

Прогноз из Dialectic Edge:
- Актив: {asset}
- Тип: {ftype}
- Прогноз: {forecast_value}
- Вердикт бота: {verdict}

Текущие данные рынка:
- Текущая цена: {current_price}
- Изменение за 24ч: {change:+.2f}%

Определи:
1. Результат: "Верно" (прогноз оправдался), "Неверно" (прогноз не оправдался), или "Смешанный" (частично верно)
2. Процент точности: 0-100%

Ответь в формате:
Результат: [Верно/Неверно/Смешанный]
Точность: [число]%"""
        
        # Пробуем Groq
        if GROQ_API_KEY:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {GROQ_API_KEY}",
                            "Content-Type": "application/json"
                        },
                        json={
                            "model": "llama-3.3-70b-versatile",
                            "messages": [{"role": "user", "content": prompt}],
                            "temperature": 0.3
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            text = data["choices"][0]["message"]["content"]
                            
                            # Парсим ответ
                            result = "⚠️ Смешанный"
                            accuracy = "50%"
                            
                            if "верно" in text.lower() and "неверно" not in text.lower():
                                result = "✅ Верно"
                            elif "неверно" in text.lower():
                                result = "❌ Неверно"
                            
                            acc_match = re.search(r"(\d+)%", text)
                            if acc_match:
                                accuracy = f"{acc_match.group(1)}%"
                            
                            return {"result": result, "accuracy": accuracy, "ai_used": True}
            except Exception as e:
                logger.warning(f"Groq error: {e}")
        
        # Fallback - спрашиваем человека
        return {"result": "❓ Требует проверки", "accuracy": "—", "ai_used": False}


class AutoTracker:
    """Главный класс для авто-трекинга."""
    
    def __init__(self):
        self.price_fetcher = PriceFetcher()
        self.result_checker = ResultChecker(self.price_fetcher)
        self.ai_checker = AIChecker()
    
    async def get_last_report(self) -> Optional[str]:
        """Получает последний отчёт из GitHub."""
        try:
            async with aiohttp.ClientSession() as session:
                url = GITHUB_RAW_URL.format(repo=GITHUB_REPO)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return await resp.text()
        except Exception as e:
            logger.warning(f"GitHub fetch error: {e}")
        return None
    
    async def get_verdict(self, report_text: str) -> str:
        """Извлекает вердикт из отчёта."""
        for line in report_text.split('\n'):
            line_upper = line.upper()
            if "ВЕРДИКТ" in line_upper or "VERDICT" in line_upper:
                if "БЫЧ" in line_upper or "BULL" in line_upper:
                    return "BULLISH"
                elif "МЕДВЕЖ" in line_upper or "BEAR" in line_upper:
                    return "BEARISH"
                elif "NEUTRAL" in line_upper or "CASH" in line_upper:
                    return "NEUTRAL"
        return "UNKNOWN"
    
    async def check_all_forecasts(self) -> list[dict]:
        """Проверяет все прогнозы."""
        # Получаем отчёт
        report = await self.get_last_report()
        if not report:
            logger.error("Не удалось получить отчёт")
            return []
        
        # Парсим прогнозы
        parser = ForecastParser()
        forecasts = parser.extract_forecasts(report)
        
        if not forecasts:
            logger.warning("Прогнозы не найдены")
            return []
        
        # Получаем текущие цены
        assets = list(set([f["asset"] for f in forecasts]))
        assets.extend(["BTC", "ETH", "VIX", "S&P", "Gold"])
        prices = await self.price_fetcher.get_all_prices(assets)
        
        # Вердикт
        verdict = await self.get_verdict(report)
        
        # Проверяем каждый прогноз
        results = []
        for forecast in forecasts:
            result = await self.result_checker.check_forecast(forecast, prices)
            
            # Если смешанный - спрашиваем AI
            if "Смешанный" in result.get("result", ""):
                ai_result = await self.ai_checker.check_mixed(forecast, prices, verdict)
                result = ai_result
            
            result.update(forecast)
            results.append(result)
        
        return results
    
    def generate_markdown(self, results: list[dict]) -> str:
        """Генерирует markdown таблицу."""
        lines = [
            f"# 📊 Dialectic Edge — Auto Track Record",
            f"> Автоматическая проверка: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "## 📝 Проверенные прогнозы",
            "",
            "| Актив | Тип | Прогноз | Результат | Точность |",
            "|------|-----|---------|-----------|----------|",
        ]
        
        for r in results:
            lines.append(
                f"| {r['asset']} | {r['type']} | {r['forecast']} | {r['result']} | {r.get('accuracy', '—')} |"
            )
        
        # Статистика
        total = len(results)
        correct = sum(1 for r in results if "✅" in r.get("result", ""))
        accuracy = (correct / total * 100) if total > 0 else 0
        
        lines.extend([
            "",
            "## 🎯 Статистика",
            "",
            f"- Всего прогнозов: **{total}**",
            f"- Верно: **{correct}**",
            f"- Точность: **{accuracy:.1f}%**",
            "",
            "> Автоматическая проверка. При сомнениях - проверь вручную."
        ])
        
        return "\n".join(lines)
    
    async def upload_to_github(self, content: str, filename: str = "AUTO_TRACK.md") -> bool:
        """Загружает результат на GitHub."""
        import base64
        import requests
        
        token = os.getenv("GITHUB_TOKEN")
        repo = GITHUB_REPO
        
        if not token:
            logger.warning("GITHUB_TOKEN не найден")
            return False
        
        try:
            # Получаем SHA файла если он существует
            url = f"https://api.github.com/repos/{repo}/contents/{filename}"
            headers = {"Authorization": f"token {token}"}
            
            sha = None
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                sha = resp.json()["sha"]
            
            # Загружаем файл
            data = {
                "message": f"Auto-update: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                "content": base64.b64encode(content.encode()).decode(),
            }
            if sha:
                data["sha"] = sha
            
            resp = requests.put(url, headers=headers, json=data)
            
            if resp.status_code in (200, 201):
                logger.info(f"✅ Загружено на GitHub: {filename}")
                return True
            else:
                logger.warning(f"GitHub error: {resp.status_code} - {resp.text}")
                return False
        except Exception as e:
            logger.warning(f"GitHub upload error: {e}")
            return False


async def main():
    """Запуск проверки."""
    tracker = AutoTracker()
    
    logger.info("🔄 Запускаю проверку прогнозов...")
    
    results = await tracker.check_all_forecasts()
    
    if results:
        md = tracker.generate_markdown(results)
        
        # Выводим в консоль
        print(md)
        
        # Загружаем на GitHub (если есть токен)
        token = os.getenv("GITHUB_TOKEN", "")
        if token:
            await tracker.upload_to_github(md, "AUTO_TRACK.md")
        else:
            logger.info("ℹ️ GITHUB_TOKEN не найден - загрузка пропущена (на Railway будет работать)")
        
        logger.info("✅ Проверка завершена")
    else:
        logger.error("Нет результатов")


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    asyncio.run(main())
