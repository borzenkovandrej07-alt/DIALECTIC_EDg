"""
auto_tracker.py — Автоматическая проверка прогнозов из всех дайджестов.
Парсит DIGEST_CACHE.md, извлекает прогнозы и проверяет по историческим ценам.
"""

import asyncio
import logging
import os
import re
import aiohttp
import json
import base64
import requests
from datetime import datetime, timedelta
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

BINANCE_URL = "https://api.binance.com/api/v3"
FNG_URL = "https://api.alternative.me/fng/"

GITHUB_REPO = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_PRICES_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/prices.json"
DIGEST_CACHE_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/DIGEST_CACHE.md"
AUTO_TRACK_FILE = "AUTO_TRACK.md"


def load_prices_from_github() -> dict:
    """Загрузить цены с GitHub."""
    if not GITHUB_TOKEN:
        return {}
    try:
        import requests
        resp = requests.get(
            GITHUB_PRICES_URL,
            headers={"Authorization": f"token {GITHUB_TOKEN}"},
            timeout=10
        )
        if resp.status_code == 200:
            import base64
            content = base64.b64decode(resp.json()["content"]).decode("utf-8")
            return json.loads(content)
    except Exception as e:
        logger.warning(f"Failed to load prices from GitHub: {e}")
    return {}


def save_prices_to_github(prices: dict):
    """Сохранить цены на GitHub."""
    if not GITHUB_TOKEN:
        logger.warning("No GITHUB_TOKEN - prices not saved")
        return
    try:
        import base64
        import requests
        
        content = json.dumps(prices, indent=2, ensure_ascii=False)
        
        resp = requests.get(GITHUB_PRICES_URL, headers={"Authorization": f"token {GITHUB_TOKEN}"}, timeout=10)
        sha = resp.json()["sha"] if resp.status_code == 200 else None
        
        data = {
            "message": "Auto-update historical prices",
            "content": base64.b64encode(content.encode()).decode(),
        }
        if sha:
            data["sha"] = sha
        
        resp = requests.put(GITHUB_PRICES_URL, headers={"Authorization": f"token {GITHUB_TOKEN}"}, json=data)
        if resp.status_code in (200, 201):
            logger.info("✅ Prices saved to GitHub")
    except Exception as e:
        logger.warning(f"Failed to save prices: {e}")


class PriceDB:
    """Работа с ценами (только GitHub)."""
    
    def __init__(self):
        self.prices = load_prices_from_github()
        logger.info(f"Loaded {len(self.prices)} prices from GitHub")
    
    def get_price(self, symbol: str, date: str) -> Optional[dict]:
        key = f"{symbol.upper()}_{date}"
        return self.prices.get(key)
    
    def save_price(self, symbol: str, date: str, price: float, change: float = 0):
        key = f"{symbol.upper()}_{date}"
        self.prices[key] = {"price": price, "change": change}


class PriceFetcher:
    """Сборщик цен с историей."""
    
    def __init__(self, price_db: PriceDB):
        self.db = price_db
        self.cache = {}
    
    async def get_historical_price(self, symbol: str, date: str) -> Optional[dict]:
        """Получить цену на дату (из БД или API)."""
        symbol_upper = symbol.upper().replace(" ", "")
        
        price = self.db.get_price(symbol_upper, date)
        if price:
            logger.info(f"DB price {symbol} {date}: {price}")
            return price
        
        price = await self._fetch_historical_from_yahoo(symbol_upper, date)
        if price:
            self.db.save_price(symbol_upper, date, price["price"], price.get("change", 0))
            logger.info(f"Fetched and saved {symbol} {date}: {price}")
            return price
        
        return None
    
    async def _fetch_historical_from_yahoo(self, symbol: str, date: str) -> Optional[dict]:
        """Скачать историческую цену с Yahoo или CoinGecko."""
        yahoo_map = {
            "VIX": "^VIX",
            "S&P": "^GSPC", "SPX": "^GSPC",
            "NDX": "^NDX", "NASDAQ": "^NDX",
            "GOLD": "GC=F", "XAU": "GC=F",
            "WTI": "CL=F", "CL": "CL=F", "OIL": "CL=F",
            "НЕФТ": "CL=F", "НЕФТЬ": "CL=F",
        }
        coingecko_map = {
            "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana",
        }
        
        ticker = yahoo_map.get(symbol)
        cg_id = coingecko_map.get(symbol)
        
        try:
            date_obj = datetime.strptime(date, "%d.%m.%Y")
            
            # CoinGecko для крипты
            if cg_id:
                async with aiohttp.ClientSession() as session:
                    ts = int(date_obj.timestamp())
                    url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/history?date={date_obj.strftime('%d-%m-%Y')}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            price = data.get("market_data", {}).get("current_price", {}).get("usd")
                            if price:
                                return {"price": price, "change": 0}
            
            # Yahoo для остального
            if not ticker:
                return None
            
            async with aiohttp.ClientSession() as session:
                for offset in [0, 1, -1]:
                    target_date = date_obj + timedelta(days=offset)
                    period_start = int((target_date - timedelta(days=5)).timestamp())
                    period_end = int((target_date + timedelta(days=1)).timestamp())
                    
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                    params = {"period1": period_start, "period2": period_end, "interval": "1d"}
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                    
                    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            result = data.get("chart", {}).get("result", [])
                            if result and result[0].get("timestamp"):
                                timestamps = result[0]["timestamp"]
                                closes = result[0]["indicators"]["quote"][0]["close"]
                                
                                for ts, close in zip(timestamps, closes):
                                    if close is not None:
                                        dt = datetime.fromtimestamp(ts)
                                        if abs((dt.date() - target_date.date()).days) <= 1:
                                            return {"price": close, "change": 0}
        except Exception as e:
            logger.warning(f"Historical price error {symbol} {date}: {e}")
        
        return None
    
    async def get_current_price(self, symbol: str) -> Optional[dict]:
        """Текущая цена (кэш)."""
        if symbol in self.cache:
            return self.cache[symbol]
        
        binance_map = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT", "SOL": "SOLUSDT"}
        yahoo_map = {
            "VIX": "^VIX", "S&P": "^GSPC", "SPX": "^GSPC",
            "NDX": "^NDX", "GOLD": "GC=F", "XAU": "GC=F",
            "WTI": "CL=F", "CL": "CL=F", "НЕФТ": "CL=F", "НЕФТЬ": "CL=F",
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                if symbol.upper() in binance_map:
                    url = f"{BINANCE_URL}/ticker/24hr"
                    async with session.get(url, params={"symbol": binance_map[symbol.upper()]}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            result = {"price": float(data["lastPrice"]), "change": float(data["priceChangePercent"])}
                            self.cache[symbol] = result
                            return result
                
                ticker = yahoo_map.get(symbol.upper())
                if ticker:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                    async with session.get(url, params={"interval": "1d", "range": "5d"}, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            result = data.get("chart", {}).get("result", [])
                            if result:
                                meta = result[0].get("meta", {})
                                price = meta.get("regularMarketPrice", 0)
                                if price > 0:
                                    result = {"price": price, "change": meta.get("regularMarketChangePercent", 0)}
                                    self.cache[symbol] = result
                                    return result
        except Exception as e:
            logger.warning(f"Current price error {symbol}: {e}")
        
        return None
    
    async def get_fear_greed(self, date: str = None) -> Optional[dict]:
        """Fear & Greed — только текущий (API не даёт историю)."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(FNG_URL, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {"value": int(data["data"][0]["value"]), "classification": data["data"][0]["value_classification"]}
        except Exception as e:
            logger.warning(f"F&G error: {e}")
        return None


class DigestParser:
    """Парсит все дайджесты из DIGEST_CACHE.md."""
    
    @staticmethod
    def extract_all_digests(text: str) -> list[dict]:
        digests = []
        pattern = r'## 📊 (\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?)'
        matches = list(re.finditer(pattern, text))
        
        for i, match in enumerate(matches):
            date_str = match.group(1)
            start = match.end()
            end = matches[i+1].start() if i+1 < len(matches) else len(text)
            digests.append({"date": date_str, "content": text[start:end].strip()})
        
        return digests
    
    @staticmethod
    def extract_forecasts(digest: dict) -> list[dict]:
        forecasts = []
        content = digest["content"]
        date = digest["date"]
        
        lines = content.split('\n')
        
        verdict_direction = None
        for marker in ["**ВЕРДИКТ:**", "ВЕРДИКТ:"]:
            idx = content.find(marker)
            if idx != -1:
                snippet = content[idx:idx+400].upper()
                if "БЫЧ" in snippet or "BUY" in snippet or "LONG" in snippet or "🐂" in snippet or "🟢" in snippet:
                    verdict_direction = "BULLISH"
                elif "МЕДВ" in snippet or "SELL" in snippet or "SHORT" in snippet or "🐻" in snippet or "🔴" in snippet:
                    verdict_direction = "BEARISH"
                break
        
        price_patterns = [
            (r'VIX\s*[:=]*\s*(\d+\.?\d*)', "VIX"),
            (r'S[&]?P\s*500?\s*[:=]*\s*(\d+\.?\d*)', "S&P"),
            (r'SPX\s*[:=]*\s*(\d+\.?\d*)', "S&P"),
            (r'(?:Нефть|WTI)\s*[:=]*\s*\$?(\d+\.?\d*)', "Нефть"),
            (r'(?:Gold|Золото|XAU)\s*[:=]*\s*\$?(\d+\.?\d*)', "Gold"),
            (r'Fear\s*&\s*Greed\s*[:=]*\s*(\d+)', "Fear&Greed"),
            (r'BTC\s*[:$=]\s*([\d,]+\.?\d*)', "BTC"),
            (r'ETH\s*[:$=]\s*([\d,]+\.?\d*)', "ETH"),
            (r'BNB\s*[:$=]\s*([\d,]+\.?\d*)', "BNB"),
            (r'SOL\s*[:$=]\s*([\d,]+\.?\d*)', "SOL"),
        ]
        
        direction_patterns = [
            (r'BTC\s*[🐻🐂🟡→]*\s*(МЕДВЕЖ[ИЙ]|BEARISH|BULLISH|быч[ий]|медвеж[ий]|NEUTRAL|CASH|LONG|SHORT)', "BTC"),
            (r'ETH\s*[🐻🐂🟡→]*\s*(МЕДВЕЖ[ИЙ]|BEARISH|BULLISH|быч[ий]|медвеж[ий]|NEUTRAL|CASH|LONG|SHORT)', "ETH"),
            (r'BNB\s*[🐻🐂🟡→]*\s*(МЕДВЕЖ[ИЙ]|BEARISH|BULLISH|быч[ий]|медвеж[ий]|NEUTRAL|CASH|LONG|SHORT)', "BNB"),
            (r'SOL\s*[🐻🐂🟡→]*\s*(МЕДВЕЖ[ИЙ]|BEARISH|BULLISH|быч[ий]|медвеж[ий]|NEUTRAL|CASH|LONG|SHORT)', "SOL"),
        ]
        
        seen = set()
        
        for line in lines:
            line = line.strip()
            
            for pattern, asset in direction_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    direction = match.group(1).upper()
                    if "БЫЧ" in direction or "BULL" in direction or "LONG" in direction:
                        direction = "BULLISH"
                    elif "МЕДВ" in direction or "BEAR" in direction or "SHORT" in direction:
                        direction = "BEARISH"
                    elif "НЕЙТРАЛЬ" in direction or "NEUTRAL" in direction or "CASH" in direction:
                        direction = "NEUTRAL"
                    
                    key = f"{asset}:{direction}:{date}"
                    if key not in seen:
                        seen.add(key)
                        forecasts.append({
                            "date": date, "type": "Daily Digest",
                            "asset": asset, "forecast": direction, "forecast_type": "direction"
                        })
                    break
        
        if verdict_direction:
            key = f"VERDICT:{verdict_direction}:{date}"
            if key not in seen:
                seen.add(key)
                forecasts.append({
                    "date": date, "type": "Daily Digest",
                    "asset": "VERDICT", "forecast": verdict_direction, "forecast_type": "direction"
                })
        
        for pattern, asset in price_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    price = match.group(1)
                    asset_norm = asset.upper()
                    if "S&P" in asset_norm: asset_norm = "S&P"
                    if "НЕФТ" in asset_norm: asset_norm = "Нефть"
                    asset_norm = asset_norm.rstrip("Ь")
                    key = f"{asset_norm}:price:{price}:{date}"
                    if key not in seen:
                        seen.add(key)
                        forecasts.append({
                            "date": date, "type": "Daily Digest",
                            "asset": asset_norm, "forecast": price, "forecast_type": "price"
                        })
                    break
        
        return forecasts


class ResultChecker:
    def __init__(self, price_fetcher: PriceFetcher):
        self.fetcher = price_fetcher
    
    async def check_forecast(self, forecast: dict) -> dict:
        asset = forecast["asset"]
        forecast_val = forecast["forecast"]
        ftype = forecast["forecast_type"]
        date = forecast["date"]
        
        price_data = None
        current_price = None
        change = None
        
        asset_upper = asset.upper()
        
        if "FEAR" in asset_upper or "GREED" in asset_upper:
            price_data = await self.fetcher.get_fear_greed(date)
            if price_data:
                current_price = price_data.get("value", 0)
        else:
            price_data = await self.fetcher.get_historical_price(asset, date)
            if not price_data:
                price_data = await self.fetcher.get_current_price(asset)
            
            if price_data:
                current_price = price_data.get("price", 0)
                change = price_data.get("change", 0)
        
        if current_price is None or current_price == 0:
            return {"result": "⚠️ Нет цены", "accuracy": "—", "fact": "—"}
        
        if ftype == "price":
            try:
                forecast_num = float(forecast_val)
            except:
                return {"result": "⚠️ Ошибка парсинга", "accuracy": "—"}
            
            diff_pct = abs((current_price - forecast_num) / forecast_num * 100) if forecast_num > 0 else 100
            
            if diff_pct < 1:
                return {"result": "✅ Точно", "accuracy": "100%", "fact": f"{current_price:.2f}"}
            elif diff_pct < 3:
                return {"result": "✅ Верно", "accuracy": "95%", "fact": f"{current_price:.2f}"}
            elif diff_pct < 5:
                return {"result": "⚠️ Близко", "accuracy": "80%", "fact": f"{current_price:.2f}"}
            else:
                return {"result": "❌ Неверно", "accuracy": "0%", "fact": f"{current_price:.2f}"}
        
        else:
            if not change:
                return {"result": "⚠️ Нет данных", "accuracy": "—", "fact": "—"}
            
            forecast_dir = forecast_val.upper()
            
            if "BULL" in forecast_dir or "БЫЧ" in forecast_dir or "LONG" in forecast_dir:
                if change > 0.5:
                    return {"result": "✅ Верно", "accuracy": "100%", "fact": f"{change:+.2f}%"}
                elif change < -0.5:
                    return {"result": "❌ Неверно", "accuracy": "0%", "fact": f"{change:+.2f}%"}
                else:
                    return {"result": "⚠️ Смешанный", "accuracy": "50%", "fact": f"{change:+.2f}%"}
            
            elif "BEAR" in forecast_dir or "МЕДВ" in forecast_dir or "SHORT" in forecast_dir:
                if change < -0.5:
                    return {"result": "✅ Верно", "accuracy": "100%", "fact": f"{change:+.2f}%"}
                elif change > 0.5:
                    return {"result": "❌ Неверно", "accuracy": "0%", "fact": f"{change:+.2f}%"}
                else:
                    return {"result": "⚠️ Смешанный", "accuracy": "50%", "fact": f"{change:+.2f}%"}
            
            elif "NEUTRAL" in forecast_dir or "CASH" in forecast_dir:
                if abs(change) <= 2:
                    return {"result": "✅ Верно", "accuracy": "100%", "fact": f"{change:+.2f}% (боковик)"}
                else:
                    return {"result": "⚠️ Близко", "accuracy": "50%", "fact": f"{change:+.2f}%"}
            
            return {"result": "⚠️ Неизвестно", "accuracy": "—", "fact": "—"}


async def main():
    tracker = AutoTracker()
    results = await tracker.check_all_forecasts()
    if results:
        md = tracker.generate_markdown(results)
        await tracker.upload_to_github(md, AUTO_TRACK_FILE)
        logger.info(f"✅ AUTO_TRACK.md обновлён")


class AutoTracker:
    """Класс для авто-проверки прогнозов, совместимый со scheduler.py."""
    
    def __init__(self):
        self.db = PriceDB()
        self.fetcher = PriceFetcher(self.db)
        self.checker = ResultChecker(self.fetcher)
    
    async def _fetch_digest_cache(self) -> str:
        """Скачать DIGEST_CACHE.md с GitHub."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(DIGEST_CACHE_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        return await resp.text()
        except Exception as e:
            logger.warning(f"Failed to fetch DIGEST_CACHE.md: {e}")
        return ""
    
    async def check_all_forecasts(self) -> list:
        """Проверить все прогнозы из всех дайджестов."""
        cache_text = await self._fetch_digest_cache()
        if not cache_text:
            logger.warning("DIGEST_CACHE.md пуст или недоступен")
            return []
        
        digests = DigestParser.extract_all_digests(cache_text)
        logger.info(f"Найдено дайджестов: {len(digests)}")
        
        all_forecasts = []
        for digest in digests:
            forecasts = DigestParser.extract_forecasts(digest)
            all_forecasts.extend(forecasts)
        
        logger.info(f"Найдено прогнозов: {len(all_forecasts)}")
        
        results = []
        for forecast in all_forecasts:
            check = await self.checker.check_forecast(forecast)
            results.append({**forecast, **check})
        
        results.sort(key=lambda x: x["date"], reverse=True)
        return results
    
    def generate_markdown(self, results: list) -> str:
        """Сгенерировать AUTO_TRACK.md в формате 1:1 с FORECASTS.md."""
        total = len(results)
        wins = sum(1 for r in results if "✅" in r.get("result", ""))
        losses = sum(1 for r in results if "❌" in r.get("result", ""))
        pending = sum(1 for r in results if "⚠" in r.get("result", "") or "Нет" in r.get("result", ""))
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        
        # Open predictions (those with ⚠ or pending)
        open_preds = [r for r in results if "⚠" in r.get("result", "") or "Нет" in r.get("result", "") or "Неизвестно" in r.get("result", "")]
        # Closed predictions (✅ or ❌)
        closed_preds = [r for r in results if "✅" in r.get("result", "") or "❌" in r.get("result", "")]
        
        # Accuracy by asset
        by_asset = {}
        for r in results:
            asset = r.get("asset", "Unknown")
            if asset not in by_asset:
                by_asset[asset] = {"calls": 0, "wins": 0, "losses": 0}
            by_asset[asset]["calls"] += 1
            if "✅" in r.get("result", ""):
                by_asset[asset]["wins"] += 1
            elif "❌" in r.get("result", ""):
                by_asset[asset]["losses"] += 1
        
        lines = [
            "# 📊 Dialectic Edge — Auto Track Record",
            "",
            f"> Последнее обновление: {now}",
            "> Автоматический трекинг точности прогнозов.",
            "> ⚠️ Не является финансовым советом. DYOR.",
            "",
            "---",
            "## 🎯 Общая статистика",
            "",
            "| Метрика | Значение |",
            "|---------|----------|",
            f"| Всего прогнозов | {total} |",
            f"| ✅ Прибыльных | {wins} |",
            f"| ❌ Убыточных | {losses} |",
            f"| ⏳ Открытых | {pending} |",
            f"| 🎯 Точность | **{win_rate:.1f}%** |",
            "",
            "---",
        ]
        
        if open_preds:
            lines += [
                "## ⏳ Открытые прогнозы",
                "",
                "| Актив | Тип | Прогноз | Факт | Результат | Дата |",
                "|-------|-----|---------|------|-----------|------|",
            ]
            for p in open_preds[:20]:
                fact = p.get("fact", "—") or "—"
                date = p.get("date", "—")
                lines.append(
                    f"| {p['asset']} | {p.get('type', '—')} | {p['forecast']} | {fact} | {p['result']} | {date} |"
                )
            lines += ["", "---"]
        
        if closed_preds:
            lines += [
                "## 📋 Последние закрытые прогнозы",
                "",
                "| Дата | Актив | Тип | Прогноз | Факт | Результат | Точность |",
                "|------|-------|-----|---------|------|-----------|----------|",
            ]
            for r in closed_preds[:30]:
                fact = r.get("fact", "—") or "—"
                acc = r.get("accuracy", "—") or "—"
                lines.append(
                    f"| {r['date']} | {r['asset']} | {r.get('type', '—')} | {r['forecast']} | {fact} | {r['result']} | {acc} |"
                )
            lines += ["", "---"]
        
        if by_asset:
            lines += [
                "## 🏆 Точность по активам",
                "",
                "| Актив | Сигналов | Побед | Точность |",
                "|-------|----------|-------|----------|",
            ]
            for asset, stats in sorted(by_asset.items()):
                wr = (stats['wins'] / max(stats['wins'] + stats['losses'], 1) * 100) if (stats['wins'] + stats['losses']) > 0 else 0
                lines.append(f"| {asset} | {stats['calls']} | {stats['wins']} | {wr:.0f}% |")
            lines += ["", "---"]
        
        lines += [
            "## ℹ️ О проекте",
            "",
            "**Dialectic Edge** — мультиагентная система финансового анализа.",
            "4 AI-модели: Bull (Groq/Llama), Bear (Mistral), Verifier, Synth (Mistral Large).",
            "",
            "---",
            "*Прошлая точность не гарантирует будущих результатов.*",
        ]
        
        return "\n".join(lines)
    
    async def upload_to_github(self, content: str, filename: str) -> bool:
        """Загрузить файл на GitHub."""
        if not GITHUB_TOKEN:
            logger.warning("No GITHUB_TOKEN — не могу загрузить на GitHub")
            return False
        
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
        
        try:
            # Get current SHA
            resp = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, timeout=10)
            sha = resp.json().get("sha") if resp.status_code == 200 else None
            
            data = {
                "message": f"📊 Update {filename} {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]",
                "content": base64.b64encode(content.encode()).decode(),
            }
            if sha:
                data["sha"] = sha
            
            resp = requests.put(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, json=data, timeout=10)
            if resp.status_code in (200, 201):
                logger.info(f"✅ {filename} обновлён на GitHub")
                return True
            else:
                logger.warning(f"GitHub upload failed: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            logger.warning(f"GitHub upload error: {e}")
            return False


if __name__ == "__main__":
    asyncio.run(main())