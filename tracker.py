"""
tracker.py — Автоматическая проверка прогнозов агентов.

ИСПРАВЛЕНО v2:
- extract_predictions_from_report полностью переписан.
  Старый regex искал "BTC: long от $96000, цель $105000, стоп $93000" —
  такой формат Synth не пишет никогда. Track record был всегда пустым.

  Новый парсер понимает реальный формат из SYNTH_SYSTEM:
    • Актив: BTC
    • Направление: LONG
    • Вход: $96,500
    • Цель: $105,000
    • Стоп: $93,000
  А также компактные варианты типа "BTC LONG $96500 → $105000 стоп $93000"
"""

import asyncio
import logging
import re
import os
from datetime import datetime, timedelta

from market_data import MarketDataFetcher
from database import (
    get_pending_predictions,
    update_prediction_result,
    save_prediction,
)

logger = logging.getLogger(__name__)
market = MarketDataFetcher()

ASSET_MAP = {
    "BTC":  ("crypto", "bitcoin"),
    "ETH":  ("crypto", "ethereum"),
    "SOL":  ("crypto", "solana"),
    "BNB":  ("crypto", "binancecoin"),
    "SPY":  ("stock",  "SPY"),
    "QQQ":  ("stock",  "QQQ"),
    "NVDA": ("stock",  "NVDA"),
    "AAPL": ("stock",  "AAPL"),
    "TSLA": ("stock",  "TSLA"),
    "GLD":  ("stock",  "GLD"),
}


# ─── Получение цены ───────────────────────────────────────────────────────────

async def get_current_price(asset: str) -> float | None:
    asset_upper = asset.upper()
    if asset_upper not in ASSET_MAP:
        return None
    asset_type, identifier = ASSET_MAP[asset_upper]

    try:
        import aiohttp
        if asset_type == "crypto":
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {"ids": identifier, "vs_currencies": "usd"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    return data.get(identifier, {}).get("usd")
        else:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{identifier}"
            headers = {"User-Agent": "Mozilla/5.0"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    return data["chart"]["result"][0]["meta"].get("regularMarketPrice")
    except Exception as e:
        logger.warning(f"Price fetch error for {asset}: {e}")
        return None


# ─── Проверка прогнозов ───────────────────────────────────────────────────────

async def check_pending_predictions():
    pending = await get_pending_predictions()
    if not pending:
        logger.info("Нет pending-прогнозов для проверки")
        return 0

    checked = 0
    for pred in pending:
        current_price = await get_current_price(pred["asset"])
        if current_price is None:
            continue

        entry     = pred["entry_price"]
        target    = pred["target_price"]
        stop      = pred["stop_loss"]
        direction = pred["direction"].upper()

        if not entry or not target or not stop:
            continue

        result  = "pending"
        pnl_pct = 0.0

        if direction == "LONG":
            if current_price >= target:
                result  = "win"
                pnl_pct = (target - entry) / entry * 100
            elif current_price <= stop:
                result  = "loss"
                pnl_pct = (stop - entry) / entry * 100
            else:
                pnl_pct = (current_price - entry) / entry * 100

        elif direction == "SHORT":
            if current_price <= target:
                result  = "win"
                pnl_pct = (entry - target) / entry * 100
            elif current_price >= stop:
                result  = "loss"
                pnl_pct = (entry - stop) / entry * 100
            else:
                pnl_pct = (entry - current_price) / entry * 100

        # Истёк ли таймфрейм?
        created = datetime.fromisoformat(pred["created_at"])
        tf = pred.get("timeframe", "1w")
        timeframe_days = {"1d": 1, "3d": 3, "1w": 7, "2w": 14, "1m": 30}
        max_days = timeframe_days.get(tf, 7)

        if (datetime.now() - created).days >= max_days and result == "pending":
            result = "win" if pnl_pct > 0 else "loss"

        if result in ("win", "loss"):
            await update_prediction_result(pred["id"], result, current_price, pnl_pct)
            checked += 1
            logger.info(
                f"Прогноз #{pred['id']} {pred['asset']} {direction}: "
                f"{result} | P&L: {pnl_pct:+.1f}%"
            )

        await asyncio.sleep(0.5)

    logger.info(f"Проверено прогнозов: {checked}/{len(pending)}")
    return checked


# ─── Парсер прогнозов (ПЕРЕПИСАН) ────────────────────────────────────────────

def _parse_price(raw: str) -> float | None:
    """
    Парсит цену из строки. Понимает форматы:
    $96,500  |  $96500  |  96.5K  |  96500  |  $96.5K
    """
    if not raw:
        return None
    raw = raw.strip().lstrip("$").replace(",", "").replace(" ", "")

    # K-нотация: 96.5K → 96500
    if raw.upper().endswith("K"):
        try:
            return float(raw[:-1]) * 1000
        except ValueError:
            return None
    try:
        return float(raw)
    except ValueError:
        return None


def _trading_plan_excerpt(report_text: str) -> str:
    """Берём блок «Торговый план» — там чаще всего явные вход/стоп/цель."""
    if not report_text:
        return ""
    m = re.search(
        r"(?:^|\n)(?:📌\s*)?(?:ТОРГОВЫЙ\s+ПЛАН|Торговый\s+план|ПЛАН\s+СДЕЛОК|"
        r"План\s+сделок|TRADING\s+PLAN|Итоговый\s+торговый\s+план)\s*:?[^\n]*\n([\s\S]{0,22000})",
        report_text,
        re.IGNORECASE | re.MULTILINE,
    )
    if m:
        return (m.group(0) or "")[:24000]
    return ""


def extract_predictions_from_report(report_text: str) -> list[dict]:
    """
    Парсит отчёт Synth и извлекает структурированные прогнозы.

    Понимает РЕАЛЬНЫЙ формат из SYNTH_SYSTEM:

        • Актив: BTC
        • Направление: LONG / SHORT / ВНЕ РЫНКА / НАБЛЮДАТЬ
        • Вход: $96,500
        • Цель: $105,000
        • Стоп: $93,000
        • Горизонт: 1 неделя

    А также компактные варианты (на случай если Synth написал иначе):
        BTC LONG вход $96500 цель $105000 стоп $93000
    """
    predictions = []
    known_assets = set(ASSET_MAP.keys())

    tp = _trading_plan_excerpt(report_text)
    if tp:
        report_text = tp + "\n\n" + report_text

    # ── Метод 1: структурированный блок "• Актив: ... • Направление: ..." ──────
    # Ищем блоки торгового плана целиком
    plan_blocks = re.findall(
        r'(?:Актив|актив)[:\s]+([A-Z]{2,5}).*?'
        r'(?:Направление|направление)[:\s]+(LONG|SHORT|long|short)[^\n]*\n.*?'
        r'(?:Вход|вход)[:\s]+\$?([\d,.KkКк]+).*?\n.*?'
        r'(?:Цел[ьи]|цел[ьи]|Target|target)[:\s]+\$?([\d,.KkКк]+).*?\n.*?'
        r'(?:Стоп|стоп|Stop|stop)[:\s]+\$?([\d,.KkКк]+)',
        report_text,
        re.IGNORECASE | re.DOTALL
    )

    for match in plan_blocks:
        asset, direction, entry_s, target_s, stop_s = match
        asset = asset.upper()
        direction = direction.upper()
        if asset not in known_assets:
            continue

        entry  = _parse_price(entry_s)
        target = _parse_price(target_s)
        stop   = _parse_price(stop_s)

        if not all([entry, target, stop]):
            continue
        if entry <= 0 or target <= 0 or stop <= 0:
            continue

        # Санити-проверка: стоп и цель должны быть логичными
        if direction == "LONG" and not (stop < entry < target):
            continue
        if direction == "SHORT" and not (target < entry < stop):
            continue

        # Горизонт
        tf_match = re.search(
            r'(?:Горизонт|горизонт|timeframe)[:\s]+([^\n]{1,20})',
            report_text[report_text.find(asset):report_text.find(asset) + 500],
            re.IGNORECASE
        )
        tf = _parse_timeframe(tf_match.group(1) if tf_match else "1w")

        predictions.append({
            "asset":       asset,
            "direction":   direction,
            "entry_price": entry,
            "target_price": target,
            "stop_loss":   stop,
            "timeframe":   tf,
        })

    # ── Метод 2: РЕАЛЬНЫЙ формат Synth ──────────────────────────────────────
    # "• BTC | LONG | Вход: $70,000 | Стоп: $68,000 | Цель: $74,000 | R/R 1:2 | Горизонт: 1w"
    # "-> SHORT: Вход $70,000 | Стоп $72,100 | Цель $65,800"
    if not predictions:
        pipe_pattern = re.compile(
            r'(?:•|-+>)\s*'
            r'(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)'
            r'\s*\|\s*(LONG|SHORT)'
            r'.*?(?:Вход|вход|Entry)[:\s]+\$?([\d,\.K]+)'
            r'.*?(?:Стоп|стоп|Stop)[:\s]+\$?([\d,\.K]+)'
            r'.*?(?:Цел[ьи]|цел[ьи]|Target)[:\s]+\$?([\d,\.K]+)',
            re.IGNORECASE
        )
        for m in pipe_pattern.finditer(report_text):
            asset     = m.group(1).upper()
            direction = m.group(2).upper()
            entry     = _parse_price(m.group(3))
            stop      = _parse_price(m.group(4))
            target    = _parse_price(m.group(5))

            if not all([entry, target, stop]):
                continue
            if direction == "LONG" and not (stop < entry < target):
                continue
            if direction == "SHORT" and not (target < entry < stop):
                continue

            tf_m = re.search(r'Горизонт[:\s]+([^|\n]{1,20})', m.group(0), re.IGNORECASE)
            tf = _parse_timeframe(tf_m.group(1) if tf_m else "1w")

            predictions.append({
                "asset":        asset,
                "direction":    direction,
                "entry_price":  entry,
                "target_price": target,
                "stop_loss":    stop,
                "timeframe":    tf,
            })

    # ── Метод 2b: тот же pipe, но порядок Вход → Цель → Стоп ─────────────────
    if not predictions:
        pipe_pattern2 = re.compile(
            r'(?:•|-+>)\s*'
            r'(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)'
            r'\s*\|\s*(LONG|SHORT)'
            r'.*?(?:Вход|вход|Entry)[:\s]+\$?([\d,\.K]+)'
            r'.*?(?:Цел[ьи]|цел[ьи]|Target)[:\s]+\$?([\d,\.K]+)'
            r'.*?(?:Стоп|стоп|Stop)[:\s]+\$?([\d,\.K]+)',
            re.IGNORECASE
        )
        for m in pipe_pattern2.finditer(report_text):
            asset = m.group(1).upper()
            direction = m.group(2).upper()
            entry = _parse_price(m.group(3))
            target = _parse_price(m.group(4))
            stop = _parse_price(m.group(5))
            if not all([entry, target, stop]):
                continue
            if direction == "LONG" and not (stop < entry < target):
                continue
            if direction == "SHORT" and not (target < entry < stop):
                continue
            tf_m = re.search(r'Горизонт[:\s]+([^|\n]{1,20})', m.group(0), re.IGNORECASE)
            tf = _parse_timeframe(tf_m.group(1) if tf_m else "1w")
            predictions.append({
                "asset": asset,
                "direction": direction,
                "entry_price": entry,
                "target_price": target,
                "stop_loss": stop,
                "timeframe": tf,
            })

    # ── Метод 3: стрелочный формат "-> SHORT: Вход $X | Стоп $Y | Цель $Z" ──
    if not predictions:
        arrow_pattern = re.compile(
            r'->\s*(LONG|SHORT)[:\s]+'
            r'Вход\s+\$?([\d,\.K]+)'
            r'.*?Стоп\s+\$?([\d,\.K]+)'
            r'.*?Цел[ьи]?\s+\$?([\d,\.K]+)',
            re.IGNORECASE
        )
        # Ищем актив рядом (в 200 символах до стрелки)
        for m in arrow_pattern.finditer(report_text):
            direction = m.group(1).upper()
            entry  = _parse_price(m.group(2))
            stop   = _parse_price(m.group(3))
            target = _parse_price(m.group(4))
            if not all([entry, target, stop]):
                continue
            # Ищем актив перед стрелкой
            prefix = report_text[max(0, m.start()-200):m.start()]
            asset_m = re.search(r"\b(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)\b", prefix)
            if not asset_m:
                continue
            asset = asset_m.group(1).upper()
            if direction == "LONG" and not (stop < entry < target):
                continue
            if direction == "SHORT" and not (target < entry < stop):
                continue
            predictions.append({
                "asset": asset, "direction": direction,
                "entry_price": entry, "target_price": target,
                "stop_loss": stop, "timeframe": "1w",
            })

    # ── Метод 4: компактный inline ────────────────────────────────────────────
    if not predictions:
        inline_pattern = re.compile(
            r'\b(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)\b'
            r'[:\s]+(LONG|SHORT|long|short)'
            r'[^$\n]{0,30}\$\s*([\d,.K]+)'
            r'[^$\n]{0,30}\$\s*([\d,.K]+)'
            r'[^$\n]{0,30}\$\s*([\d,.K]+)',
            re.IGNORECASE
        )
        for m in inline_pattern.finditer(report_text):
            asset     = m.group(1).upper()
            direction = m.group(2).upper()
            entry  = _parse_price(m.group(3))
            target = _parse_price(m.group(4))
            stop   = _parse_price(m.group(5))
            if not all([entry, target, stop]):
                continue
            if direction == "LONG" and not (stop < entry < target):
                continue
            if direction == "SHORT" and not (target < entry < stop):
                continue
            predictions.append({
                "asset": asset, "direction": direction,
                "entry_price": entry, "target_price": target,
                "stop_loss": stop, "timeframe": "1w",
            })

    # ── Метод 5: триггер-формат торгового плана ──────────────────────────────
    # "Триггер LONG: BTC пробой $70,200 (+3%) → LONG 5% портфеля."
    # "Триггер SHORT: BTC пробой $66,700 (-2%) → SHORT 3% портфеля."
    trigger_pattern = re.compile(
        r'Триггер\s+(LONG|SHORT)[:\s]+'
        r'(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)\s+'
        r'(?:пробой|выше|ниже|уровень)?\s*\$?([\d,\.K]+)'
        r'(?:\s*\([^\)]*\))?\s*→\s*(?:LONG|SHORT)\s+([\d]+)%',
        re.IGNORECASE
    )
    for m in trigger_pattern.finditer(report_text):
        direction = m.group(1).upper()
        asset = m.group(2).upper()
        trigger_price = _parse_price(m.group(3))
        pct_size = int(m.group(4)) if m.group(4) else 3

        if not trigger_price or trigger_price <= 0:
            continue

        # Для триггеров: entry = trigger price, stop/target вычисляем
        if direction == "LONG":
            entry = trigger_price
            target = round(trigger_price * 1.05, 2)  # +5% цель
            stop = round(trigger_price * 0.97, 2)    # -3% стоп
        else:
            entry = trigger_price
            target = round(trigger_price * 0.95, 2)  # -5% цель
            stop = round(trigger_price * 1.03, 2)    # +3% стоп

        # Проверяем нет ли уже такого актива
        existing = [p for p in predictions if p["asset"] == asset]
        if not existing:
            predictions.append({
                "asset": asset, "direction": direction,
                "entry_price": entry, "target_price": target,
                "stop_loss": stop, "timeframe": "1w",
                "is_trigger": True, "trigger_pct_size": pct_size,
            })

    # ── Метод 6: pipe-формат с указанием входа/стопа/цели (Золото и др.) ─────
    # "Золото (GC=F) | LONG | Вход: $4,809.40 | Стоп: $4,665 (-3%) | Цель: $5,098 (+6%)"
    pipe_trade_pattern = re.compile(
        r'(?:Золото|Gold|Серебро|Silver|Нефть|Oil|Платина|Platinum|Палладий|Palladium|Медь|Copper)?'
        r'(?:\s*\(([A-Z]{1,4}(?:=F)?)\))?\s*\|\s*(LONG|SHORT)\s*\|\s*'
        r'(?:Вход|Entry)[:\s]+\$?([\d,\.]+)'
        r'(?:\s*\([^\)]*\))?\s*\|\s*'
        r'(?:Стоп|Stop)[:\s]+\$?([\d,\.]+)'
        r'(?:\s*\([^\)]*\))?\s*\|\s*'
        r'(?:Цель|Target)[:\s]+\$?([\d,\.]+)',
        re.IGNORECASE
    )
    asset_name_map = {
        "ЗОЛОТО": "GLD", "GOLD": "GLD", "GC=F": "GLD",
        "СЕРЕБРО": "SLV", "SILVER": "SLV", "SI=F": "SLV",
        "НЕФТЬ": "CL=F", "OIL": "CL=F", "CL=F": "CL=F",
        "ПЛАТИНА": "PL=F", "PLATINUM": "PL=F",
        "МЕДЬ": "HG=F", "COPPER": "HG=F",
    }
    for m in pipe_trade_pattern.finditer(report_text):
        ticker = (m.group(1) or "").upper()
        direction = m.group(2).upper()
        entry = _parse_price(m.group(3))
        stop = _parse_price(m.group(4))
        target = _parse_price(m.group(5))

        if not all([entry, stop, target]) or entry <= 0:
            continue

        asset = asset_name_map.get(ticker, ticker)
        if asset not in known_assets and ticker not in asset_name_map:
            # Добавляем как custom asset
            pass

        existing = [p for p in predictions if p["asset"] == asset]
        if not existing:
            predictions.append({
                "asset": asset, "direction": direction,
                "entry_price": entry, "target_price": target,
                "stop_loss": stop, "timeframe": "1w",
            })

    if predictions:
        logger.info(f"📊 Найдено {len(predictions)} прогнозов в отчёте")
    else:
        logger.debug("Прогнозы в отчёте не найдены (Synth написал ВНЕ РЫНКА или нестандартный формат)")

    return predictions


def _parse_timeframe(raw: str) -> str:
    """Нормализует строку горизонта в код таймфрейма."""
    raw = raw.lower().strip()
    if any(x in raw for x in ["скальп", "день", "1d", "intraday"]):
        return "1d"
    if any(x in raw for x in ["3 дн", "3d"]):
        return "3d"
    if any(x in raw for x in ["недел", "1w", "week"]):
        return "1w"
    if any(x in raw for x in ["2 недел", "2w"]):
        return "2w"
    if any(x in raw for x in ["месяц", "1m", "month"]):
        return "1m"
    return "1w"  # дефолт


def _extract_price_levels_from_report(report_text: str) -> dict:
    """
    Извлекает ценовые уровни из текста отчёта, когда нет структурированных прогнозов.
    Ищет паттерны:
    - "BTC: $68,110.98 | RSI 46.4"
    - "Триггер LONG: BTC пробой $70,200"
    - "Вход: $4,809.40 | Стоп: $4,665 | Цель: $5,098"
    - "Золото (GC=F) | LONG | Вход: $4,809.40"
    """
    entries = {}
    stops = {}
    targets = {}

    # 1) Текущие уровни: "BTC: $68,110.98 | RSI"
    current_price_pattern = re.compile(
        r'\b(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)\b'
        r'[:\s]+\$?([\d,]+\.?\d*)\s*\|',
        re.IGNORECASE
    )
    for m in current_price_pattern.finditer(report_text):
        asset = m.group(1).upper()
        price = _parse_price(m.group(2))
        if price and asset not in entries:
            entries[asset] = price

    # 2) Триггеры: "Триггер LONG: BTC пробой $70,200"
    trigger_pattern = re.compile(
        r'Триггер\s+(LONG|SHORT)[:\s]+'
        r'(BTC|ETH|SOL|BNB|SPY|QQQ|NVDA|AAPL|TSLA|GLD)\s+'
        r'(?:пробой|выше|ниже|уровень)?\s*\$?([\d,\.K]+)',
        re.IGNORECASE
    )
    for m in trigger_pattern.finditer(report_text):
        direction = m.group(1).upper()
        asset = m.group(2).upper()
        price = _parse_price(m.group(3))
        if price:
            entries[asset] = price
            if direction == "LONG":
                targets[asset] = round(price * 1.05, 2)
                stops[asset] = round(price * 0.97, 2)
            else:
                targets[asset] = round(price * 0.95, 2)
                stops[asset] = round(price * 1.03, 2)

    # 3) Pipe-формат: "Золото | LONG | Вход: $X | Стоп: $Y | Цель: $Z"
    pipe_pattern = re.compile(
        r'(?:Вход|Entry)[:\s]+\$?([\d,\.]+)'
        r'(?:\s*\([^\)]*\))?\s*\|\s*'
        r'(?:Стоп|Stop)[:\s]+\$?([\d,\.]+)'
        r'(?:\s*\([^\)]*\))?\s*\|\s*'
        r'(?:Цель|Target)[:\s]+\$?([\d,\.]+)',
        re.IGNORECASE
    )
    for m in pipe_pattern.finditer(report_text):
        entry = _parse_price(m.group(1))
        stop = _parse_price(m.group(2))
        target = _parse_price(m.group(3))
        if entry:
            entries["GLD"] = entry
        if stop:
            stops["GLD"] = stop
        if target:
            targets["GLD"] = target

    # 4) Выход из CASH: "VIX < 22 И Fear&Greed > 30"
    cash_exit = re.search(r'Выход\s+из\s+CASH[:\s]+VIX\s*<\s*([\d.]+)', report_text, re.IGNORECASE)
    if cash_exit:
        vix_level = float(cash_exit.group(1))
        entries["VIX"] = vix_level

    return {"entries": entries, "stops": stops, "targets": targets}


async def save_predictions_from_report(
    report_text: str,
    source_news: str = "",
    bot=None,
    admin_ids: list = None,
    prompt_versions: dict | None = None,
    model_inputs_snapshot: dict | None = None,
):
    """Извлекает и сохраняет все прогнозы из отчёта."""
    predictions = extract_predictions_from_report(report_text)

    saved = 0

    # Вердикт: markdown / emoji; первая строка блока
    verdict = "NEUTRAL"
    vm = re.search(
        r"ВЕРДИКТ\s*СУДЬИ\s*[:：]\s*(.+?)(?:\n{2,}|$)",
        report_text,
        re.IGNORECASE | re.DOTALL,
    )
    if vm:
        first_line = (vm.group(1) or "").split("\n")[0]
        verdict_line = re.sub(r"[*_`🟡🟢🔴🏆\[\]]", "", first_line).strip().upper()
        logger.info("Вердикт судьи из отчёта: %s", verdict_line[:120])
        if "БЫЧ" in verdict_line or "BULL" in verdict_line:
            verdict = "BUY"
        elif "МЕДВЕЖ" in verdict_line or "BEAR" in verdict_line:
            verdict = "SELL"
        else:
            verdict = "NEUTRAL"

    # Также проверяем ВЕРДИКТ И ТОРГОВЫЙ ПЛАН / ИТОГОВЫЙ СИНТЕЗ
    if verdict == "NEUTRAL":
        vm2 = re.search(
            r"(?:ВЕРДИКТ|ИТОГОВЫЙ\s+СИНТЕЗ)[^\n]{0,200}(?:БЫЧ|BULL|МЕДВЕЖ|BEAR)",
            report_text,
            re.IGNORECASE | re.DOTALL,
        )
        if vm2:
            block = vm2.group(0).upper()
            if "БЫЧ" in block or "BULL" in block:
                verdict = "BUY"
            elif "МЕДВЕЖ" in block or "BEAR" in block:
                verdict = "SELL"
    
    # Save daily context for signal trader — ВСЕГДА, даже при пустых predictions
    from database import save_daily_context
    symbols = [p["asset"] for p in predictions] if predictions else []
    entries = {p["asset"]: p["entry_price"] for p in predictions if p.get("entry_price")} if predictions else {}
    stop_losses = {p["asset"]: p["stop_loss"] for p in predictions if p.get("stop_loss")} if predictions else {}
    targets = {p["asset"]: p["target_price"] for p in predictions if p.get("target_price")} if predictions else {}
    timeframes = {p["asset"]: p["timeframe"] for p in predictions} if predictions else {}
    
    # Если нет явных прогнозов, извлекаем уровни из текста отчёта
    if not predictions:
        price_levels = _extract_price_levels_from_report(report_text)
        entries = price_levels.get("entries", {})
        stop_losses = price_levels.get("stops", {})
        targets = price_levels.get("targets", {})
        symbols = list(set(list(entries.keys()) + list(stop_losses.keys()) + list(targets.keys())))
    
    await save_daily_context(
        verdict=verdict,
        symbols=symbols,
        entries=entries,
        stop_losses=stop_losses,
        targets=targets,
        timeframes=timeframes,
        news_summary=source_news[:300],
        prompt_versions=prompt_versions,
        model_inputs_snapshot=model_inputs_snapshot,
    )
    logger.info(f"Saved daily context: verdict={verdict}, symbols={symbols}, predictions={len(predictions)}")
    
    for pred in predictions:
        try:
            await save_prediction(
                asset=pred["asset"],
                direction=pred["direction"],
                entry_price=pred["entry_price"],
                target_price=pred["target_price"],
                stop_loss=pred["stop_loss"],
                timeframe=pred["timeframe"],
                source_news=source_news[:300],
            )

            saved += 1
        except Exception as e:
            logger.warning(f"Не удалось сохранить прогноз: {e}")

    if saved:
        logger.info(f"Сохранено {saved} прогнозов из отчёта")

    return saved
