"""
COT (Commitments of Traders) — официальные недельные данные CFTC.

Источник (годовой архив CSV, внутри — все отчёты за год):
  - Сырьё (disaggregated): fut_disagg_txt_{year}.zip → f_year.txt
  - Финансы (BTC, S&P, валюды): fut_fin_txt_{year}.zip → FinFutYY.txt

Требуется User-Agent, иначе CFTC отвечает 403. TLS: certifi; при проблемах — COT_INSECURE_SSL=1.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import ssl
import time
import zipfile
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

CFTC_UA = "Mozilla/5.0 (compatible; DialecticEdge-Bot/1.0; +https://github.com/borzenkovandrej07-alt/DIALECTIC_EDg)"

try:
    import certifi

    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CTX = True  # type: ignore


def _connector():
    import os

    if os.getenv("COT_INSECURE_SSL", "").strip().lower() in ("1", "true", "yes"):
        return aiohttp.TCPConnector(ssl=False)
    return aiohttp.TCPConnector(ssl=_SSL_CTX)


ASSET_CODES: dict[str, dict[str, str]] = {
    "Bitcoin": {"dataset": "fin", "code": "133741"},
    "Gold": {"dataset": "disagg", "code": "088691"},
    "Silver": {"dataset": "disagg", "code": "084691"},
    "Crude Oil": {"dataset": "disagg", "code": "067651"},
    "S&P 500": {"dataset": "fin", "code": "13874A"},
    "US Dollar Index": {"dataset": "fin", "code": "098662"},
    "Euro": {"dataset": "fin", "code": "099741"},
}

_rows_cache: dict[tuple[str, int], tuple[float, list[dict[str, Any]]]] = {}
_CACHE_SEC = 8 * 3600


def _clean(val: Optional[str]) -> str:
    if val is None:
        return ""
    return str(val).strip().strip('"')


def _i(val: Optional[str]) -> int:
    try:
        return int(float(_clean(val).replace(",", "")))
    except (TypeError, ValueError):
        return 0


async def _download_zip_rows(
    session: aiohttp.ClientSession,
    dataset: str,
    year: int,
) -> list[dict[str, Any]]:
    key = (dataset, year)
    now = time.time()
    hit = _rows_cache.get(key)
    if hit and hit[0] > now:
        return hit[1]

    if dataset == "fin":
        url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
    else:
        url = f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"

    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=90),
            headers={"User-Agent": CFTC_UA},
        ) as resp:
            if resp.status != 200:
                logger.warning("COT zip %s -> HTTP %s", url, resp.status)
                return []
            data = await resp.read()
    except Exception as e:
        logger.warning("COT zip fetch error %s: %s", url, e)
        return []

    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        names = [n for n in z.namelist() if n.lower().endswith(".txt")]
        if not names:
            return []
        text = z.read(names[0]).decode("utf-8", "replace")
        rows = list(csv.DictReader(text.splitlines()))
    except Exception as e:
        logger.warning("COT zip parse error: %s", e)
        return []

    _rows_cache[key] = (now + _CACHE_SEC, rows)
    return rows


def _latest_row_for_code(rows: list[dict[str, Any]], code: str) -> Optional[dict[str, Any]]:
    code = code.strip().upper()
    matches = [
        r
        for r in rows
        if _clean(r.get("CFTC_Contract_Market_Code")).strip('"').upper() == code
    ]
    if not matches:
        return None
    return max(matches, key=lambda r: _clean(r.get("Report_Date_as_YYYY-MM-DD")))


def _row_to_record_fin(row: dict[str, Any], label: str) -> dict[str, Any]:
    d_long = _i(row.get("Dealer_Positions_Long_All"))
    d_short = _i(row.get("Dealer_Positions_Short_All"))
    a_long = _i(row.get("Asset_Mgr_Positions_Long_All"))
    a_short = _i(row.get("Asset_Mgr_Positions_Short_All"))
    l_long = _i(row.get("Lev_Money_Positions_Long_All"))
    l_short = _i(row.get("Lev_Money_Positions_Short_All"))
    return {
        "date": _clean(row.get("Report_Date_as_YYYY-MM-DD")),
        "asset": label,
        "market": _clean(row.get("Market_and_Exchange_Names"))[:120],
        "open_interest": _i(row.get("Open_Interest_All")),
        "commercials_long": d_long,
        "commercials_short": d_short,
        "large_speculators_long": a_long + l_long,
        "large_speculators_short": a_short + l_short,
        "small_speculators_long": 0,
        "small_speculators_short": 0,
        "dataset": "TFF (Dealer | AssetMgr+LevMoney)",
    }


def _row_to_record_disagg(row: dict[str, Any], label: str) -> dict[str, Any]:
    c_long = _i(row.get("Prod_Merc_Positions_Long_All"))
    c_short = _i(row.get("Prod_Merc_Positions_Short_All"))
    m_long = _i(row.get("M_Money_Positions_Long_All"))
    m_short = _i(row.get("M_Money_Positions_Short_All"))
    return {
        "date": _clean(row.get("Report_Date_as_YYYY-MM-DD")),
        "asset": label,
        "market": _clean(row.get("Market_and_Exchange_Names"))[:120],
        "open_interest": _i(row.get("Open_Interest_All")),
        "commercials_long": c_long,
        "commercials_short": c_short,
        "large_speculators_long": m_long,
        "large_speculators_short": m_short,
        "small_speculators_long": 0,
        "small_speculators_short": 0,
        "dataset": "Disaggregated (ProdMerc | M_Money)",
    }


async def _fetch_one_asset(
    session: aiohttp.ClientSession,
    label: str,
    meta: dict[str, str],
    disagg_rows: Optional[list[dict[str, Any]]],
    fin_rows: Optional[list[dict[str, Any]]],
) -> Optional[dict[str, Any]]:
    ds = meta["dataset"]
    code = meta["code"]
    rows = fin_rows if ds == "fin" else disagg_rows
    if rows is None:
        return None
    row = _latest_row_for_code(rows, code)
    if not row:
        logger.warning("COT: no row for %s code=%s dataset=%s", label, code, ds)
        return None
    if ds == "fin":
        return _row_to_record_fin(row, label)
    return _row_to_record_disagg(row, label)


async def get_cot_for_assets(assets: list[str] | None = None) -> dict[str, dict[str, Any]]:
    if assets is None:
        assets = ["Bitcoin", "Gold", "Crude Oil"]

    need_disagg = any(ASSET_CODES.get(a, {}).get("dataset") == "disagg" for a in assets)
    need_fin = any(ASSET_CODES.get(a, {}).get("dataset") == "fin" for a in assets)

    y = time.gmtime().tm_year
    years_try = [y, y - 1]
    requested = [a for a in assets if ASSET_CODES.get(a)]
    out: dict[str, dict[str, Any]] = {}

    async with aiohttp.ClientSession(connector=_connector()) as session:
        disagg_rows: Optional[list[dict[str, Any]]] = None
        fin_rows: Optional[list[dict[str, Any]]] = None

        if need_disagg:
            for year in years_try:
                disagg_rows = await _download_zip_rows(session, "disagg", year)
                if disagg_rows:
                    break
        if need_fin:
            for year in years_try:
                fin_rows = await _download_zip_rows(session, "fin", year)
                if fin_rows:
                    break

        tasks = [_fetch_one_asset(session, a, ASSET_CODES[a], disagg_rows, fin_rows) for a in requested]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)

    for asset, result in zip(requested, results_list):
        if isinstance(result, Exception):
            logger.warning("COT asset %s: %s", asset, result)
            continue
        if result:
            out[asset] = result
    return out


def format_cot_for_agents(cot_data: dict[str, dict[str, Any]]) -> str:
    if not cot_data:
        return "COT data not available"

    lines = [
        "=== COT (Commitments of Traders, CFTC weekly) ===",
        "_Lag: report Tue positions, publish usually Fri._",
        "",
    ]

    for asset, d in sorted(cot_data.items()):
        cl = d.get("commercials_long", 0)
        cs = d.get("commercials_short", 0)
        ll = d.get("large_speculators_long", 0)
        ls = d.get("large_speculators_short", 0)
        net_c = cl - cs
        net_l = ll - ls
        c_bias = "NET LONG" if net_c > 0 else "NET SHORT" if net_c < 0 else "FLAT"
        l_bias = "NET LONG" if net_l > 0 else "NET SHORT" if net_l < 0 else "FLAT"
        oi = d.get("open_interest", 0)
        lines.append(f"{asset} -- report {d.get('date', 'N/A')} [{d.get('dataset', '')}]")
        if d.get("market"):
            lines.append(f"  Contract: {d['market']}")
        lines.append(f"  OI: {oi:,} contracts")
        lines.append(
            f"  Hedgers (commercials): long {cl:,} short {cs:,} -> {c_bias} (net {net_c:+,})"
        )
        lines.append(
            f"  Large specs (report category): long {ll:,} short {ls:,} -> {l_bias} (net {net_l:+,})"
        )
        lines.append("")

    return "\n".join(lines).rstrip()


if __name__ == "__main__":
    async def _test():
        data = await get_cot_for_assets(["Bitcoin", "Gold", "Crude Oil"])
        print(format_cot_for_agents(data))

    asyncio.run(_test())
