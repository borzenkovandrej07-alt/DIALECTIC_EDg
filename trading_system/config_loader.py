"""Load and merge trading JSON config (non-destructive; defaults from package)."""

from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PKG_DIR = Path(__file__).resolve().parent
_DEFAULT_PATH = _PKG_DIR / "default_trading_config.json"


def _deep_merge(base: dict, override: dict) -> dict:
    out = deepcopy(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_trading_config(path: str | Path | None = None) -> dict[str, Any]:
    """
    Load `config/trading_config.json` if present, else copy defaults next to project.
    """
    if path is None:
        path = os.getenv("TRADING_CONFIG_PATH", "").strip() or "config/trading_config.json"
    p = Path(path)
    if not p.is_absolute():
        p = Path.cwd() / p

    if not _DEFAULT_PATH.exists():
        raise FileNotFoundError(f"Missing bundled defaults: {_DEFAULT_PATH}")

    with open(_DEFAULT_PATH, encoding="utf-8") as f:
        defaults = json.load(f)

    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        merged = deepcopy(defaults)
        merged.setdefault("paths", {})["config_file"] = str(p)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        logger.info("Created trading config at %s", p)
        return merged

    with open(p, encoding="utf-8") as f:
        user = json.load(f)
    return _deep_merge(defaults, user)


def cache_dir_from_config(cfg: dict[str, Any]) -> Path:
    sub = (cfg.get("paths") or {}).get("cache_subdir") or "trading_market_cache"
    root = os.getenv("DATA_DIR", "").strip() or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    if root:
        base = Path(root) / sub
    else:
        base = Path.cwd() / ".cache" / sub
    base.mkdir(parents=True, exist_ok=True)
    return base


def cli_results_path(cfg: dict[str, Any]) -> Path:
    rel = (cfg.get("paths") or {}).get("cli_results_file") or "data/cli_backtest_results.json"
    out = Path(rel)
    if not out.is_absolute():
        out = Path.cwd() / out
    out.parent.mkdir(parents=True, exist_ok=True)
    return out
