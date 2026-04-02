"""
session_manager.py — Управление торговыми сессиями.

Без БД — всё хранится в BACKTEST.md на GitHub.
Когда капитал падает ниже SESSION_MIN_CAPITAL — сессия закрывается,
генерируется урок, начинается новая сессия с адаптированными параметрами.
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional

from config import (
    AUTOTRADE_OPEN_SCORE_THRESHOLD,
    AUTOTRADE_NEUTRAL_SL_PCT,
    AUTOTRADE_NEUTRAL_TP_PCT,
)

logger = logging.getLogger(__name__)

SESSION_MIN_CAPITAL = 10.0  # Ниже этого — сессия закрывается
SESSION_START_CAPITAL = 100.0
MAX_SESSIONS_IN_FILE = 20  # Храним последние 20 сессий

# Адаптивные параметры по умолчанию
DEFAULT_PARAMS = {
    "open_score_threshold": AUTOTRADE_OPEN_SCORE_THRESHOLD,
    "neutral_sl_pct": AUTOTRADE_NEUTRAL_SL_PCT,
    "neutral_tp_pct": AUTOTRADE_NEUTRAL_TP_PCT,
    "quantity_pct": 1.0,
    "max_trades_per_session": 50,
}


class SessionState:
    """Текущая сессия."""

    def __init__(self):
        self.session_id: int = 0
        self.start_time: str = ""
        self.start_capital: float = SESSION_START_CAPITAL
        self.current_capital: float = SESSION_START_CAPITAL
        self.trades: list = []
        self.wins: int = 0
        self.losses: int = 0
        self.total_pnl: float = 0.0
        self.peak_capital: float = SESSION_START_CAPITAL
        self.max_drawdown: float = 0.0
        self.lesson: str = ""
        self.status: str = "active"  # active | closed

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "start_time": self.start_time,
            "end_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "start_capital": round(self.start_capital, 2),
            "end_capital": round(self.current_capital, 2),
            "pnl": round(self.total_pnl, 2),
            "pnl_pct": round((self.total_pnl / self.start_capital) * 100, 2) if self.start_capital else 0,
            "trades": len(self.trades),
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.wins / max(self.wins + self.losses, 1) * 100, 1),
            "peak_capital": round(self.peak_capital, 2),
            "max_drawdown_pct": round(self.max_drawdown / max(self.peak_capital, 1) * 100, 1),
            "lesson": self.lesson,
            "status": self.status,
        }


class SessionManager:
    """
    Управляет сессиями. Хранит состояние в памяти,
    экспортирует в BACKTEST.md на GitHub.
    """

    def __init__(self):
        self.current_session = SessionState()
        self.past_sessions: list[dict] = []
        self._params = dict(DEFAULT_PARAMS)
        self._loaded = False

    def _load_from_backtest(self, content: str):
        """Парсит BACKTEST.md и восстанавливает историю сессий."""
        if not content:
            return

        # Ищем секцию сессий
        session_section = re.search(
            r"## 📚 История сессий\n\n([\s\S]*?)(?=\n## |\Z)",
            content,
        )
        if not session_section:
            return

        section_text = session_section.group(1)

        # Парсим таблицу сессий
        rows = re.findall(
            r"\|\s*(\d+)\s*\|\s*([\d\-:\s]+)\s*\|\s*([\d\-:\s]+)\s*\|\s*\$?([\d,.]+)\s*\|\s*\$?([\d,.]+)\s*\|\s*\$?([-\d,.]+)\s*\|\s*([-\d,.]+)%\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d,.]+)%\s*\|\s*(\w+)\s*\|",
            section_text,
        )

        for row in rows:
            self.past_sessions.append({
                "session_id": int(row[0]),
                "start_time": row[1].strip(),
                "end_time": row[2].strip(),
                "start_capital": float(row[3].replace(",", "")),
                "end_capital": float(row[4].replace(",", "")),
                "pnl": float(row[5].replace(",", "")),
                "pnl_pct": float(row[6]),
                "trades": int(row[7]),
                "wins": int(row[8]),
                "losses": int(row[9]),
                "win_rate": float(row[10]),
                "status": row[11].strip(),
            })

        # Ищем уроки
        lessons_section = re.search(
            r"## 🧠 Уроки из прошлых сессий\n\n([\s\S]*?)(?=\n## |\Z)",
            content,
        )
        if lessons_section:
            lessons_text = lessons_section.group(1)
            # Парсим уроки
            lesson_blocks = re.findall(
                r"### Сессия #(\d+)\n\n(.*?)(?=\n### |\Z)",
                lessons_text,
                re.DOTALL,
            )
            for session_id, lesson_text in lesson_blocks:
                for s in self.past_sessions:
                    if s["session_id"] == int(session_id):
                        s["lesson"] = lesson_text.strip()
                        break

        # Ищем текущую сессию
        current_section = re.search(
            r"## 🔄 Текущая сессия\n\n([\s\S]*?)(?=\n## |\Z)",
            content,
        )
        if current_section:
            current_text = current_section.group(1)
            id_match = re.search(r"ID:\s*(\d+)", current_text)
            start_match = re.search(r"Старт:\s*([\d\-:\s]+)", current_text)
            capital_match = re.search(r"Капитал:\s*\$?([\d,.]+)", current_text)
            trades_match = re.search(r"Сделок:\s*(\d+)", current_text)
            wins_match = re.search(r"Побед:\s*(\d+)", current_text)
            losses_match = re.search(r"Поражений:\s*(\d+)", current_text)
            pnl_match = re.search(r"PnL:\s*\$?([-\d,.]+)", current_text)

            if id_match:
                self.current_session.session_id = int(id_match.group(1))
            if start_match:
                self.current_session.start_time = start_match.group(1).strip()
            if capital_match:
                cap = float(capital_match.group(1).replace(",", ""))
                self.current_session.current_capital = cap
                self.current_session.start_capital = cap
            if trades_match:
                pass  # trades count
            if wins_match:
                self.current_session.wins = int(wins_match.group(1))
            if losses_match:
                self.current_session.losses = int(losses_match.group(1))
            if pnl_match:
                self.current_session.total_pnl = float(pnl_match.group(1).replace(",", ""))

            self.current_session.status = "active"

        # Адаптируем параметры на основе прошлых сессий
        self._adapt_params()
        self._loaded = True

    def _adapt_params(self):
        """Корректирует параметры на основе прошлых сессий."""
        if not self.past_sessions:
            return

        # Считаем общую статистику
        total_sessions = len(self.past_sessions)
        profitable_sessions = sum(1 for s in self.past_sessions if s.get("pnl", 0) > 0)
        avg_win_rate = sum(s.get("win_rate", 0) for s in self.past_sessions) / max(total_sessions, 1)

        # Если большинство сессий убыточные — ужесточаем параметры
        if profitable_sessions < total_sessions / 2:
            # Повышаем порог входа на 10% за каждую убыточную сессию подряд
            losing_streak = 0
            for s in reversed(self.past_sessions):
                if s.get("pnl", 0) <= 0:
                    losing_streak += 1
                else:
                    break

            if losing_streak > 0:
                self._params["open_score_threshold"] = min(
                    DEFAULT_PARAMS["open_score_threshold"] * (1 + losing_streak * 0.15),
                    30.0,  # Максимум 30
                )
                self._params["neutral_sl_pct"] = min(
                    DEFAULT_PARAMS["neutral_sl_pct"] * (1 - losing_streak * 0.05),
                    0.02,  # Минимум 2%
                )
                self._params["quantity_pct"] = max(
                    DEFAULT_PARAMS["quantity_pct"] * (1 - losing_streak * 0.1),
                    0.3,  # Минимум 30% от размера
                )
                logger.info(
                    f"📉 Адаптация: {losing_streak} убыточных сессий подряд. "
                    f"Порог: {self._params['open_score_threshold']:.1f}, "
                    f"Стоп: {self._params['neutral_sl_pct']:.2%}, "
                    f"Размер: {self._params['quantity_pct']:.1%}"
                )
        else:
            # Если большинство прибыльных — можно чуть расслабить
            winning_streak = 0
            for s in reversed(self.past_sessions):
                if s.get("pnl", 0) > 0:
                    winning_streak += 1
                else:
                    break

            if winning_streak >= 2:
                self._params["open_score_threshold"] = max(
                    DEFAULT_PARAMS["open_score_threshold"] * 0.9,
                    15.0,
                )
                self._params["quantity_pct"] = min(
                    DEFAULT_PARAMS["quantity_pct"] * 1.1,
                    2.0,
                )
                logger.info(
                    f"📈 Адаптация: {winning_streak} прибыльных сессий подряд. "
                    f"Порог: {self._params['open_score_threshold']:.1f}, "
                    f"Размер: {self._params['quantity_pct']:.1%}"
                )

    def generate_lesson(self) -> str:
        """Генерирует урок из текущей сессии."""
        s = self.current_session
        lines = []

        lines.append(f"**Сессия #{s.session_id}:** {s.pnl:+.2f} ({s.pnl_pct:+.1f}%)")
        lines.append(f"- Сделок: {len(s.trades)} | Побед: {s.wins} | Поражений: {s.losses}")
        lines.append(f"- Макс. просадка: {s.max_drawdown_pct:.1f}%")

        if s.pnl < 0:
            lines.append("")
            lines.append("**Что пошло не так:**")
            if s.losses > s.wins:
                lines.append("- Больше проигрышных сделок, чем выигрышных")
            if s.max_drawdown_pct > 20:
                lines.append("- Глубокая просадка капитала")

            # Анализируем убыточные сделки
            losing_trades = [t for t in s.trades if t.get("pnl", 0) < 0]
            if losing_trades:
                symbols = set(t.get("symbol", "") for t in losing_trades)
                lines.append(f"- Проблемные активы: {', '.join(symbols)}")

            lines.append("")
            lines.append("**Рекомендации:**")
            if s.losses > s.wins:
                lines.append("- Повысить порог входа (быть избирательнее)")
            if s.max_drawdown_pct > 20:
                lines.append("- Ужесточить стоп-лоссы")
            lines.append("- Уменьшить размер позиции")
        else:
            lines.append("")
            lines.append("**Что сработало:**")
            if s.wins > s.losses:
                lines.append("- Хороший винрейт")
            winning_trades = [t for t in s.trades if t.get("pnl", 0) > 0]
            if winning_trades:
                symbols = set(t.get("symbol", "") for t in winning_trades)
                lines.append(f"- Удачные активы: {', '.join(symbols)}")

        return "\n".join(lines)

    def update_capital(self, new_capital: float):
        """Обновляет капитал текущей сессии."""
        old_capital = self.current_session.current_capital
        self.current_session.current_capital = new_capital
        self.current_session.total_pnl = new_capital - self.current_session.start_capital

        if new_capital > self.current_session.peak_capital:
            self.current_session.peak_capital = new_capital

        drawdown = self.current_session.peak_capital - new_capital
        if drawdown > self.current_session.max_drawdown:
            self.current_session.max_drawdown = drawdown

    def record_trade(self, trade: dict):
        """Записывает сделку в текущую сессию."""
        self.current_session.trades.append(trade)

        pnl = trade.get("pnl", 0)
        if pnl > 0:
            self.current_session.wins += 1
        elif pnl < 0:
            self.current_session.losses += 1

    def should_close_session(self) -> bool:
        """Проверяет, нужно ли закрыть сессию."""
        if self.current_session.current_capital <= SESSION_MIN_CAPITAL:
            return True
        if len(self.current_session.trades) >= self._params.get("max_trades_per_session", 50):
            return True
        return False

    def close_session(self) -> dict:
        """Закрывает текущую сессию и начинает новую."""
        self.current_session.status = "closed"
        self.current_session.lesson = self.generate_lesson()

        session_data = self.current_session.to_dict()
        self.past_sessions.append(session_data)

        # Ограничиваем историю
        if len(self.past_sessions) > MAX_SESSIONS_IN_FILE:
            self.past_sessions = self.past_sessions[-MAX_SESSIONS_IN_FILE:]

        logger.info(
            f"🏁 Сессия #{self.current_session.session_id} закрыта: "
            f"PnL {self.current_session.total_pnl:+.2f}, "
            f"Сделок: {len(self.current_session.trades)}"
        )

        # Начинаем новую сессию
        new_session = SessionState()
        new_session.session_id = self.current_session.session_id + 1
        new_session.start_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        self.current_session = new_session

        # Адаптируем параметры
        self._adapt_params()

        return session_data

    def get_adaptive_params(self) -> dict:
        """Возвращает адаптированные параметры для signal_trader."""
        return dict(self._params)

    def format_backtest_md(
        self,
        signals: list[dict],
        stats: dict,
        config: dict = None,
    ) -> str:
        """Формирует содержимое BACKTEST.md с сессиями."""
        capital = config.get("capital", 100.0) if config else 100.0
        enabled = config.get("enabled", 1) if config else 1

        lines = [
            "# 📊 Dialectic Edge — Backtest & Sessions",
            f"> Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            "## 💵 Капитал",
            f"- Текущий: **${capital:,.2f}**",
            f"- Статус: **{'✅ Включён' if enabled else '❌ Выключен'}**",
            "",
            "## 🔄 Текущая сессия",
            "",
            f"- ID: **#{self.current_session.session_id}**",
            f"- Старт: {self.current_session.start_time or '—'}",
            f"- Капитал: ${self.current_session.current_capital:,.2f}",
            f"- Сделок: {len(self.current_session.trades)}",
            f"- Побед: {self.current_session.wins} | Поражений: {self.current_session.losses}",
            f"- PnL: ${self.current_session.total_pnl:+,.2f}",
            f"- Макс. просадка: ${self.current_session.max_drawdown:,.2f}",
            "",
            "## 📈 Статистика (все сессии)",
            "",
        ]

        # Общая статистика
        total_sessions = len(self.past_sessions)
        if total_sessions > 0:
            profitable = sum(1 for s in self.past_sessions if s.get("pnl", 0) > 0)
            avg_pnl = sum(s.get("pnl", 0) for s in self.past_sessions) / total_sessions
            avg_win_rate = sum(s.get("win_rate", 0) for s in self.past_sessions) / total_sessions

            lines.extend([
                f"- Всего сессий: **{total_sessions}**",
                f"- Прибыльных: **{profitable}** ({profitable/total_sessions*100:.0f}%)",
                f"- Средний PnL: **${avg_pnl:+,.2f}**",
                f"- Средний винрейт: **{avg_win_rate:.1f}%**",
                "",
            ])
        else:
            lines.extend([
                "- Нет завершённых сессий",
                "",
            ])

        # Адаптивные параметры
        lines.extend([
            "## ⚙️ Адаптивные параметры",
            "",
            f"- Порог входа: **{self._params['open_score_threshold']:.1f}**",
            f"- Стоп-лосс: **{self._params['neutral_sl_pct']:.2%}**",
            f"- Тейк-профит: **{self._params['neutral_tp_pct']:.2%}**",
            f"- Размер позиции: **{self._params['quantity_pct']:.1%}**",
            "",
            "## 📚 История сессий",
            "",
            "| # | Старт | Конец | Старт $ | Конец $ | PnL $ | PnL % | Сделок | Побед | WR % | Статус |",
            "|---|-------|-------|---------|---------|-------|-------|--------|-------|------|--------|",
        ])

        for s in self.past_sessions:
            lines.append(
                f"| {s['session_id']} | {s['start_time']} | {s['end_time']} | "
                f"${s['start_capital']:,.0f} | ${s['end_capital']:,.0f} | "
                f"${s['pnl']:+,.0f} | {s['pnl_pct']:+.1f}% | "
                f"{s['trades']} | {s['wins']} | {s['win_rate']:.0f}% | {s['status']} |"
            )

        # Уроки
        lines.extend([
            "",
            "## 🧠 Уроки из прошлых сессий",
            "",
        ])

        for s in self.past_sessions:
            if s.get("lesson"):
                lines.append(f"### Сессия #{s['session_id']}")
                lines.append("")
                lines.append(s["lesson"])
                lines.append("")

        # История сделок
        lines.extend([
            "## 📋 История сделок",
            "",
        ])

        closed_signals = [s for s in signals if s.get("status") == "closed"]
        if closed_signals:
            lines.extend([
                "| Дата | Актив | Направление | Вход | Выход | PnL $ | PnL % |",
                "|------|-------|-------------|------|-------|-------|-------|",
            ])
            for s in closed_signals:
                date = s.get("created_at", "")[:10] or ""
                symbol = s.get("symbol", "") or ""
                direction = s.get("direction", "") or ""
                entry = s.get("entry_price") or 0
                exit_price = s.get("exit_price") or 0
                pnl = s.get("pnl") or 0
                pnl_pct = s.get("pnl_pct") or 0

                lines.append(
                    f"| {date} | {symbol} | {direction} | ${entry:,.0f} | ${exit_price:,.0f} | ${pnl:+,.0f} | {pnl_pct:+.1f}% |"
                )
        else:
            lines.append("Нет закрытых сделок.")

        # Открытые позиции
        open_signals = [s for s in signals if s.get("status") == "open"]
        if open_signals:
            lines.extend([
                "",
                "## 🔵 Открытые позиции",
                "",
            ])
            for s in open_signals:
                date = s.get("created_at", "")[:10] or ""
                symbol = s.get("symbol", "") or ""
                direction = s.get("direction", "") or ""
                entry = s.get("entry_price") or 0
                qty = s.get("quantity") or 0
                lines.append(f"- **{symbol}** {direction} @ ${entry:,.2f} (qty: {qty:.4f}) — {date}")

        return "\n".join(lines)


# Глобальный экземпляр
session_manager = SessionManager()
