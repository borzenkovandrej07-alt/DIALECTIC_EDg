"""
scheduler.py — Фоновые задачи по расписанию.

ИСПРАВЛЕНО v2:
- export_now() больше НЕ вызывается после каждого /daily.
  Это вызывало бесконечный цикл: /daily → GitHub коммит → Railway деплой →
  бот рестартует → /daily по расписанию → GitHub коммит → Railway деплой...

- GitHub экспорт теперь происходит только 1 раз в сутки (в 00:05 UTC),
  а не после каждого запроса пользователя.

- Добавлена защита от двойного запуска экспорта (_last_export_date).
"""
import asyncio
import logging
import os
from datetime import datetime, date
from database import (
    get_daily_subscribers,
    reset_daily_counts,
    get_signals_subscribers,
)

try:
    from alert_system import AlertSystem
    ALERT_SYSTEM_ENABLED = True
except ImportError:
    ALERT_SYSTEM_ENABLED = False

try:
    from signals import SignalsSystem
    SIGNALS_SYSTEM_ENABLED = True
except ImportError:
    SIGNALS_SYSTEM_ENABLED = False

try:
    from auto_tracker import AutoTracker
    AUTO_TRACKER_ENABLED = True
except ImportError:
    AUTO_TRACKER_ENABLED = False

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, bot, send_daily_fn, check_predictions_fn):
        self.bot = bot
        self.send_daily = send_daily_fn
        self.check_predictions = check_predictions_fn
        self._running = False
        self._last_export_date: date | None = None
        self._alert_system = None
        self._signals_system = None
        
        if ALERT_SYSTEM_ENABLED:
            try:
                github_repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
                self._alert_system = AlertSystem(self.bot, github_repo)
                logger.info("✅ Alert system инициализирован")
            except Exception as e:
                logger.warning(f"Alert system init error: {e}")
        
        if SIGNALS_SYSTEM_ENABLED:
            try:
                github_repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
                self._signals_system = SignalsSystem(self.bot, github_repo)
                logger.info("✅ Signals system инициализирован")
            except Exception as e:
                logger.warning(f"Signals system init error: {e}")
        
        self._auto_tracker = None
        if AUTO_TRACKER_ENABLED:
            try:
                self._auto_tracker = AutoTracker()
                logger.info("✅ Auto tracker инициализирован")
            except Exception as e:
                logger.warning(f"Auto tracker init error: {e}")

    async def start(self):
        self._running = True
        logger.info("⏰ Scheduler запущен")

        tasks = [
            self._daily_digest_loop(),
            self._prediction_checker_loop(),
            self._midnight_reset_loop(),
            self._daily_github_export_loop(),
        ]
        
        if ALERT_SYSTEM_ENABLED and self._alert_system:
            tasks.append(self._alert_checker_loop())
        
        if SIGNALS_SYSTEM_ENABLED and self._signals_system:
            tasks.append(self._signals_checker_loop())
        
        if AUTO_TRACKER_ENABLED and self._auto_tracker:
            tasks.append(self._auto_tracker_loop())
        
        await asyncio.gather(*tasks)

    async def _daily_digest_loop(self):
        """Каждую минуту проверяет — не пора ли слать дайджест подписчикам."""
        while self._running:
            try:
                now = datetime.now()
                current_time = now.strftime("%H:%M")
                subscribers = await get_daily_subscribers()
                for user in subscribers:
                    sub_time = user.get("sub_time", "08:00")
                    if sub_time == current_time:
                        logger.info(f"📬 Отправляю дайджест пользователю {user['user_id']}")
                        try:
                            await self.send_daily(user["user_id"])
                        except Exception as e:
                            logger.warning(f"Ошибка рассылки для {user['user_id']}: {e}")
            except Exception as e:
                logger.error(f"Daily digest loop error: {e}")
            await asyncio.sleep(60)

    async def _prediction_checker_loop(self):
        """Проверяет прогнозы каждые 6 часов."""
        while self._running:
            try:
                logger.info("🔍 Проверяю прогнозы агентов...")
                checked = await self.check_predictions()
                logger.info(f"Проверено прогнозов: {checked}")
            except Exception as e:
                logger.error(f"Prediction checker error: {e}")
            await asyncio.sleep(6 * 3600)

    async def _midnight_reset_loop(self):
        """Сбрасывает счётчики запросов в полночь."""
        while self._running:
            now = datetime.now()
            seconds_to_midnight = (
                (24 - now.hour - 1) * 3600
                + (60 - now.minute - 1) * 60
                + (60 - now.second)
            )
            await asyncio.sleep(seconds_to_midnight)
            try:
                await reset_daily_counts()
                logger.info("🌙 Счётчики запросов сброшены (полночь)")
            except Exception as e:
                logger.error(f"Midnight reset error: {e}")

    async def _daily_github_export_loop(self):
        """
        Экспортирует track record на GitHub ОДИН РАЗ В СУТКИ в 00:05 UTC.

        ИСПРАВЛЕНО: раньше export_now() вызывался после каждого /daily,
        что создавало GitHub коммит → Railway триггерился на новый коммит →
        бесконечный цикл деплоев.

        Теперь:
        - Экспорт только в 00:05 UTC (один раз в сутки)
        - Защита _last_export_date исключает двойной запуск
        - Никаких коммитов от пользовательских запросов
        """
        # Небольшая задержка при старте чтобы БД успела инициализироваться
        await asyncio.sleep(30)

        while self._running:
            try:
                now = datetime.now()
                today = now.date()

                # Экспорт ОТКЛЮЧЕН — теперь вручную
                # Включить: раскомментировать ниже
                # if (now.hour == 0 and now.minute == 5
                #         and self._last_export_date != today):
                #     from github_export import export_to_github
                #     success = await export_to_github()
                #     if success:
                #         self._last_export_date = today
                #         logger.info("✅ Track record экспортирован на GitHub (ежесуточно)")
                #     else:
                #         logger.warning("⚠️ GitHub export не выполнен — проверь GITHUB_TOKEN")
                pass

            except Exception as e:
                logger.error(f"GitHub export error: {e}")

            # Проверяем каждую минуту (синхронизируемся с минутным циклом)
            await asyncio.sleep(60)

    async def _alert_checker_loop(self):
        """Проверяет серию вердиктов и шлёт алерт подписчикам каждые 4 часа."""
        await asyncio.sleep(300)  # ждём 5 минут при старте

        while self._running:
            try:
                if self._alert_system is None:
                    await asyncio.sleep(3600)
                    continue

                subscribers = await get_daily_subscribers()
                if subscribers:
                    sent = await self._alert_system.check_and_alert(subscribers)
                    if sent > 0:
                        logger.info(f"📢 Алерты отправлены: {sent}")
            except Exception as e:
                logger.error(f"Alert checker error: {e}")

            await asyncio.sleep(4 * 3600)  # каждые 4 часа

    async def _signals_checker_loop(self):
        """Проверяет сигналы и отправляет подписчикам каждые 2 часа."""
        await asyncio.sleep(600)  # ждём 10 минут при старте

        while self._running:
            try:
                if self._signals_system is None:
                    await asyncio.sleep(3600)
                    continue

                subscribers = await get_signals_subscribers()
                if subscribers:
                    sent = await self._signals_system.check_and_send_signals(subscribers)
                    if sent > 0:
                        logger.info(f"📡 Сигналы отправлены: {sent}")
            except Exception as e:
                logger.error(f"Signals checker error: {e}")

            await asyncio.sleep(2 * 3600)  # каждые 2 часа

    async def _auto_tracker_loop(self):
        """Проверяет прогнозы в 00:10 UTC (через 10 минут после дайджеста)."""
        await asyncio.sleep(120)  # ждём 2 минуты при старте

        while self._running:
            try:
                now = datetime.now()
                current_time = now.strftime("%H:%M")
                
                # Запускаем в 00:10 UTC каждый день
                if current_time == "00:10":
                    logger.info("🔄 Запускаю авто-проверку прогнозов...")
                    
                    results = await self._auto_tracker.check_all_forecasts()
                    
                    if results:
                        md = self._auto_tracker.generate_markdown(results)
                        await self._auto_tracker.upload_to_github(md, "AUTO_TRACK.md")
                        logger.info(f"✅ Auto track обновлён")
                    
                    # Ждём минуту чтобы не запустить дважды
                    await asyncio.sleep(60)
                    
            except Exception as e:
                logger.error(f"Auto tracker error: {e}")

            # Проверяем каждую минуту
            await asyncio.sleep(60)

    async def export_now(self):
        """
        ИСПРАВЛЕНО: метод оставлен для обратной совместимости,
        но теперь НЕ делает ничего чтобы не триггерить Railway деплои.

        Если нужен ручной экспорт — используй /admin команду или
        запусти github_export.py напрямую локально.
        """
        logger.debug("export_now() вызван но пропущен (отключено для предотвращения Railway loop)")
        pass
