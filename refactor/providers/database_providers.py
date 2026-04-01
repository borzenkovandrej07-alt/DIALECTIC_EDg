"""Database Provider Implementations - Async SQLite Wrapper"""

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class SQLiteProvider:
    """Асинхронный SQLite провайдер для работы с БД"""

    def __init__(
        self,
        db_path: str = "dialectic.db",
        timeout: float = 5.0,
        check_same_thread: bool = False,
    ):
        self.db_path = Path(db_path)
        self.timeout = timeout
        self.check_same_thread = check_same_thread
        self.connection: Optional[sqlite3.Connection] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self) -> None:
        """Подключение к БД"""
        try:
            self.loop = asyncio.get_event_loop()
            # Создаём БД в основном потоке
            self.connection = await self.loop.run_in_executor(
                None,
                sqlite3.connect,
                str(self.db_path),
                self.timeout,
                self.check_same_thread,
            )
            # Включаем внешние ключи
            await self.execute("PRAGMA foreign_keys = ON")
            logger.info(f"✅ SQLite connected: {self.db_path}")
        except Exception as e:
            logger.error(f"❌ SQLite connection failed: {e}")
            self.connection = None

    async def close(self) -> None:
        """Закрытие подключения"""
        if self.connection:
            try:
                await self.loop.run_in_executor(None, self.connection.close)
                logger.info("✅ SQLite connection closed")
            except Exception as e:
                logger.warning(f"SQLite close error: {e}")

    async def execute(self, query: str, params: Tuple = ()) -> None:
        """Выполнение запроса без результата (INSERT, UPDATE, DELETE)"""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            await self.loop.run_in_executor(
                None, self._execute, query, params
            )
        except Exception as e:
            logger.error(f"SQLite execute error: {e}")
            raise

    async def executemany(self, query: str, params_list: List[Tuple]) -> None:
        """Выполнение нескольких запросов"""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            await self.loop.run_in_executor(
                None, self._executemany, query, params_list
            )
        except Exception as e:
            logger.error(f"SQLite executemany error: {e}")
            raise

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
        """Получение одной строки"""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            result = await self.loop.run_in_executor(
                None, self._fetchone, query, params
            )
            return result
        except Exception as e:
            logger.error(f"SQLite fetchone error: {e}")
            raise

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        """Получение всех строк"""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            results = await self.loop.run_in_executor(
                None, self._fetchall, query, params
            )
            return results
        except Exception as e:
            logger.error(f"SQLite fetchall error: {e}")
            raise

    async def fetchcount(self, query: str, params: Tuple = ()) -> int:
        """Получение количества строк"""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            count = await self.loop.run_in_executor(
                None, self._fetchcount, query, params
            )
            return count
        except Exception as e:
            logger.error(f"SQLite fetchcount error: {e}")
            raise

    def _execute(self, query: str, params: Tuple) -> None:
        """Синхронное выполнение запроса"""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()

    def _executemany(self, query: str, params_list: List[Tuple]) -> None:
        """Синхронное выполнение нескольких запросов"""
        cursor = self.connection.cursor()
        cursor.executemany(query, params_list)
        self.connection.commit()

    def _fetchone(self, query: str, params: Tuple) -> Optional[Dict[str, Any]]:
        """Синхронное получение одной строки"""
        cursor = self.connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _fetchall(self, query: str, params: Tuple) -> List[Dict[str, Any]]:
        """Синхронное получение всех строк"""
        cursor = self.connection.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def _fetchcount(self, query: str, params: Tuple) -> int:
        """Синхронное получение COUNT(*)"""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        return result[0] if result else 0

    async def create_table(
        self, table_name: str, columns: Dict[str, str]
    ) -> None:
        """
        Создание таблицы
        columns: {"col_name": "col_type, PRIMARY KEY, ..."}
        """
        cols_def = ", ".join(f"{name} {dtype}" for name, dtype in columns.items())
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def})"
        await self.execute(query)

    async def insert(
        self, table_name: str, data: Dict[str, Any]
    ) -> int:
        """Вставка строки, возвращает ID"""
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" * len(data))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        await self.execute(query, tuple(data.values()))

        # Получаем последний ID
        result = await self.fetchone(f"SELECT last_insert_rowid() as id")
        return result["id"] if result else 0

    async def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        where: str,
        where_params: Tuple = (),
    ) -> int:
        """Обновление строк, возвращает количество затронутых"""
        updates = ", ".join(f"{k} = ?" for k in data.keys())
        query = f"UPDATE {table_name} SET {updates} WHERE {where}"
        params = tuple(data.values()) + where_params
        await self.execute(query, params)

        # Получаем количество затронутых рядов
        result = await self.fetchone("SELECT changes() as count")
        return result["count"] if result else 0

    async def delete(
        self,
        table_name: str,
        where: str,
        where_params: Tuple = (),
    ) -> int:
        """Удаление строк, возвращает количество удалённых"""
        query = f"DELETE FROM {table_name} WHERE {where}"
        await self.execute(query, where_params)

        # Получаем количество удалённых
        result = await self.fetchone("SELECT changes() as count")
        return result["count"] if result else 0

    async def select_by_id(
        self, table_name: str, id_value: Any, id_col: str = "id"
    ) -> Optional[Dict[str, Any]]:
        """Получение строки по ID"""
        query = f"SELECT * FROM {table_name} WHERE {id_col} = ?"
        return await self.fetchone(query, (id_value,))

    async def select_all(self, table_name: str) -> List[Dict[str, Any]]:
        """Получение всех строк таблицы"""
        query = f"SELECT * FROM {table_name}"
        return await self.fetchall(query)

    async def health_check(self) -> bool:
        """Проверка здоровья БД"""
        if not self.connection:
            return False

        try:
            await self.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"SQLite health check failed: {e}")
            return False

    async def vacuum(self) -> None:
        """Оптимизация БД (удаление мусора)"""
        try:
            await self.execute("VACUUM")
            logger.info("✅ Database vacuumed")
        except Exception as e:
            logger.warning(f"Vacuum failed: {e}")

    async def get_size(self) -> int:
        """Получение размера файла БД в байтах"""
        try:
            size = self.db_path.stat().st_size
            return size
        except Exception as e:
            logger.warning(f"Failed to get DB size: {e}")
            return 0

    def __repr__(self) -> str:
        status = "✅" if self.connection else "❌"
        return f"SQLiteProvider({self.db_path}){status}"
