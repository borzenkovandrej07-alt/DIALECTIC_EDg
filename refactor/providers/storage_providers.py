"""Report Storage Provider Implementation"""

import asyncio
import json
import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ReportMetadata:
    """Метаданные отчёта"""
    report_id: str
    title: str
    created_at: datetime
    updated_at: datetime
    author: str
    tags: List[str]
    version: int = 1


class JSONReportStorage:
    """JSON-файловое хранилище отчётов"""

    def __init__(self, storage_dir: str = "reports"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.index_file = self.storage_dir / "_index.json"

    def _get_report_file(self, report_id: str) -> Path:
        """Получить путь файла отчёта"""
        safe_id = report_id.replace("/", "_").replace(":", "_")[:100]
        return self.storage_dir / f"{safe_id}.json"

    def _load_index(self) -> Dict[str, Dict[str, Any]]:
        """Загрузить индекс отчётов"""
        if self.index_file.exists():
            try:
                with open(self.index_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Index load error: {e}")
        return {}

    def _save_index(self, index: Dict[str, Dict[str, Any]]) -> None:
        """Сохранить индекс отчётов"""
        try:
            with open(self.index_file, "w") as f:
                json.dump(index, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Index save error: {e}")

    async def save_report(
        self,
        report_id: str,
        title: str,
        content: Dict[str, Any],
        author: str = "system",
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Сохранить отчёт"""
        try:
            report_file = self._get_report_file(report_id)
            now = datetime.now().isoformat()

            report_data = {
                "id": report_id,
                "title": title,
                "content": content,
                "metadata": {
                    "created_at": now,
                    "updated_at": now,
                    "author": author,
                    "tags": tags or [],
                    "version": 1,
                },
            }

            # Сохраняем файл
            with open(report_file, "w") as f:
                json.dump(report_data, f, indent=2, default=str)

            # Обновляем индекс
            index = self._load_index()
            index[report_id] = {
                "title": title,
                "author": author,
                "created_at": now,
                "tags": tags or [],
            }
            self._save_index(index)

            logger.info(f"✅ Report saved: {report_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            return False

    async def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Получить отчёт"""
        try:
            report_file = self._get_report_file(report_id)

            if not report_file.exists():
                return None

            with open(report_file, "r") as f:
                report_data = json.load(f)

            logger.debug(f"Report loaded: {report_id}")
            return report_data

        except Exception as e:
            logger.warning(f"Failed to load report: {e}")
            return None

    async def list_reports(
        self, author: Optional[str] = None, tag: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Получить список отчётов с фильтрацией"""
        index = self._load_index()
        results = []

        for report_id, metadata in index.items():
            # Фильтруем по автору
            if author and metadata.get("author") != author:
                continue

            # Фильтруем по тегу
            if tag and tag not in metadata.get("tags", []):
                continue

            results.append(
                {
                    "id": report_id,
                    "title": metadata.get("title", ""),
                    "author": metadata.get("author", ""),
                    "created_at": metadata.get("created_at", ""),
                    "tags": metadata.get("tags", []),
                }
            )

        return results

    async def update_report(
        self,
        report_id: str,
        content: Optional[Dict[str, Any]] = None,
        title: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Обновить отчёт"""
        try:
            report = await self.get_report(report_id)
            if not report:
                return False

            # Обновляем поля
            if content is not None:
                report["content"] = content

            if title is not None:
                report["title"] = title

            if tags is not None:
                report["metadata"]["tags"] = tags

            report["metadata"]["updated_at"] = datetime.now().isoformat()
            report["metadata"]["version"] += 1

            # Сохраняем обновления
            report_file = self._get_report_file(report_id)
            with open(report_file, "w") as f:
                json.dump(report, f, indent=2, default=str)

            # Обновляем индекс
            index = self._load_index()
            index[report_id]["updated_at"] = report["metadata"]["updated_at"]
            if tags is not None:
                index[report_id]["tags"] = tags
            self._save_index(index)

            logger.info(f"✅ Report updated: {report_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to update report: {e}")
            return False

    async def delete_report(self, report_id: str) -> bool:
        """Удалить отчёт"""
        try:
            report_file = self._get_report_file(report_id)

            if report_file.exists():
                report_file.unlink()

            # Удаляем из индекса
            index = self._load_index()
            if report_id in index:
                del index[report_id]
                self._save_index(index)

            logger.info(f"✅ Report deleted: {report_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete report: {e}")
            return False

    async def archive_report(self, report_id: str, archive_dir: str = "archive") -> bool:
        """Архивировать отчёт"""
        try:
            report_file = self._get_report_file(report_id)

            if not report_file.exists():
                return False

            archive_path = Path(archive_dir)
            archive_path.mkdir(exist_ok=True)

            # Копируем в архив
            archive_file = archive_path / report_file.name
            archive_file.write_text(report_file.read_text())

            # Удаляем оригинал
            report_file.unlink()

            # Удаляем из индекса
            index = self._load_index()
            if report_id in index:
                del index[report_id]
                self._save_index(index)

            logger.info(f"✅ Report archived: {report_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to archive report: {e}")
            return False

    async def search_reports(self, query: str) -> List[Dict[str, Any]]:
        """Поиск отчётов по названию/тегам"""
        index = self._load_index()
        results = []

        query_lower = query.lower()

        for report_id, metadata in index.items():
            title = metadata.get("title", "").lower()
            tags = metadata.get("tags", [])

            # Проверяем совпадение в названии или тегах
            if query_lower in title or any(
                query_lower in tag.lower() for tag in tags
            ):
                results.append(
                    {
                        "id": report_id,
                        "title": metadata.get("title", ""),
                        "author": metadata.get("author", ""),
                        "created_at": metadata.get("created_at", ""),
                        "tags": tags,
                    }
                )

        return results

    async def get_stats(self) -> Dict[str, Any]:
        """Получить статистику хранилища"""
        index = self._load_index()
        total_size = sum(f.stat().st_size for f in self.storage_dir.glob("*.json"))

        authors = set()
        all_tags = set()

        for metadata in index.values():
            authors.add(metadata.get("author", ""))
            all_tags.update(metadata.get("tags", []))

        return {
            "total_reports": len(index),
            "total_authors": len(authors),
            "unique_tags": len(all_tags),
            "storage_size_bytes": total_size,
            "storage_size_mb": round(total_size / 1024 / 1024, 2),
        }

    def __repr__(self) -> str:
        report_count = len(self._load_index())
        return f"JSONReportStorage({self.storage_dir}, {report_count} reports)"
