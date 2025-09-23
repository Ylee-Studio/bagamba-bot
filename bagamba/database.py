import sqlite3
import logging
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class IncidentStatus(Enum):
    CREATED = "created"
    ASSIGNED = "assigned"
    AWAITING_RESPONSE = "awaiting_response"
    CLOSED = "closed"
    FROZEN = "frozen"


@dataclass
class Incident:
    ticket_key: str
    channel_id: str
    thread_ts: str
    author_id: str
    status: IncidentStatus
    assigned_to: Optional[str] = None
    created_at: datetime | None = None
    last_notification: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class Database:
    db_path: str = "incidents.db"

    def init_db(self):
        """Инициализирует базу данных и создает таблицы"""
        with sqlite3.connect(self.db_path) as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    ticket_key TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    author_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    assigned_to TEXT,
                    created_at TEXT NOT NULL,
                    last_notification TEXT
                )
            """)
            db.commit()
            logger.info("База данных инициализирована")

    def add_incident(self, incident: Incident) -> bool:
        """Добавляет новый инцидент в базу данных"""
        try:
            with sqlite3.connect(self.db_path) as db:
                db.execute(
                    """
                    INSERT INTO incidents (ticket_key, channel_id, thread_ts, author_id, status, assigned_to, created_at, last_notification)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        incident.ticket_key,
                        incident.channel_id,
                        incident.thread_ts,
                        incident.author_id,
                        incident.status.value,
                        incident.assigned_to,
                        incident.created_at.isoformat()
                        if incident.created_at
                        else None,
                        incident.last_notification.isoformat()
                        if incident.last_notification
                        else None,
                    ),
                )
                db.commit()
                logger.info(f"Инцидент {incident.ticket_key} добавлен в базу данных")
                return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении инцидента {incident.ticket_key}: {e}")
            return False

    def get_incident(self, ticket_key: str) -> Optional[Incident]:
        """Получает инцидент по ключу тикета"""
        try:
            with sqlite3.connect(self.db_path) as db:
                cursor = db.execute(
                    """
                    SELECT ticket_key, channel_id, thread_ts, author_id, status, 
                           assigned_to, created_at, last_notification
                    FROM incidents WHERE ticket_key = ?
                """,
                    (ticket_key,),
                )
                row = cursor.fetchone()

                if row:
                    return Incident(
                        ticket_key=row[0],
                        channel_id=row[1],
                        thread_ts=row[2],
                        author_id=row[3],
                        status=IncidentStatus(row[4]),
                        assigned_to=row[5],
                        created_at=datetime.fromisoformat(row[6]) if row[6] else None,
                        last_notification=datetime.fromisoformat(row[7])
                        if row[7]
                        else None,
                    )
                return None
        except Exception as e:
            logger.error(f"Ошибка при получении инцидента {ticket_key}: {e}")
            return None

    def get_incident_by_thread(
        self, channel_id: str, thread_ts: str
    ) -> Optional[Incident]:
        """Получает инцидент по каналу и времени треда"""
        try:
            with sqlite3.connect(self.db_path) as db:
                cursor = db.execute(
                    """
                    SELECT ticket_key, channel_id, thread_ts, author_id, status, 
                           assigned_to, created_at, last_notification
                    FROM incidents WHERE channel_id = ? AND thread_ts = ?
                """,
                    (channel_id, thread_ts),
                )
                row = cursor.fetchone()

                if row:
                    return Incident(
                        ticket_key=row[0],
                        channel_id=row[1],
                        thread_ts=row[2],
                        author_id=row[3],
                        status=IncidentStatus(row[4]),
                        assigned_to=row[5],
                        created_at=datetime.fromisoformat(row[6]) if row[6] else None,
                        last_notification=datetime.fromisoformat(row[7])
                        if row[7]
                        else None,
                    )
                return None
        except Exception as e:
            logger.error(
                f"Ошибка при получении инцидента по треду {channel_id}/{thread_ts}: {e}"
            )
            return None

    def update_incident(self, incident: Incident) -> bool:
        """Обновляет инцидент в базе данных"""
        try:
            with sqlite3.connect(self.db_path) as db:
                # Проверяем текущий статус перед обновлением
                cursor = db.execute(
                    """
                    SELECT status FROM incidents WHERE ticket_key = ?
                """,
                    (incident.ticket_key,),
                )
                current_status = cursor.fetchone()

                if not current_status:
                    logger.error(
                        f"Инцидент {incident.ticket_key} не найден в базе данных"
                    )
                    return False

                # Выполняем обновление только если статус изменился
                cursor = db.execute(
                    """
                    UPDATE incidents 
                    SET status = ?, assigned_to = ?, last_notification = ?
                    WHERE ticket_key = ? AND status = ?
                """,
                    (
                        incident.status.value,
                        incident.assigned_to,
                        incident.last_notification.isoformat()
                        if incident.last_notification
                        else None,
                        incident.ticket_key,
                        current_status[0],  # Текущий статус
                    ),
                )

                if cursor.rowcount == 0:
                    logger.warning(
                        f"Инцидент {incident.ticket_key} не обновлен - статус изменился с {current_status[0]} на {incident.status.value}"
                    )
                    return False

                db.commit()
                logger.info(f"Инцидент {incident.ticket_key} обновлен в базе данных")
                return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении инцидента {incident.ticket_key}: {e}")
            return False

    def get_all_incidents(self) -> List[Incident]:
        """Получает все инциденты"""
        try:
            with sqlite3.connect(self.db_path) as db:
                cursor = db.execute("""
                    SELECT ticket_key, channel_id, thread_ts, author_id, status, 
                           assigned_to, created_at, last_notification
                    FROM incidents ORDER BY created_at DESC
                """)
                rows = cursor.fetchall()

                incidents = []
                for row in rows:
                    incidents.append(
                        Incident(
                            ticket_key=row[0],
                            channel_id=row[1],
                            thread_ts=row[2],
                            author_id=row[3],
                            status=IncidentStatus(row[4]),
                            assigned_to=row[5],
                            created_at=datetime.fromisoformat(row[6])
                            if row[6]
                            else None,
                            last_notification=datetime.fromisoformat(row[7])
                            if row[7]
                            else None,
                        )
                    )
                return incidents
        except Exception as e:
            logger.error(f"Ошибка при получении всех инцидентов: {e}")
            return []

    def get_active_incidents(self) -> List[Incident]:
        """Получает все активные инциденты (не закрытые)"""
        try:
            with sqlite3.connect(self.db_path) as db:
                cursor = db.execute(
                    """
                    SELECT ticket_key, channel_id, thread_ts, author_id, status, 
                           assigned_to, created_at, last_notification
                    FROM incidents 
                    WHERE status != ? 
                    ORDER BY created_at DESC
                """,
                    (IncidentStatus.CLOSED.value,),
                )
                rows = cursor.fetchall()

                incidents = []
                for row in rows:
                    incidents.append(
                        Incident(
                            ticket_key=row[0],
                            channel_id=row[1],
                            thread_ts=row[2],
                            author_id=row[3],
                            status=IncidentStatus(row[4]),
                            assigned_to=row[5],
                            created_at=datetime.fromisoformat(row[6])
                            if row[6]
                            else None,
                            last_notification=datetime.fromisoformat(row[7])
                            if row[7]
                            else None,
                        )
                    )
                return incidents
        except Exception as e:
            logger.error(f"Ошибка при получении активных инцидентов: {e}")
            return []
