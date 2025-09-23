import logging
from datetime import datetime
from typing import Optional, List
from database import Database, Incident, IncidentStatus
from redis_scheduler import RedisNotificationScheduler
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class IncidentManager:
    db: Database
    notification_manager: RedisNotificationScheduler

    def init(self):
        """Инициализирует менеджер инцидентов"""
        self.db.init_db()

        # RedisNotificationScheduler не требует callback и start

        # Загружаем активные инциденты при запуске
        active_incidents = self.db.get_active_incidents()
        logger.info(
            f"Загружено {len(active_incidents)} активных инцидентов из базы данных"
        )

        # Восстанавливаем уведомления для активных инцидентов
        self.notification_manager.restore_notifications(active_incidents)

        logger.info("✅ Система уведомлений инициализирована")

    def create_incident(
        self, ticket_key: str, channel_id: str, thread_ts: str, author_id: str
    ) -> Optional[Incident]:
        """Создает новый инцидент"""
        incident = Incident(
            ticket_key=ticket_key,
            channel_id=channel_id,
            thread_ts=thread_ts,
            author_id=author_id,
            status=IncidentStatus.CREATED,
            created_at=datetime.now(),
            last_notification=datetime.now(),
        )

        if self.db.add_incident(incident):
            logger.info(f"Создан инцидент {ticket_key}")
            return incident
        else:
            logger.error(f"Не удалось создать инцидент {ticket_key}")
            return None

    def get_incident(self, ticket_key: str) -> Optional[Incident]:
        """Получает инцидент по ключу тикета"""
        return self.db.get_incident(ticket_key)

    def get_incident_by_thread(
        self, channel_id: str, thread_ts: str
    ) -> Optional[Incident]:
        """Получает инцидент по каналу и времени треда"""
        return self.db.get_incident_by_thread(channel_id, thread_ts)

    def take_incident_in_progress(self, ticket_key: str, assigned_to: str) -> bool:
        """Переводит инцидент в статус 'В работе'"""
        incident = self.get_incident(ticket_key)
        if incident and incident.status == IncidentStatus.CREATED:
            incident.status = IncidentStatus.ASSIGNED
            incident.assigned_to = assigned_to
            if self.db.update_incident(incident):
                # Отменяем уведомления о создании
                self.notification_manager.cancel_notification(ticket_key, "default")
                logger.info(
                    f"Инцидент {ticket_key} взят в работу пользователем {assigned_to}"
                )
                return True
        logger.warning(
            f"❌ Не удалось взять инцидент {ticket_key} в работу: инцидент={incident is not None}, статус={incident.status if incident else 'None'}"
        )
        return False

    def set_awaiting_response(self, ticket_key: str) -> bool:
        """Переводит инцидент в статус 'Ожидание ответа'"""
        incident = self.get_incident(ticket_key)
        if incident and (
            incident.status == IncidentStatus.ASSIGNED
            or incident.status == IncidentStatus.FROZEN
        ):
            incident.status = IncidentStatus.AWAITING_RESPONSE
            if self.db.update_incident(incident):
                # Отменяем все текущие уведомления
                self.notification_manager.cancel_all_notifications(ticket_key)
                logger.info(
                    f"Инцидент {ticket_key} переведен в статус 'ожидание ответа'"
                )
                return True
            logger.error(
                f"❌ Ошибка при обновлении инцидента {ticket_key} в базе данных"
            )
        else:
            logger.warning(
                f"❌ Не удалось перевести инцидент {ticket_key} в статус 'ожидание ответа': инцидент={incident is not None}, статус={incident.status if incident else 'None'}"
            )
        return False

    def close_incident(self, ticket_key: str) -> bool:
        """Закрывает инцидент"""
        incident = self.get_incident(ticket_key)
        if incident:
            incident.status = IncidentStatus.CLOSED
            if self.db.update_incident(incident):
                # Отменяем все уведомления
                self.notification_manager.cancel_all_notifications(ticket_key)
                logger.info(f"Инцидент {ticket_key} закрыт")
                return True
        return False

    def freeze_incident(self, ticket_key: str) -> bool:
        """Заморозить инцидент (не напоминать)"""
        incident = self.get_incident(ticket_key)
        if incident:
            incident.status = IncidentStatus.FROZEN
            if self.db.update_incident(incident):
                # Отменяем все уведомления
                self.notification_manager.cancel_all_notifications(ticket_key)
                logger.info(f"Инцидент {ticket_key} заморожен")
                return True
        return False

    def start_notification_task(
        self,
        ticket_key: str,
        incident_data: dict,
        interval_minutes: int,
        notification_type: str = "default",
    ):
        """Запускает задачу уведомлений для инцидента"""
        self.notification_manager.schedule_notification(
            ticket_key, incident_data, interval_minutes, notification_type
        )

    def stop_notification_task(
        self, ticket_key: str, notification_type: str = "default"
    ):
        """Останавливает задачу уведомлений для инцидента"""
        self.notification_manager.cancel_notification(ticket_key, notification_type)

    def get_all_incidents(self) -> List[Incident]:
        """Получает все инциденты"""
        return self.db.get_all_incidents()

    def get_active_incidents(self) -> List[Incident]:
        """Получает все активные инциденты"""
        return self.db.get_active_incidents()

    def get_incidents_by_status(self, status: IncidentStatus) -> List[Incident]:
        """Получает инциденты по статусу"""
        all_incidents = self.db.get_all_incidents()
        return [incident for incident in all_incidents if incident.status == status]
