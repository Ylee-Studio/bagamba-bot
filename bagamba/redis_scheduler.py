import logging
import json
from datetime import datetime, timedelta
import redis
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RedisClient(redis.Redis):
    url: str
    db: int
    client: redis.Redis = field(init=False)

    def __post_init__(self):
        super().__init__(
            **redis.Redis.from_url(
                self.url, db=self.db
            ).connection_pool.connection_kwargs
        )


@dataclass
class RedisNotificationScheduler:
    """Упрощенный планировщик уведомлений через Redis"""

    redis_client: redis.Redis
    notification_queue: str = "notifications:queue"
    notification_prefix: str = "notification:"

    def schedule_notification(
        self,
        ticket_key: str,
        incident_data: dict,
        interval_minutes: int,
        notification_type: str = "default",
    ):
        """Планирует уведомление через Redis queue"""
        try:
            notification_key = (
                f"{self.notification_prefix}{ticket_key}:{notification_type}"
            )

            # Проверяем, есть ли уже уведомление для этого тикета
            if self.redis_client.exists(notification_key):
                logger.warning(
                    f"⚠️ Уведомление для {ticket_key}:{notification_type} уже существует, отменяем предыдущее"
                )
                self.cancel_notification(ticket_key, notification_type)

            # Вычисляем время следующего уведомления
            scheduled_time = datetime.now() + timedelta(minutes=interval_minutes)

            # Создаем данные уведомления
            notification_data = {
                "ticket_key": ticket_key,
                "incident_data": incident_data,
                "notification_type": notification_type,
                "interval_minutes": interval_minutes,
                "scheduled_time": scheduled_time.isoformat(),
                "created_at": datetime.now().isoformat(),
            }

            # Добавляем в очередь уведомлений
            self.redis_client.lpush(
                self.notification_queue, json.dumps(notification_data)
            )

            # Также сохраняем как отдельный ключ для отслеживания
            self.redis_client.set(
                notification_key, json.dumps(notification_data), ex=86400
            )  # TTL 24 часа

            logger.info(
                f"⏰ Запланировано уведомление для {ticket_key} через {interval_minutes} минут (время: {scheduled_time})"
            )

        except Exception as e:
            logger.error(
                f"❌ Ошибка при планировании уведомления для {ticket_key}: {e}"
            )

    def cancel_notification(self, ticket_key: str, notification_type: str = "default"):
        """Отменяет уведомление"""
        try:
            notification_key = (
                f"{self.notification_prefix}{ticket_key}:{notification_type}"
            )

            # Удаляем из Redis
            deleted_count = self.redis_client.delete(notification_key)

            # Также удаляем из очереди все уведомления для этого тикета
            self._remove_from_queue(ticket_key, notification_type)

            if deleted_count > 0:
                logger.info(f"❌ Отменено уведомление для {ticket_key}")

        except Exception as e:
            logger.error(f"❌ Ошибка при отмене уведомления для {ticket_key}: {e}")

    def _remove_from_queue(self, ticket_key: str, notification_type: str):
        """Удаляет уведомления из очереди"""
        try:
            # Получаем все элементы из очереди
            queue_items = self.redis_client.lrange(self.notification_queue, 0, -1)

            # Фильтруем элементы, оставляя только те, которые не относятся к отменяемому уведомлению
            filtered_items = []
            removed_count = 0

            for item in queue_items:
                try:
                    notification_data = json.loads(item)
                    if (
                        notification_data.get("ticket_key") == ticket_key
                        and notification_data.get("notification_type")
                        == notification_type
                    ):
                        removed_count += 1
                        logger.info(
                            f"🗑️ Удалено уведомление из очереди: {ticket_key}:{notification_type}"
                        )
                    else:
                        filtered_items.append(item)
                except json.JSONDecodeError:
                    # Если элемент не является валидным JSON, оставляем его
                    filtered_items.append(item)

            # Очищаем очередь и добавляем отфильтрованные элементы
            if removed_count > 0:
                self.redis_client.delete(self.notification_queue)
                if filtered_items:
                    self.redis_client.lpush(self.notification_queue, *filtered_items)
                logger.info(
                    f"✅ Удалено {removed_count} уведомлений из очереди для {ticket_key}:{notification_type}"
                )

        except Exception as e:
            logger.error(
                f"❌ Ошибка при удалении из очереди {ticket_key}:{notification_type}: {e}"
            )

    def cancel_all_notifications(self, ticket_key: str):
        """Отменяет все уведомления для тикета"""
        try:
            # Ищем все ключи для данного тикета
            notification_pattern = f"{self.notification_prefix}{ticket_key}:*"
            notification_keys = self.redis_client.keys(notification_pattern)

            if notification_keys:
                deleted_count = self.redis_client.delete(*notification_keys)
                logger.info(
                    f"❌ Отменены все уведомления для {ticket_key} ({deleted_count} ключей)"
                )

            # Также удаляем все уведомления из очереди для этого тикета
            self._remove_all_from_queue(ticket_key)

        except Exception as e:
            logger.error(f"❌ Ошибка при отмене всех уведомлений для {ticket_key}: {e}")

    def _remove_all_from_queue(self, ticket_key: str):
        """Удаляет все уведомления для тикета из очереди"""
        try:
            # Получаем все элементы из очереди
            queue_items = self.redis_client.lrange(self.notification_queue, 0, -1)

            # Фильтруем элементы, оставляя только те, которые не относятся к отменяемому тикету
            filtered_items = []
            removed_count = 0

            for item in queue_items:
                try:
                    notification_data = json.loads(item)
                    if notification_data.get("ticket_key") == ticket_key:
                        removed_count += 1
                        logger.info(f"🗑️ Удалено уведомление из очереди: {ticket_key}")
                    else:
                        filtered_items.append(item)
                except json.JSONDecodeError:
                    # Если элемент не является валидным JSON, оставляем его
                    filtered_items.append(item)

            # Очищаем очередь и добавляем отфильтрованные элементы
            if removed_count > 0:
                self.redis_client.delete(self.notification_queue)
                if filtered_items:
                    self.redis_client.lpush(self.notification_queue, *filtered_items)
                logger.info(
                    f"✅ Удалено {removed_count} уведомлений из очереди для {ticket_key}"
                )

        except Exception as e:
            logger.error(
                f"❌ Ошибка при удалении всех уведомлений из очереди для {ticket_key}: {e}"
            )

    def get_notification_stats(self) -> dict:
        """Получает статистику уведомлений"""
        try:
            notification_count = len(
                self.redis_client.keys(f"{self.notification_prefix}*")
            )
            queue_length = self.redis_client.llen(self.notification_queue)

            return {
                "total_notifications": notification_count,
                "queue_length": queue_length,
                "redis_connected": self.redis_client.ping(),
            }

        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики: {e}")
            return {
                "total_notifications": 0,
                "queue_length": 0,
                "redis_connected": False,
            }

    def restore_notifications(self, incidents):
        """Восстанавливает уведомления для активных инцидентов при запуске"""
        try:
            from database import IncidentStatus

            for incident in incidents:
                if incident.status == IncidentStatus.CREATED:
                    incident_data = {
                        "ticket_key": incident.ticket_key,
                        "channel_id": incident.channel_id,
                        "thread_ts": incident.thread_ts,
                        "author_id": incident.author_id,
                        "status": incident.status.value,
                        "assigned_to": incident.assigned_to,
                        "created_at": (
                            incident.created_at.isoformat()
                            if incident.created_at
                            else datetime.now().isoformat()
                        ),
                        "last_notification": (
                            incident.last_notification.isoformat()
                            if incident.last_notification
                            else None
                        ),
                    }

                    # Восстанавливаем уведомление
                    self.schedule_notification(
                        incident.ticket_key, incident_data, 5, "default"
                    )
                    logger.info(
                        f"🔄 Восстановлено уведомление для {incident.ticket_key}"
                    )

        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении уведомлений: {e}")
