import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Callable
import redis
from config import Config

logger = logging.getLogger(__name__)


class NotificationManager:
    def __init__(self):
        self.redis = None
        self.notification_callback: Optional[Callable] = None

    async def init(self):
        """Инициализирует подключение к Redis"""
        try:
            self.redis = redis.from_url(Config.REDIS_URL, db=Config.REDIS_DB)
            self.redis.ping()
            logger.info("Подключение к Redis установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
            raise

    async def close(self):
        """Закрывает подключение к Redis"""
        if self.redis:
            self.redis.close()
            logger.info("Подключение к Redis закрыто")

    def set_notification_callback(self, callback: Callable):
        """Устанавливает callback для уведомлений"""
        self.notification_callback = callback

    async def schedule_notification(
        self,
        ticket_key: str,
        incident_data: dict,
        interval_minutes: int,
        notification_type: str = "default",
    ):
        """Планирует уведомление через Redis"""
        try:
            # Создаем ключ для уведомления
            notification_key = f"notification:{ticket_key}:{notification_type}"

            # Данные для уведомления
            notification_data = {
                "ticket_key": ticket_key,
                "incident_data": incident_data,
                "notification_type": notification_type,
                "scheduled_at": datetime.now().isoformat(),
                "interval_minutes": interval_minutes,
            }

            # Сохраняем данные уведомления
            self.redis.setex(
                notification_key,
                interval_minutes * 60,  # TTL в секундах
                json.dumps(notification_data),
            )

            # Добавляем в список активных уведомлений
            self.redis.sadd("active_notifications", notification_key)

            logger.info(
                f"Запланировано уведомление для {ticket_key} через {interval_minutes} минут"
            )

        except Exception as e:
            logger.error(f"Ошибка при планировании уведомления для {ticket_key}: {e}")

    async def cancel_notification(
        self, ticket_key: str, notification_type: str = "default"
    ):
        """Отменяет уведомление"""
        try:
            notification_key = f"notification:{ticket_key}:{notification_type}"

            # Удаляем уведомление
            self.redis.delete(notification_key)
            self.redis.srem("active_notifications", notification_key)

            logger.info(f"Отменено уведомление для {ticket_key}")

        except Exception as e:
            logger.error(f"Ошибка при отмене уведомления для {ticket_key}: {e}")

    async def cancel_all_notifications(self, ticket_key: str):
        """Отменяет все уведомления для тикета"""
        try:
            # Получаем все ключи уведомлений для тикета
            pattern = f"notification:{ticket_key}:*"
            keys = self.redis.keys(pattern)

            if keys:
                # Удаляем все уведомления
                self.redis.delete(*keys)
                self.redis.srem("active_notifications", *keys)

                logger.info(f"Отменены все уведомления для {ticket_key}")

        except Exception as e:
            logger.error(f"Ошибка при отмене всех уведомлений для {ticket_key}: {e}")

    async def check_expired_notifications(self):
        """Проверяет и обрабатывает истекшие уведомления"""
        try:
            # Получаем все активные уведомления
            active_notifications = self.redis.smembers("active_notifications")
            logger.debug(f"🔍 Найдено {len(active_notifications)} активных уведомлений")

            for notification_key in active_notifications:
                notification_key = notification_key.decode("utf-8")
                logger.debug(f"🔍 Проверяем уведомление: {notification_key}")

                # Проверяем, существует ли уведомление
                if not self.redis.exists(notification_key):
                    # Уведомление истекло, удаляем из списка активных
                    self.redis.srem("active_notifications", notification_key)

                    # Извлекаем данные уведомления из ключа
                    parts = notification_key.split(":")
                    if len(parts) >= 3:
                        ticket_key = parts[1]
                        notification_type = parts[2]

                        logger.info(
                            f"⏰ Уведомление истекло для {ticket_key} (тип: {notification_type})"
                        )

                        # Вызываем callback для обработки уведомления
                        if self.notification_callback:
                            await self.notification_callback(
                                ticket_key, notification_type
                            )

                        logger.info(
                            f"✅ Обработано истекшее уведомление для {ticket_key}"
                        )

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке истекших уведомлений: {e}")

    async def start_notification_loop(self):
        """Запускает цикл проверки уведомлений"""
        logger.info("🔄 Запущен цикл проверки уведомлений")

        while True:
            try:
                await self.check_expired_notifications()
                logger.debug("🔍 Проверка уведомлений завершена")
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле уведомлений: {e}")
                await asyncio.sleep(60)  # При ошибке ждем дольше

    async def get_active_notifications(self) -> list:
        """Получает список активных уведомлений"""
        try:
            active_notifications = self.redis.smembers("active_notifications")
            return [key.decode("utf-8") for key in active_notifications]
        except Exception as e:
            logger.error(f"Ошибка при получении активных уведомлений: {e}")
            return []

    async def get_notification_data(self, notification_key: str) -> Optional[dict]:
        """Получает данные уведомления"""
        try:
            data = self.redis.get(notification_key)
            if data:
                return json.loads(data.decode("utf-8"))
            return None
        except Exception as e:
            logger.error(
                f"Ошибка при получении данных уведомления {notification_key}: {e}"
            )
            return None
