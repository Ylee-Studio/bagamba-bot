#!/usr/bin/env python3
"""
Redis Worker для отправки уведомлений
"""

import sys
import time
import json
import logging
import signal
from datetime import datetime, timedelta
from typing import Optional
import redis
from dataclasses import dataclass
from config import Config
from slack_sdk import WebClient
from database import Database
from redis_scheduler import RedisClient

from duty_manager import DutyManager

# Загружаем переменные окружения

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


@dataclass
class NotificationSender:
    worker_client: WebClient
    duty_manager: DutyManager

    def __call__(self, incident_data: dict, notification_type: str):
        try:
            ticket_key = incident_data["ticket_key"]
            channel_id = incident_data["channel_id"]
            thread_ts = incident_data["thread_ts"]
            author_id = incident_data["author_id"]

            logger.info(
                f"🔔 Отправка уведомления для {ticket_key} (тип: {notification_type})"
            )

            if notification_type == "default":
                # Уведомление дежурному о необработанном инциденте
                # Инициализируем duty_manager если нужно

                current_duty = self.duty_manager.get_current_duty_person()
                if current_duty:
                    message = f"<@{current_duty.slack_id}> Пожалуйста, возьмите инцидент в работу"
                    self.worker_client.chat_postMessage(
                        channel=channel_id, thread_ts=thread_ts, text=message
                    )
                    logger.info(
                        f"📤 Отправлено уведомление дежурному {current_duty.name} для {ticket_key}"
                    )
                else:
                    logger.warning(f"⚠️ Дежурный не найден для уведомления {ticket_key}")

            elif notification_type == "awaiting_response":
                # Уведомление автору о том, что ждем ответа
                message = "🧘 Ожидание ответа"
                self.worker_client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text=f"<@{author_id}> {message}",
                )
                logger.info(
                    f"📤 Отправлено уведомление автору {author_id} для {ticket_key}"
                )

            else:
                logger.warning(f"⚠️ Неизвестный тип уведомления: {notification_type}")

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке уведомления из worker'а: {e}")


@dataclass
class NotificationWorker:
    """Worker для обработки уведомлений из Redis"""

    redis_client: redis.Redis
    db: Database
    send_notification_sync_from_worker: NotificationSender
    notification_queue = "notifications:queue"
    notification_prefix = "notification:"
    running: bool = True

    def __post_init__(self):
        # Обработчики сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        logger.info(f"🛑 Получен сигнал {signum}, завершаем работу...")
        self.running = False

    def _get_incident_status(self, ticket_key: str) -> Optional[str]:
        """Получает текущий статус инцидента из базы данных"""
        try:
            incident = self.db.get_incident(ticket_key)
            if incident:
                return incident.status.value
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статуса инцидента {ticket_key}: {e}")
            return None

    def _send_notification(self, incident_data: dict, notification_type: str):
        """Отправляет уведомление в Slack"""
        try:
            self.send_notification_sync_from_worker(incident_data, notification_type)
            logger.info(
                f"✅ Уведомление отправлено для {incident_data['ticket_key']} (тип: {notification_type})"
            )

        except Exception as e:
            logger.error(
                f"❌ Ошибка при отправке уведомления для {incident_data.get('ticket_key', 'unknown')}: {e}"
            )

    def _process_notification(self, notification_data: dict):
        """Обрабатывает одно уведомление"""
        try:
            ticket_key = notification_data["ticket_key"]
            incident_data = notification_data["incident_data"]
            notification_type = notification_data["notification_type"]

            logger.info(
                f"🔔 Обрабатываем уведомление для {ticket_key} (тип: {notification_type})"
            )

            # Проверяем текущий статус инцидента
            current_status = self._get_incident_status(ticket_key)
            if not current_status:
                logger.warning(
                    f"⚠️ Инцидент {ticket_key} не найден в базе данных, пропускаем уведомление"
                )
                return

            # Проверяем, нужно ли отправлять уведомление
            if current_status.upper() == "CLOSED":
                logger.info(f"✅ Инцидент {ticket_key} закрыт, отменяем уведомления")
                self._cancel_notification(ticket_key, notification_type)
                return

            if current_status.upper() == "FROZEN":
                logger.info(f"❄️ Инцидент {ticket_key} заморожен, отменяем уведомления")
                self._cancel_notification(ticket_key, notification_type)
                return

            # Обновляем данные инцидента
            incident_data["status"] = current_status

            # Отправляем уведомление
            self._send_notification(incident_data, notification_type)

            # Планируем следующее уведомление, если инцидент все еще активен
            if current_status.upper() in ["CREATED", "AWAITING_RESPONSE"]:
                self._schedule_next_notification(notification_data)
            else:
                logger.info(
                    f"🛑 Инцидент {ticket_key} в статусе {current_status}, прекращаем уведомления"
                )
                self._cancel_notification(ticket_key, notification_type)

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке уведомления: {e}")

    def _schedule_next_notification(self, notification_data: dict):
        """Планирует следующее уведомление"""
        try:
            # Обновляем время следующего уведомления
            next_time = datetime.now() + timedelta(
                minutes=notification_data["interval_minutes"]
            )
            notification_data["scheduled_time"] = next_time.isoformat()
            notification_data["created_at"] = datetime.now().isoformat()

            # Добавляем в очередь
            self.redis_client.lpush(
                self.notification_queue, json.dumps(notification_data)
            )

            # Обновляем ключ
            notification_key = f"{self.notification_prefix}{notification_data['ticket_key']}:{notification_data['notification_type']}"
            self.redis_client.set(
                notification_key, json.dumps(notification_data), ex=86400
            )

            logger.info(
                f"⏰ Запланировано следующее уведомление для {notification_data['ticket_key']} на {next_time}"
            )

        except Exception as e:
            logger.error(f"❌ Ошибка при планировании следующего уведомления: {e}")

    def _cancel_notification(self, ticket_key: str, notification_type: str):
        """Отменяет уведомление"""
        try:
            notification_key = (
                f"{self.notification_prefix}{ticket_key}:{notification_type}"
            )
            self.redis_client.delete(notification_key)

            # Также удаляем из очереди все уведомления для этого тикета
            self._remove_from_queue(ticket_key, notification_type)

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
                            f"🗑️ Worker удалил уведомление из очереди: {ticket_key}:{notification_type}"
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
                    f"✅ Worker удалил {removed_count} уведомлений из очереди для {ticket_key}:{notification_type}"
                )

        except Exception as e:
            logger.error(
                f"❌ Ошибка при удалении из очереди {ticket_key}:{notification_type}: {e}"
            )

    def run(self):
        """Основной цикл worker'а"""
        logger.info("🚀 NotificationWorker запущен")

        while self.running:
            try:
                # Получаем уведомление из очереди (блокирующий вызов с таймаутом)
                result = self.redis_client.brpop(self.notification_queue, timeout=5)

                if result:
                    # result[0] - имя очереди, result[1] - данные
                    notification_json = result[1].decode("utf-8")
                    notification_data = json.loads(notification_json)

                    # Проверяем, пора ли отправлять уведомление
                    scheduled_time = datetime.fromisoformat(
                        notification_data["scheduled_time"]
                    )
                    current_time = datetime.now()

                    if current_time >= scheduled_time:
                        # Время пришло, обрабатываем уведомление
                        self._process_notification(notification_data)
                    else:
                        # Время еще не пришло, возвращаем в очередь
                        self.redis_client.lpush(
                            self.notification_queue, notification_json
                        )
                        # Ждем до времени отправки
                        wait_seconds = (scheduled_time - current_time).total_seconds()
                        if wait_seconds > 0:
                            time.sleep(min(wait_seconds, 60))  # Максимум 60 секунд
                else:
                    # Нет уведомлений в очереди, ждем
                    time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Ошибка в основном цикле worker'а: {e}")
                time.sleep(10)  # При ошибке ждем дольше

        logger.info("🛑 NotificationWorker остановлен")


def main():
    """Главная функция"""
    try:
        # Создаем Redis клиент
        redis_client = redis.Redis.from_url(Config.REDIS_URL, db=Config.REDIS_DB)
        
        # Проверяем подключение к Redis
        try:
            redis_client.ping()
            logger.info("✅ Подключение к Redis установлено")
        except redis.ConnectionError as e:
            logger.error(f"❌ Не удалось подключиться к Redis: {e}")
            logger.error("Убедитесь, что Redis запущен и доступен по адресу: " + Config.REDIS_URL)
            sys.exit(1)
        
        worker = NotificationWorker(
            redis_client=redis_client,
            send_notification_sync_from_worker=NotificationSender(
                duty_manager=DutyManager(
                    google_sheets_url=Config.GOOGLE_SHEET_URL,
                    credentials_path=Config.GOOGLE_CREDENTIALS_PATH,
                    sheet_range=Config.GOOGLE_SHEET_RANGE,
                ),
                worker_client=WebClient(token=Config.SLACK_BOT_TOKEN),
            ),
            db=Database(),
        )
        worker.run()
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал прерывания")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
